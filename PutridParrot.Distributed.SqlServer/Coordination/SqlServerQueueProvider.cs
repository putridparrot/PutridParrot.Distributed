using Microsoft.Data.SqlClient;
using PutridParrot.Distributed.Coordination;

namespace PutridParrot.Distributed.SqlServer.Coordination;

/// <summary>
/// SQL Server implementation of the distributed queue provider using table-based storage.
/// </summary>
public class SqlServerQueueProvider : IDistributedQueueProvider
{
    private readonly string _connectionString;

    /// <summary>
    /// Initializes a new instance of the SqlServerQueueProvider class.
    /// </summary>
    /// <param name="connectionString">SQL Server connection string.</param>
    public SqlServerQueueProvider(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    public async Task EnsureInitializedAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            IF OBJECT_ID('dbo.WorkItems', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.WorkItems (
                    Id NVARCHAR(128) PRIMARY KEY,
                    QueueName NVARCHAR(256) NOT NULL,
                    Payload NVARCHAR(MAX) NOT NULL,
                    State INT NOT NULL,
                    Priority INT NOT NULL DEFAULT 0,
                    WorkerId NVARCHAR(256),
                    AttemptCount INT NOT NULL DEFAULT 0,
                    ErrorMessage NVARCHAR(MAX),
                    EnqueuedAt DATETIME2 NOT NULL,
                    UpdatedAt DATETIME2 NOT NULL,
                    VisibilityDeadline DATETIME2,
                    CONSTRAINT IX_QueueName_State UNIQUE (QueueName, State)
                );
                CREATE NONCLUSTERED INDEX IX_QueueName_State_Priority 
                    ON dbo.WorkItems(QueueName, State, Priority DESC);
            END

            IF OBJECT_ID('dbo.QueueStats', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.QueueStats (
                    QueueName NVARCHAR(256) PRIMARY KEY,
                    TotalProcessed BIGINT NOT NULL DEFAULT 0,
                    CompletedCount BIGINT NOT NULL DEFAULT 0,
                    DeadLetterCount BIGINT NOT NULL DEFAULT 0
                );
            END
        ";

        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<WorkItem> EnqueueAsync(
        string queueName,
        WorkItem workItem,
        QueueOptions options,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            INSERT INTO dbo.WorkItems (Id, QueueName, Payload, State, Priority, AttemptCount, EnqueuedAt, UpdatedAt)
            VALUES (@id, @queueName, @payload, @state, @priority, 0, @enqueued, @updated);

            IF NOT EXISTS (SELECT 1 FROM dbo.QueueStats WHERE QueueName = @queueName)
                INSERT INTO dbo.QueueStats (QueueName, TotalProcessed) VALUES (@queueName, 1);
            ELSE
                UPDATE dbo.QueueStats SET TotalProcessed = TotalProcessed + 1 WHERE QueueName = @queueName;
        ";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@id", workItem.Id);
        command.Parameters.AddWithValue("@queueName", queueName);
        command.Parameters.AddWithValue("@payload", workItem.Payload);
        command.Parameters.AddWithValue("@state", (int)WorkItemState.Pending);
        command.Parameters.AddWithValue("@priority", workItem.Priority);
        command.Parameters.AddWithValue("@enqueued", workItem.EnqueuedAt);
        command.Parameters.AddWithValue("@updated", workItem.UpdatedAt);

        await command.ExecuteNonQueryAsync(cancellationToken);

        return workItem;
    }

    public async Task<WorkItem?> DequeueAsync(
        string queueName,
        string workerId,
        QueueOptions options,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
            BEGIN TRANSACTION;

            DECLARE @id NVARCHAR(128);
            SELECT TOP 1 @id = Id FROM dbo.WorkItems 
            WHERE QueueName = @queueName AND State = @pendingState
            ORDER BY Priority DESC, EnqueuedAt ASC;

            IF @id IS NOT NULL
            BEGIN
                UPDATE dbo.WorkItems 
                SET State = @processingState, WorkerId = @workerId, AttemptCount = AttemptCount + 1, 
                    VisibilityDeadline = DATEADD(SECOND, @timeoutSeconds, GETUTCDATE()), UpdatedAt = GETUTCDATE()
                WHERE Id = @id;

                SELECT Id, QueueName, Payload, State, Priority, WorkerId, AttemptCount, ErrorMessage, 
                       EnqueuedAt, UpdatedAt, VisibilityDeadline
                FROM dbo.WorkItems WHERE Id = @id;
            END

            COMMIT TRANSACTION;
        ";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@queueName", queueName);
        command.Parameters.AddWithValue("@pendingState", (int)WorkItemState.Pending);
        command.Parameters.AddWithValue("@processingState", (int)WorkItemState.Processing);
        command.Parameters.AddWithValue("@workerId", workerId);
        command.Parameters.AddWithValue("@timeoutSeconds", (int)options.VisibilityTimeout.TotalSeconds);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new WorkItem
            {
                Id = reader.GetString(0),
                Payload = reader.GetString(2),
                State = (WorkItemState)reader.GetInt32(3),
                Priority = reader.GetInt32(4),
                WorkerId = reader.IsDBNull(5) ? null : reader.GetString(5),
                AttemptCount = reader.GetInt32(6),
                ErrorMessage = reader.IsDBNull(7) ? null : reader.GetString(7),
                EnqueuedAt = reader.GetDateTime(8),
                UpdatedAt = reader.GetDateTime(9),
                VisibilityDeadline = reader.IsDBNull(10) ? null : reader.GetDateTime(10)
            };
        }

        return null;
    }

    public async Task AcknowledgeAsync(
        string queueName,
        string workItemId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            DELETE FROM dbo.WorkItems WHERE Id = @id AND QueueName = @queueName;
            UPDATE dbo.QueueStats SET CompletedCount = CompletedCount + 1 WHERE QueueName = @queueName;
        ";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@id", workItemId);
        command.Parameters.AddWithValue("@queueName", queueName);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task NackAsync(
        string queueName,
        string workItemId,
        string? errorMessage,
        QueueOptions options,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            DECLARE @attemptCount INT;
            SELECT @attemptCount = AttemptCount FROM dbo.WorkItems WHERE Id = @id;

            IF @attemptCount >= @maxAttempts
            BEGIN
                EXEC sp_executesql N'dbo.MoveToDeadLetterAsync',
                    N'@id NVARCHAR(128), @queueName NVARCHAR(256), @reason NVARCHAR(MAX)',
                    @id, @queueName, @reason;
            END
            ELSE
            BEGIN
                UPDATE dbo.WorkItems 
                SET State = @pendingState, WorkerId = NULL, VisibilityDeadline = NULL, 
                    ErrorMessage = @errorMessage, UpdatedAt = GETUTCDATE()
                WHERE Id = @id;
            END
        ";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@id", workItemId);
        command.Parameters.AddWithValue("@queueName", queueName);
        command.Parameters.AddWithValue("@maxAttempts", options.MaxAttempts);
        command.Parameters.AddWithValue("@errorMessage", errorMessage ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("@pendingState", (int)WorkItemState.Pending);
        command.Parameters.AddWithValue("@reason", errorMessage ?? "Max attempts exceeded");

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MoveToDeadLetterAsync(
        string queueName,
        string workItemId,
        string reason,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            UPDATE dbo.WorkItems 
            SET State = @deadLetterState, ErrorMessage = @reason, UpdatedAt = GETUTCDATE()
            WHERE Id = @id AND QueueName = @queueName;

            UPDATE dbo.QueueStats SET DeadLetterCount = DeadLetterCount + 1 
            WHERE QueueName = @queueName;
        ";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@id", workItemId);
        command.Parameters.AddWithValue("@queueName", queueName);
        command.Parameters.AddWithValue("@deadLetterState", (int)WorkItemState.DeadLetter);
        command.Parameters.AddWithValue("@reason", reason);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<QueueState> GetStateAsync(
        string queueName,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            SELECT 
                ISNULL(COUNT(CASE WHEN State = @pendingState THEN 1 END), 0) as PendingCount,
                ISNULL(COUNT(CASE WHEN State = @processingState THEN 1 END), 0) as ProcessingCount,
                ISNULL(COUNT(CASE WHEN State = @deadLetterState THEN 1 END), 0) as DeadLetterCount,
                ISNULL((SELECT CompletedCount FROM dbo.QueueStats WHERE QueueName = @queueName), 0) as CompletedCount,
                ISNULL((SELECT TotalProcessed FROM dbo.QueueStats WHERE QueueName = @queueName), 0) as TotalProcessed
            FROM dbo.WorkItems
            WHERE QueueName = @queueName;
        ";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@queueName", queueName);
        command.Parameters.AddWithValue("@pendingState", (int)WorkItemState.Pending);
        command.Parameters.AddWithValue("@processingState", (int)WorkItemState.Processing);
        command.Parameters.AddWithValue("@deadLetterState", (int)WorkItemState.DeadLetter);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new QueueState
            {
                QueueName = queueName,
                PendingCount = reader.GetInt64(0),
                ProcessingCount = reader.GetInt64(1),
                DeadLetterCount = reader.GetInt64(2),
                CompletedCount = reader.GetInt64(3),
                TotalProcessed = reader.GetInt64(4),
                Timestamp = DateTime.UtcNow
            };
        }

        return new QueueState { QueueName = queueName, Timestamp = DateTime.UtcNow };
    }

    public async Task ResetAsync(
        string queueName,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            DELETE FROM dbo.WorkItems WHERE QueueName = @queueName;
            DELETE FROM dbo.QueueStats WHERE QueueName = @queueName;
        ";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@queueName", queueName);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IEnumerable<WorkItem>> GetDeadLetterItemsAsync(
        string queueName,
        QueueOptions options,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var items = new List<WorkItem>();

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            SELECT Id, Payload, State, Priority, WorkerId, AttemptCount, ErrorMessage, 
                   EnqueuedAt, UpdatedAt, VisibilityDeadline
            FROM dbo.WorkItems 
            WHERE QueueName = @queueName AND State = @deadLetterState
            ORDER BY UpdatedAt DESC;
        ";

        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@queueName", queueName);
        command.Parameters.AddWithValue("@deadLetterState", (int)WorkItemState.DeadLetter);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new WorkItem
            {
                Id = reader.GetString(0),
                Payload = reader.GetString(1),
                State = (WorkItemState)reader.GetInt32(2),
                Priority = reader.GetInt32(3),
                WorkerId = reader.IsDBNull(4) ? null : reader.GetString(4),
                AttemptCount = reader.GetInt32(5),
                ErrorMessage = reader.IsDBNull(6) ? null : reader.GetString(6),
                EnqueuedAt = reader.GetDateTime(7),
                UpdatedAt = reader.GetDateTime(8),
                VisibilityDeadline = reader.IsDBNull(9) ? null : reader.GetDateTime(9)
            });
        }

        return items;
    }
}
