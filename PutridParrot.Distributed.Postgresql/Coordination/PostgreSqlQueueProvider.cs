using Npgsql;
using PutridParrot.Distributed.Coordination;

namespace PutridParrot.Distributed.Postgresql.Coordination;

/// <summary>
/// PostgreSQL implementation of the distributed queue provider using table-based storage.
/// </summary>
public class PostgreSqlQueueProvider : IDistributedQueueProvider
{
    private readonly string _connectionString;

    /// <summary>
    /// Initializes a new instance of the PostgreSqlQueueProvider class.
    /// </summary>
    /// <param name="connectionString">PostgreSQL connection string.</param>
    public PostgreSqlQueueProvider(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    public async Task EnsureInitializedAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            CREATE TABLE IF NOT EXISTS work_items (
                id VARCHAR(128) PRIMARY KEY,
                queue_name VARCHAR(256) NOT NULL,
                payload TEXT NOT NULL,
                state INTEGER NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                worker_id VARCHAR(256),
                attempt_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                enqueued_at TIMESTAMP WITH TIME ZONE NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                visibility_deadline TIMESTAMP WITH TIME ZONE
            );

            CREATE INDEX IF NOT EXISTS ix_work_items_queue_state_priority 
                ON work_items(queue_name, state, priority DESC);

            CREATE TABLE IF NOT EXISTS queue_stats (
                queue_name VARCHAR(256) PRIMARY KEY,
                total_processed BIGINT NOT NULL DEFAULT 0,
                completed_count BIGINT NOT NULL DEFAULT 0,
                dead_letter_count BIGINT NOT NULL DEFAULT 0
            );
        ";

        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<WorkItem> EnqueueAsync(
        string queueName,
        WorkItem workItem,
        QueueOptions options,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            INSERT INTO work_items (id, queue_name, payload, state, priority, attempt_count, enqueued_at, updated_at)
            VALUES (@id, @queueName, @payload, @state, @priority, 0, @enqueued, @updated);

            INSERT INTO queue_stats (queue_name, total_processed) VALUES (@queueName, 1)
            ON CONFLICT (queue_name) DO UPDATE SET total_processed = queue_stats.total_processed + 1;
        ";

        await using var command = new NpgsqlCommand(sql, connection);
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

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;

            SELECT id FROM work_items 
            WHERE queue_name = @queueName AND state = @pendingState
            ORDER BY priority DESC, enqueued_at ASC
            LIMIT 1 FOR UPDATE SKIP LOCKED;
        ";

        string? itemId = null;
        await using (var command = new NpgsqlCommand(sql, connection))
        {
            command.Parameters.AddWithValue("@queueName", queueName);
            command.Parameters.AddWithValue("@pendingState", (int)WorkItemState.Pending);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                itemId = reader.GetString(0);
            }
        }

        if (itemId != null)
        {
            var updateSql = @"
                UPDATE work_items 
                SET state = @processingState, worker_id = @workerId, attempt_count = attempt_count + 1,
                    visibility_deadline = CURRENT_TIMESTAMP + (@timeoutSeconds || ' seconds')::INTERVAL, 
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = @id
                RETURNING id, queue_name, payload, state, priority, worker_id, attempt_count, error_message,
                          enqueued_at, updated_at, visibility_deadline;
            ";

            await using var updateCommand = new NpgsqlCommand(updateSql, connection);
            updateCommand.Parameters.AddWithValue("@id", itemId);
            updateCommand.Parameters.AddWithValue("@processingState", (int)WorkItemState.Processing);
            updateCommand.Parameters.AddWithValue("@workerId", workerId);
            updateCommand.Parameters.AddWithValue("@timeoutSeconds", (int)options.VisibilityTimeout.TotalSeconds);

            await using var reader = await updateCommand.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                var workItem = new WorkItem
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

                await using var commitCommand = new NpgsqlCommand("COMMIT;", connection);
                await commitCommand.ExecuteNonQueryAsync(cancellationToken);
                return workItem;
            }
        }

        await using var rollbackCommand = new NpgsqlCommand("COMMIT;", connection);
        await rollbackCommand.ExecuteNonQueryAsync(cancellationToken);
        return null;
    }

    public async Task AcknowledgeAsync(
        string queueName,
        string workItemId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            DELETE FROM work_items WHERE id = @id AND queue_name = @queueName;
            INSERT INTO queue_stats (queue_name, completed_count) VALUES (@queueName, 1)
            ON CONFLICT (queue_name) DO UPDATE SET completed_count = queue_stats.completed_count + 1;
        ";

        await using var command = new NpgsqlCommand(sql, connection);
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
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            SELECT attempt_count FROM work_items WHERE id = @id;
        ";

        int attemptCount = 0;
        await using (var command = new NpgsqlCommand(sql, connection))
        {
            command.Parameters.AddWithValue("@id", workItemId);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result != null)
            {
                attemptCount = (int)result;
            }
        }

        if (attemptCount >= options.MaxAttempts)
        {
            await MoveToDeadLetterAsync(queueName, workItemId, errorMessage ?? "Max attempts exceeded", cancellationToken);
        }
        else
        {
            var updateSql = @"
                UPDATE work_items 
                SET state = @pendingState, worker_id = NULL, visibility_deadline = NULL,
                    error_message = @errorMessage, updated_at = CURRENT_TIMESTAMP
                WHERE id = @id;
            ";

            await using var command = new NpgsqlCommand(updateSql, connection);
            command.Parameters.AddWithValue("@id", workItemId);
            command.Parameters.AddWithValue("@pendingState", (int)WorkItemState.Pending);
            command.Parameters.AddWithValue("@errorMessage", errorMessage ?? (object)DBNull.Value);

            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    public async Task MoveToDeadLetterAsync(
        string queueName,
        string workItemId,
        string reason,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            UPDATE work_items 
            SET state = @deadLetterState, error_message = @reason, updated_at = CURRENT_TIMESTAMP
            WHERE id = @id AND queue_name = @queueName;

            INSERT INTO queue_stats (queue_name, dead_letter_count) VALUES (@queueName, 1)
            ON CONFLICT (queue_name) DO UPDATE SET dead_letter_count = queue_stats.dead_letter_count + 1;
        ";

        await using var command = new NpgsqlCommand(sql, connection);
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

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            SELECT 
                COALESCE(COUNT(CASE WHEN state = @pendingState THEN 1 END), 0) as pending_count,
                COALESCE(COUNT(CASE WHEN state = @processingState THEN 1 END), 0) as processing_count,
                COALESCE(COUNT(CASE WHEN state = @deadLetterState THEN 1 END), 0) as dead_letter_count,
                COALESCE((SELECT completed_count FROM queue_stats WHERE queue_name = @queueName), 0) as completed_count,
                COALESCE((SELECT total_processed FROM queue_stats WHERE queue_name = @queueName), 0) as total_processed
            FROM work_items
            WHERE queue_name = @queueName;
        ";

        await using var command = new NpgsqlCommand(sql, connection);
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
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            DELETE FROM work_items WHERE queue_name = @queueName;
            DELETE FROM queue_stats WHERE queue_name = @queueName;
        ";

        await using var command = new NpgsqlCommand(sql, connection);
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

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = @"
            SELECT id, payload, state, priority, worker_id, attempt_count, error_message,
                   enqueued_at, updated_at, visibility_deadline
            FROM work_items 
            WHERE queue_name = @queueName AND state = @deadLetterState
            ORDER BY updated_at DESC;
        ";

        await using var command = new NpgsqlCommand(sql, connection);
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
