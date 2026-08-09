using Microsoft.Data.SqlClient;
using PutridParrot.Distributed.Coordination;

namespace PutridParrot.Distributed.SqlServer.Coordination;

/// <summary>
/// SQL Server implementation of IDistributedSemaphoreProvider using application locks and transactions.
/// Implements a counting semaphore with persistent state in a database table.
/// </summary>
public class SqlServerSemaphoreProvider : IDistributedSemaphoreProvider
{
    private readonly string _connectionString;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private volatile bool _initialized;

    /// <summary>
    /// Creates a new SQL Server semaphore provider.
    /// </summary>
    /// <param name="connectionString">SQL Server connection string</param>
    public SqlServerSemaphoreProvider(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    /// <summary>
    /// Initializes the database schema on first use.
    /// </summary>
    private async Task EnsureInitializedAsync(CancellationToken cancellationToken = default)
    {
        if (_initialized)
        {
            return;
        }

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_initialized)
            {
                return;
            }

            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = new SqlCommand(
                @"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Semaphores')
                  BEGIN
                      CREATE TABLE Semaphores (
                          SemaphoreKey NVARCHAR(256) PRIMARY KEY,
                          AvailablePermits BIGINT NOT NULL,
                          MaxPermits BIGINT NOT NULL,
                          CreatedAt DATETIME2 DEFAULT GETUTCDATE(),
                          UpdatedAt DATETIME2 DEFAULT GETUTCDATE()
                      );

                      CREATE INDEX idx_semaphores_updated_at ON Semaphores(UpdatedAt);
                  END",
                connection);

            await command.ExecuteNonQueryAsync(cancellationToken);
            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    /// <summary>
    /// Attempts to acquire permits from the semaphore using application locks and transactions.
    /// </summary>
    public async Task<long> TryAcquirePermitsAsync(
        string key,
        long permitsRequested,
        long maxPermits,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // Acquire application lock
        using var lockCmd = new SqlCommand(
            "EXEC sp_getapplock @Resource = @key, @LockMode = 'Exclusive', @LockOwner = 'Session'",
            connection);
        lockCmd.Parameters.AddWithValue("@key", key);
        await lockCmd.ExecuteNonQueryAsync(cancellationToken);

        try
        {
            using var transaction = connection.BeginTransaction();
            try
            {
                // Get or create semaphore state
                long availablePermits = maxPermits;
                bool exists = false;

                using (var getCmd = new SqlCommand(
                    "SELECT AvailablePermits FROM Semaphores WHERE SemaphoreKey = @Key",
                    connection,
                    transaction))
                {
                    getCmd.Parameters.AddWithValue("@Key", key);

                    using var reader = await getCmd.ExecuteReaderAsync(cancellationToken);
                    if (await reader.ReadAsync(cancellationToken))
                    {
                        availablePermits = reader.GetInt64(0);
                        exists = true;
                    }
                }

                // Create if doesn't exist
                if (!exists)
                {
                    using var insertCmd = new SqlCommand(
                        @"INSERT INTO Semaphores (SemaphoreKey, AvailablePermits, MaxPermits) 
                          VALUES (@Key, @Available, @Max)",
                        connection,
                        transaction);
                    insertCmd.Parameters.AddWithValue("@Key", key);
                    insertCmd.Parameters.AddWithValue("@Available", maxPermits);
                    insertCmd.Parameters.AddWithValue("@Max", maxPermits);

                    await insertCmd.ExecuteNonQueryAsync(cancellationToken);
                    availablePermits = maxPermits;
                }

                // Try to acquire permits
                if (availablePermits >= permitsRequested)
                {
                    long newAvailable = availablePermits - permitsRequested;

                    using var updateCmd = new SqlCommand(
                        @"UPDATE Semaphores 
                          SET AvailablePermits = @Available, UpdatedAt = GETUTCDATE()
                          WHERE SemaphoreKey = @Key",
                        connection,
                        transaction);
                    updateCmd.Parameters.AddWithValue("@Available", newAvailable);
                    updateCmd.Parameters.AddWithValue("@Key", key);

                    await updateCmd.ExecuteNonQueryAsync(cancellationToken);
                    await transaction.CommitAsync(cancellationToken);
                    return permitsRequested;
                }

                await transaction.CommitAsync(cancellationToken);
                return 0;
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken);
                throw;
            }
        }
        finally
        {
            // Release application lock
            using var unlockCmd = new SqlCommand(
                "EXEC sp_releaseapplock @Resource = @key, @LockOwner = 'Session'",
                connection);
            unlockCmd.Parameters.AddWithValue("@key", key);
            await unlockCmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Releases permits back to the semaphore.
    /// </summary>
    public async Task<bool> ReleasePermitsAsync(
        string key,
        long permitsToRelease,
        long maxPermits,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // Acquire application lock
        using var lockCmd = new SqlCommand(
            "EXEC sp_getapplock @Resource = @key, @LockMode = 'Exclusive', @LockOwner = 'Session'",
            connection);
        lockCmd.Parameters.AddWithValue("@key", key);
        await lockCmd.ExecuteNonQueryAsync(cancellationToken);

        try
        {
            using var transaction = connection.BeginTransaction();
            try
            {
                // Get current state
                long availablePermits = maxPermits;

                using (var getCmd = new SqlCommand(
                    "SELECT AvailablePermits FROM Semaphores WHERE SemaphoreKey = @Key",
                    connection,
                    transaction))
                {
                    getCmd.Parameters.AddWithValue("@Key", key);

                    using var reader = await getCmd.ExecuteReaderAsync(cancellationToken);
                    if (await reader.ReadAsync(cancellationToken))
                    {
                        availablePermits = reader.GetInt64(0);
                    }
                }

                // Release permits (cap at maxPermits)
                long newAvailable = Math.Min(availablePermits + permitsToRelease, maxPermits);

                using (var updateCmd = new SqlCommand(
                    @"UPDATE Semaphores 
                      SET AvailablePermits = @Available, UpdatedAt = GETUTCDATE()
                      WHERE SemaphoreKey = @Key",
                    connection,
                    transaction))
                {
                    updateCmd.Parameters.AddWithValue("@Available", newAvailable);
                    updateCmd.Parameters.AddWithValue("@Key", key);

                    await updateCmd.ExecuteNonQueryAsync(cancellationToken);
                }

                await transaction.CommitAsync(cancellationToken);
                return true;
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken);
                throw;
            }
        }
        finally
        {
            // Release application lock
            using var unlockCmd = new SqlCommand(
                "EXEC sp_releaseapplock @Resource = @key, @LockOwner = 'Session'",
                connection);
            unlockCmd.Parameters.AddWithValue("@key", key);
            await unlockCmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Gets the current available permits.
    /// </summary>
    public async Task<long> GetAvailablePermitsAsync(
        string key,
        long maxPermits,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        using var command = new SqlCommand(
            "SELECT ISNULL(AvailablePermits, @Max) FROM Semaphores WHERE SemaphoreKey = @Key",
            connection);
        command.Parameters.AddWithValue("@Key", key);
        command.Parameters.AddWithValue("@Max", maxPermits);

        var result = await command.ExecuteScalarAsync(cancellationToken);

        if (result is long permits)
            return Math.Max(0, permits);

        return maxPermits;
    }

    /// <summary>
    /// Resets the semaphore to full capacity.
    /// </summary>
    public async Task<bool> ResetAsync(
        string key,
        long maxPermits,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        using var command = new SqlCommand(
            @"DELETE FROM Semaphores WHERE SemaphoreKey = @Key;
              INSERT INTO Semaphores (SemaphoreKey, AvailablePermits, MaxPermits) 
              VALUES (@Key, @Max, @Max)",
            connection);

        command.Parameters.AddWithValue("@Key", key);
        command.Parameters.AddWithValue("@Max", maxPermits);

        await command.ExecuteNonQueryAsync(cancellationToken);
        return true;
    }
}

/*
 * SQL Server Semaphore Implementation Notes:
 * 
 * - Stores semaphore state in 'Semaphores' table with columns:
 *   * SemaphoreKey: Unique identifier
 *   * AvailablePermits: Current available permits (0 to MaxPermits)
 *   * MaxPermits: Maximum permits capacity
 * 
 * - Uses sp_getapplock for serialized access per semaphore
 * - Uses transactions for atomic read-modify-write
 * - Automatically creates table on first use
 * - Slower than Redis (~5-20ms per operation) but fully persistent
 * 
 * Use Cases:
 * - Long-term semaphore management (survives restarts)
 * - Audit logging (SQL Server maintains all changes)
 * - Integration with existing SQL Server databases
 */
