using Microsoft.Data.SqlClient;
using PutridParrot.Distributed.Coordination;
using System.Data;

namespace PutridParrot.Distributed.SqlServer.Coordination;

/// <summary>
/// SQL Server implementation of IDistributedCacheProvider using application locks (sp_getapplock/sp_releaseapplock).
/// This provider uses SQL Server's built-in application lock mechanism which is designed for distributed locking scenarios.
/// </summary>
public class SqlServerCacheProvider : IDistributedCacheProvider
{
    private readonly string _connectionString;
    private readonly Dictionary<string, SqlConnection> _lockConnections = new();
    private readonly Lock _lockObject = new();

    /// <summary>
    /// Creates a new SQL Server cache provider.
    /// </summary>
    /// <param name="connectionString">SQL Server connection string</param>
    public SqlServerCacheProvider(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    /// <summary>
    /// Attempts to acquire a lock using SQL Server's sp_getapplock.
    /// Uses "Session" mode to maintain the lock for the connection lifetime.
    /// </summary>
    public async Task<bool> TryAcquireLockAsync(
        string key,
        string value,
        TimeSpan expiry,
        CancellationToken cancellationToken = default)
    {
        var lockKey = GetLockKey(key, value);

        lock (_lockObject)
        {
            // Check if this lock is already held
            if (_lockConnections.ContainsKey(lockKey))
            {
                return false;
            }
        }

        var connection = new SqlConnection(_connectionString);

        try
        {
            await connection.OpenAsync(cancellationToken);

            // sp_getapplock acquires an application lock
            // @LockMode = 'Exclusive' ensures only one holder
            // @LockOwner = 'Session' maintains lock for connection lifetime
            // @LockTimeout = 0 means immediate return (non-blocking)
            await using var command = new SqlCommand("sp_getapplock", connection)
            {
                CommandType = CommandType.StoredProcedure
            };

            command.Parameters.AddWithValue("@Resource", key);
            command.Parameters.AddWithValue("@LockMode", "Exclusive");
            command.Parameters.AddWithValue("@LockOwner", "Session");
            command.Parameters.AddWithValue("@LockTimeout", 0); // Immediate return

            var returnParam = command.Parameters.Add("@ReturnValue", SqlDbType.Int);
            returnParam.Direction = ParameterDirection.ReturnValue;

            await command.ExecuteNonQueryAsync(cancellationToken);

            var returnValue = (int)returnParam.Value;

            // Return values:
            // 0 or greater = success
            // -1 = timeout (lock not acquired)
            // -2 = canceled
            // -3 = deadlock victim
            // -999 = parameter validation or other error
            if (returnValue >= 0)
            {
                lock (_lockObject)
                {
                    _lockConnections[lockKey] = connection;
                }

                // Start a task to release the lock after expiry
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(expiry, cancellationToken);
                        await ReleaseLockAsync(key, value, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected on cancellation
                    }
                }, cancellationToken);

                return true;
            }

            // Lock not acquired, close the connection
            await connection.DisposeAsync();
            return false;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Releases a lock using SQL Server's sp_releaseapplock.
    /// </summary>
    public async Task<bool> ReleaseLockAsync(
        string key,
        string value,
        CancellationToken cancellationToken = default)
    {
        var lockKey = GetLockKey(key, value);
        SqlConnection? connection;

        lock (_lockObject)
        {
            if (!_lockConnections.TryGetValue(lockKey, out connection))
            {
                return false;
            }

            _lockConnections.Remove(lockKey);
        }

        try
        {
            if (connection.State == ConnectionState.Open)
            {
                // sp_releaseapplock releases an application lock
                using var command = new SqlCommand("sp_releaseapplock", connection)
                {
                    CommandType = CommandType.StoredProcedure
                };

                command.Parameters.AddWithValue("@Resource", key);
                command.Parameters.AddWithValue("@LockOwner", "Session");

                var returnParam = command.Parameters.Add("@ReturnValue", SqlDbType.Int);
                returnParam.Direction = ParameterDirection.ReturnValue;

                await command.ExecuteNonQueryAsync(cancellationToken);

                var returnValue = (int)returnParam.Value;
                // 0 = success, -999 = error
                return returnValue == 0;
            }

            return false;
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }

    /// <summary>
    /// Extends the lock expiration time by scheduling a new auto-release.
    /// Note: SQL Server locks don't have built-in expiration, so we simulate it with delayed release.
    /// </summary>
    public async Task<bool> ExtendLockAsync(
        string key,
        string value,
        TimeSpan expiry,
        CancellationToken cancellationToken = default)
    {
        var lockKey = GetLockKey(key, value);

        lock (_lockObject)
        {
            // Check if we hold this lock
            if (!_lockConnections.ContainsKey(lockKey))
            {
                return false;
            }
        }

        // Schedule extension by delaying release
        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(expiry, cancellationToken);
                await ReleaseLockAsync(key, value, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Expected on cancellation
            }
        }, cancellationToken);

        return await Task.FromResult(true);
    }

    private static string GetLockKey(string key, string value) => $"{key}:{value}";
}

/*
 * SQL Server Setup:
 * 
 * No database schema setup required! SQL Server's sp_getapplock is a built-in
 * system stored procedure that manages application locks.
 * 
 * Connection String Examples:
 * 
 * // Windows Authentication
 * Server=localhost;Database=MyDatabase;Integrated Security=true;TrustServerCertificate=true;
 * 
 * // SQL Authentication
 * Server=localhost;Database=MyDatabase;User Id=myuser;Password=mypassword;TrustServerCertificate=true;
 * 
 * // Azure SQL Database
 * Server=tcp:myserver.database.windows.net,1433;Database=MyDatabase;User Id=myuser;Password=mypassword;Encrypt=true;
 * 
 * 
 * Usage Example:
 * 
 * var connectionString = "Server=localhost;Database=TestDB;Integrated Security=true;TrustServerCertificate=true;";
 * var cacheProvider = new SqlServerCacheProvider(connectionString);
 * 
 * await using var lockInstance = new DistributedLock(cacheProvider, "my-resource");
 * if (await lockInstance.TryAcquireAsync(TimeSpan.FromSeconds(30)))
 * {
 *     // Critical section
 * }
 * 
 * 
 * How It Works:
 * 
 * - Uses sp_getapplock to acquire exclusive locks on named resources
 * - Locks are held at the session level (connection must stay open)
 * - Automatically released when connection closes
 * - Simulates expiration by scheduling automatic release after timeout
 * - Thread-safe with proper connection management
 * 
 * 
 * Advantages:
 * - Built-in to SQL Server, no schema changes needed
 * - Transactional support available
 * - Deadlock detection
 * - Easy to monitor with sys.dm_tran_locks
 * 
 * 
 * Limitations:
 * - Requires open connection for lock duration
 * - No built-in expiration (simulated with delayed release)
 * - Connection pool considerations
 */

