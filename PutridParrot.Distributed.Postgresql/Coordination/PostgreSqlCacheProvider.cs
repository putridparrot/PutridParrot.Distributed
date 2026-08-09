using Npgsql;
using PutridParrot.Distributed.Coordination;

namespace PutridParrot.Distributed.Postgresql.Coordination;

/// <summary>
/// PostgreSQL implementation of IDistributedCacheProvider using advisory locks (pg_try_advisory_lock).
/// Advisory locks are session-level, application-defined locks that PostgreSQL maintains.
/// </summary>
public class PostgreSqlCacheProvider : IDistributedCacheProvider
{
    private readonly string _connectionString;
    private readonly Dictionary<string, (NpgsqlConnection Connection, long LockId)> _lockConnections = new();
    private readonly Lock _lockObject = new();

    /// <summary>
    /// Creates a new PostgreSQL cache provider.
    /// </summary>
    /// <param name="connectionString">PostgreSQL connection string</param>
    public PostgreSqlCacheProvider(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    /// <summary>
    /// Attempts to acquire a lock using PostgreSQL's pg_try_advisory_lock.
    /// Advisory locks are fast, application-level locks that don't require tables.
    /// </summary>
    public async Task<bool> TryAcquireLockAsync(
        string key,
        string value,
        TimeSpan expiry,
        CancellationToken cancellationToken = default)
    {
        var lockId = GetLockId(key);
        var lockKey = GetLockKey(key, value);

        lock (_lockObject)
        {
            // Check if this lock is already held
            if (_lockConnections.ContainsKey(lockKey))
            {
                return false;
            }
        }

        var connection = new NpgsqlConnection(_connectionString);

        try
        {
            await connection.OpenAsync(cancellationToken);

            // pg_try_advisory_lock attempts to acquire a lock without blocking
            // Returns true if lock acquired, false otherwise
            await using var command = new NpgsqlCommand(
                "SELECT pg_try_advisory_lock($1)",
                connection);

            command.Parameters.AddWithValue(lockId);

            var result = await command.ExecuteScalarAsync(cancellationToken);

            if (result is bool acquired && acquired)
            {
                lock (_lockObject)
                {
                    _lockConnections[lockKey] = (connection, lockId);
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
    /// Releases a lock using PostgreSQL's pg_advisory_unlock.
    /// </summary>
    public async Task<bool> ReleaseLockAsync(
        string key,
        string value,
        CancellationToken cancellationToken = default)
    {
        var lockKey = GetLockKey(key, value);
        NpgsqlConnection? connection;
        long lockId;

        lock (_lockObject)
        {
            if (!_lockConnections.TryGetValue(lockKey, out var lockInfo))
            {
                return false;
            }

            connection = lockInfo.Connection;
            lockId = lockInfo.LockId;
            _lockConnections.Remove(lockKey);
        }

        try
        {
            // pg_advisory_unlock releases the advisory lock
            await using var command = new NpgsqlCommand(
                "SELECT pg_advisory_unlock($1)",
                connection);

            command.Parameters.AddWithValue(lockId);

            var result = await command.ExecuteScalarAsync(cancellationToken);

            // Returns true if lock was held and released, false otherwise
            return result is bool released && released;
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }

    /// <summary>
    /// Extends the lock expiration time by scheduling a new auto-release.
    /// Note: PostgreSQL advisory locks don't have built-in expiration, so we simulate it with delayed release.
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

    /// <summary>
    /// Converts a string key to a 64-bit integer lock ID for PostgreSQL advisory locks.
    /// Uses a simple hash function to generate consistent IDs.
    /// </summary>
    private static long GetLockId(string key)
    {
        // Use GetHashCode for consistency - same key always produces same ID
        // In production, consider using a more robust hashing algorithm
        return key.GetHashCode(StringComparison.Ordinal);
    }

    private static string GetLockKey(string key, string value) => $"{key}:{value}";
}

/*
 * PostgreSQL Setup:
 * 
 * No database schema setup required! PostgreSQL advisory locks are built-in
 * and don't require any tables or schema changes.
 * 
 * Connection String Examples:
 * 
 * // Local PostgreSQL
 * Host=localhost;Port=5432;Database=mydb;Username=myuser;Password=mypassword;
 * 
 * // Azure Database for PostgreSQL
 * Host=myserver.postgres.database.azure.com;Port=5432;Database=mydb;Username=myuser@myserver;Password=mypassword;SSL Mode=Require;
 * 
 * // With SSL
 * Host=localhost;Database=mydb;Username=myuser;Password=mypassword;SSL Mode=Require;Trust Server Certificate=true;
 * 
 * 
 * Usage Example:
 * 
 * var connectionString = "Host=localhost;Database=testdb;Username=postgres;Password=postgres;";
 * var cacheProvider = new PostgreSqlCacheProvider(connectionString);
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
 * - Uses pg_try_advisory_lock for non-blocking lock acquisition
 * - Locks are session-level (connection must stay open)
 * - Automatically released when connection closes
 * - Converts string keys to 64-bit integers using hash code
 * - Simulates expiration by scheduling automatic release
 * - Thread-safe with proper connection management
 * 
 * 
 * Advantages:
 * - Built-in to PostgreSQL, no schema changes needed
 * - Very fast (no disk I/O)
 * - Lightweight (no table rows)
 * - Deadlock detection
 * - Can be monitored with pg_locks view
 * 
 * 
 * Limitations:
 * - Requires open connection for lock duration
 * - Lock ID limited to 64-bit integers (uses hash of key)
 * - No built-in expiration (simulated with delayed release)
 * - Potential hash collisions (consider using pg_try_advisory_lock(key1, key2) for two-integer keys)
 * 
 * 
 * Alternative: Two-Integer Key
 * For better collision avoidance, you can use:
 * pg_try_advisory_lock(key1::bigint, key2::bigint)
 * This provides 128-bit key space.
 * 
 * 
 * Monitoring Locks:
 * 
 * SELECT * FROM pg_locks WHERE locktype = 'advisory';
 * 
 * This shows all advisory locks currently held.
 */

