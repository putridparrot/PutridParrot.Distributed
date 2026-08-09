using PutridParrot.Distributed.Coordination;
using StackExchange.Redis;

namespace PutridParrot.Distributed.Redis.Coordination;

/// <summary>
/// Redis implementation of IDistributedSemaphoreProvider using Lua scripts.
/// Implements a counting semaphore with atomic increment/decrement operations.
/// </summary>
public class RedisSemaphoreProvider : IDistributedSemaphoreProvider
{
    private readonly IConnectionMultiplexer _redis;
    private readonly int _database;

    /// <summary>
    /// Creates a new Redis semaphore provider.
    /// </summary>
    /// <param name="redis">Redis connection multiplexer</param>
    /// <param name="database">Redis database number (default 0)</param>
    public RedisSemaphoreProvider(IConnectionMultiplexer redis, int database = 0)
    {
        _redis = redis ?? throw new ArgumentNullException(nameof(redis));
        _database = database;
    }

    /// <summary>
    /// Attempts to acquire permits from the semaphore using a Lua script.
    /// </summary>
    public async Task<long> TryAcquirePermitsAsync(
        string key,
        long permitsRequested,
        long maxPermits,
        CancellationToken cancellationToken = default)
    {
        var db = _redis.GetDatabase(_database);

        // Lua script for atomic acquire
        // KEYS[1] = semaphore key
        // ARGV[1] = permits requested
        // ARGV[2] = max permits
        // Returns: permits acquired (0 if unsuccessful)
        const string acquireScript = @"
            local current = redis.call('get', KEYS[1])
            if not current then
                -- Initialize with max permits
                redis.call('set', KEYS[1], tostring(ARGV[2]))
                current = tonumber(ARGV[2])
            else
                current = tonumber(current)
            end

            local requested = tonumber(ARGV[1])
            if current >= requested then
                local newValue = current - requested
                redis.call('set', KEYS[1], tostring(newValue))
                return requested
            else
                return 0
            end";

        var result = await db.ScriptEvaluateAsync(
            acquireScript,
            keys: [key],
            values: [permitsRequested, maxPermits]);

        return Convert.ToInt64(result);
    }

    /// <summary>
    /// Releases permits back to the semaphore using a Lua script.
    /// </summary>
    public async Task<bool> ReleasePermitsAsync(
        string key,
        long permitsToRelease,
        long maxPermits,
        CancellationToken cancellationToken = default)
    {
        var db = _redis.GetDatabase(_database);

        // Lua script for atomic release
        // KEYS[1] = semaphore key
        // ARGV[1] = permits to release
        // ARGV[2] = max permits
        // Returns: 1 if successful, 0 otherwise
        const string releaseScript = @"
            local current = redis.call('get', KEYS[1])
            if not current then
                -- Initialize if doesn't exist
                redis.call('set', KEYS[1], tostring(ARGV[2]))
                current = tonumber(ARGV[2])
            else
                current = tonumber(current)
            end

            local toRelease = tonumber(ARGV[1])
            local max = tonumber(ARGV[2])
            local newValue = math.min(current + toRelease, max)

            redis.call('set', KEYS[1], tostring(newValue))
            return 1";

        var result = await db.ScriptEvaluateAsync(
            releaseScript,
            keys: [key],
            values: [permitsToRelease, maxPermits]);

        return (int)result == 1;
    }

    /// <summary>
    /// Gets the current available permits without modifying the semaphore.
    /// </summary>
    public async Task<long> GetAvailablePermitsAsync(
        string key,
        long maxPermits,
        CancellationToken cancellationToken = default)
    {
        var db = _redis.GetDatabase(_database);

        var value = await db.StringGetAsync(key);

        if (!value.HasValue)
        {
            // Not initialized yet
            return maxPermits;
        }

        if (long.TryParse(value.ToString(), out var permits))
        {
            return Math.Max(0, permits);
        }

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
        var db = _redis.GetDatabase(_database);
        await db.StringSetAsync(key, maxPermits.ToString());
        return true;
    }
}

/*
 * Redis Semaphore Implementation Notes:
 * 
 * - Stores a single integer value representing available permits
 * - Lua scripts ensure atomic read-modify-write operations
 * - Lazily initializes to maxPermits on first acquire
 * - No TTL by default (semaphore persists until explicitly deleted or reset)
 * - Very fast: ~1-5ms per operation
 * - Not persistent (lost if Redis restarts)
 * 
 * Use Cases:
 * - Connection pooling across instances
 * - Rate limiting with strict slot limits
 * - Concurrent job processing with resource caps
 * - License seat management (distributed)
 */
