using PutridParrot.Distributed.Coordination;
using StackExchange.Redis;

namespace PutridParrot.Distributed.Redis.Coordination;

/// <summary>
/// Redis implementation of IDistributedCacheProvider using StackExchange.Redis.
/// </summary>
public class RedisCacheProvider : IDistributedCacheProvider
{
    private readonly IDatabase _database;

    /// <summary>
    /// Creates a new Redis cache provider.
    /// </summary>
    /// <param name="redis">The Redis connection multiplexer</param>
    /// <param name="database">The database number (default is 0)</param>
    public RedisCacheProvider(IConnectionMultiplexer redis, int database = 0)
    {
        _database = redis.GetDatabase(database);
    }

    /// <summary>
    /// Attempts to acquire a lock by setting a key with a value if it doesn't exist.
    /// Uses Redis SET command with NX (Not eXists) and EX (EXpiry) options.
    /// </summary>
    public async Task<bool> TryAcquireLockAsync(
        string key,
        string value,
        TimeSpan expiry,
        CancellationToken cancellationToken = default)
    {
        // SET key value NX EX seconds
        // NX - Only set if key does not exist
        // When.NotExists ensures atomicity
        return await _database.StringSetAsync(
            key,
            value,
            expiry,
            when: When.NotExists,
            flags: CommandFlags.None);
    }

    /// <summary>
    /// Releases a lock by deleting the key only if it matches the expected value.
    /// Uses Lua script to ensure atomic check-and-delete operation.
    /// </summary>
    public async Task<bool> ReleaseLockAsync(
        string key,
        string value,
        CancellationToken cancellationToken = default)
    {
        // Lua script ensures atomicity: only delete if value matches
        // This prevents accidentally releasing a lock held by another instance
        const string script = @"
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end";

        var result = await _database.ScriptEvaluateAsync(
            script,
            keys: [key],
            values: [value]);

        return (int)result == 1;
    }

    /// <summary>
    /// Extends the expiration time of an existing lock if the value matches.
    /// Uses Lua script to ensure atomic check-and-extend operation.
    /// </summary>
    public async Task<bool> ExtendLockAsync(
        string key,
        string value,
        TimeSpan expiry,
        CancellationToken cancellationToken = default)
    {
        // Lua script ensures atomicity: only extend if value matches
        // This prevents extending a lock that has been released and reacquired by another instance
        const string script = @"
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('expire', KEYS[1], ARGV[2])
            else
                return 0
            end";

        var result = await _database.ScriptEvaluateAsync(
            script,
            keys: [key],
            values: [value, (int)expiry.TotalSeconds]);

        return (int)result == 1;
    }
}

