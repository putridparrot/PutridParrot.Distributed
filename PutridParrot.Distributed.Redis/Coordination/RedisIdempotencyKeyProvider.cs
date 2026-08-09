using PutridParrot.Distributed.Coordination;
using StackExchange.Redis;

namespace PutridParrot.Distributed.Redis.Coordination;

/// <summary>
/// Redis-based distributed idempotency key provider.
/// 
/// Uses Redis hashes to store operation results with automatic TTL expiration.
/// Fast, in-memory storage suitable for high-throughput idempotency scenarios.
/// </summary>
public class RedisIdempotencyKeyProvider : IDistributedIdempotencyKeyProvider
{
    private readonly IDatabase _database;

    public RedisIdempotencyKeyProvider(IConnectionMultiplexer redis, int database = 0)
    {
        ArgumentNullException.ThrowIfNull(redis);
        _database = redis.GetDatabase(database);
    }

    /// <summary>
    /// Gets a cached result from Redis.
    /// </summary>
    public async Task<string?> GetCachedResultAsync(string idempotencyKey, CancellationToken cancellationToken = default)
    {
        const string hashKey = "idempotency:result";
        var value = await _database.HashGetAsync(hashKey, idempotencyKey);
        return value.IsNullOrEmpty ? null : value.ToString();
    }

    /// <summary>
    /// Stores a result in Redis with TTL expiration.
    /// </summary>
    public async Task<bool> StoreCachedResultAsync(
        string idempotencyKey,
        string result,
        TimeSpan? ttl = null,
        CancellationToken cancellationToken = default)
    {
        //const string hashKey = "idempotency:result";
        //const string claimKey = "idempotency:claim";

        // Use Lua script to atomically check, store result, and clean up claim
        const string script = @"
            local resultKey = KEYS[1]
            local claimKey = KEYS[2]
            local idempotencyKey = ARGV[1]
            local result = ARGV[2]
            local ttl = tonumber(ARGV[3])

            -- Store result
            redis.call('hset', resultKey, idempotencyKey, result)

            -- Set TTL on the hash if provided
            if ttl and ttl > 0 then
                redis.call('expire', resultKey, ttl)
            end

            -- Remove claim
            redis.call('hdel', claimKey, idempotencyKey)

            return 1";

        var ttlSeconds = (int?)ttl?.TotalSeconds ?? -1;
        var resultVal = await _database.ScriptEvaluateAsync(
            script,
            keys: ["idempotency:result", "idempotency:claim"],
            values: [idempotencyKey, result, ttlSeconds]);

        return (int)resultVal == 1;
    }

    /// <summary>
    /// Tries to claim an idempotency key for processing.
    /// Only succeeds if the key hasn't been claimed before.
    /// </summary>
    public async Task<bool> TryClaimAsync(string idempotencyKey, CancellationToken cancellationToken = default)
    {
        const string claimKey = "idempotency:claim";

        // Use SET NX (set if not exists) for atomic claim
        var claimed = await _database.HashSetAsync(claimKey, idempotencyKey, DateTime.UtcNow.Ticks);

        // Set expiration on claim hash to prevent stale claims
        await _database.KeyExpireAsync(claimKey, TimeSpan.FromMinutes(5));

        return claimed;
    }

    /// <summary>
    /// Gets the process count (0 or 1 in properly functioning idempotency).
    /// </summary>
    public async Task<int> GetProcessCountAsync(string idempotencyKey, CancellationToken cancellationToken = default)
    {
        const string hashKey = "idempotency:result";
        var exists = await _database.HashExistsAsync(hashKey, idempotencyKey);
        return exists ? 1 : 0;
    }

    /// <summary>
    /// Deletes an idempotency key and its result.
    /// </summary>
    public async Task<bool> DeleteAsync(string idempotencyKey, CancellationToken cancellationToken = default)
    {
        const string hashKey = "idempotency:result";
        const string claimKey = "idempotency:claim";

        // Remove from both result and claim hashes
        await _database.HashDeleteAsync(hashKey, idempotencyKey);
        var deleted = await _database.HashDeleteAsync(claimKey, idempotencyKey);

        return deleted;
    }

    /// <summary>
    /// Clears all idempotency keys (cleanup).
    /// </summary>
    public async Task ResetAsync(CancellationToken cancellationToken = default)
    {
        await _database.KeyDeleteAsync(["idempotency:result", "idempotency:claim"]);
    }
}

