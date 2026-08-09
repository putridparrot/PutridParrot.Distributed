using PutridParrot.Distributed.Coordination;
using StackExchange.Redis;

namespace PutridParrot.Distributed.Redis.Coordination;

/// <summary>
/// Redis-backed implementation of distributed leader election.
/// Uses string keys with TTL for fast, non-blocking leadership coordination.
/// </summary>
public class RedisLeaderElectionProvider : IDistributedLeaderElectionProvider
{
    private readonly IDatabase _db;

    /// <summary>
    /// Initializes a new instance of the RedisLeaderElectionProvider.
    /// </summary>
    /// <param name="connectionMultiplexer">Redis connection multiplexer.</param>
    public RedisLeaderElectionProvider(IConnectionMultiplexer connectionMultiplexer)
    {
        _db = connectionMultiplexer.GetDatabase();
    }

    /// <summary>
    /// Attempts to acquire leadership using SET NX (set if not exists).
    /// </summary>
    public async Task<bool> CandidateAsync(
        string leaderKey,
        string candidateId,
        LeaderElectionOptions options,
        CancellationToken cancellationToken = default)
    {
        var timestamp = DateTime.UtcNow.Ticks.ToString();
        var value = $"{candidateId}:{timestamp}";

        // SET NX: only succeeds if key doesn't exist
        var result = await _db.StringSetAsync(
            leaderKey,
            value,
            options.StateTtl,
            When.NotExists);

        return result;
    }

    /// <summary>
    /// Renews leadership by checking ownership and extending TTL.
    /// Uses a Lua script for atomicity.
    /// </summary>
    public async Task<bool> RenewAsync(
        string leaderKey,
        string candidateId,
        LeaderElectionOptions options,
        CancellationToken cancellationToken = default)
    {
        // Lua script to atomically check owner and renew TTL
        var luaScript = @"
            if redis.call('exists', KEYS[1]) == 0 then
                return 0
            end
            local value = redis.call('get', KEYS[1])
            if value ~= nil and string.match(value, '^' .. ARGV[1] .. ':') then
                redis.call('pexpire', KEYS[1], tonumber(ARGV[2]))
                return 1
            end
            return 0
        ";

        var ttlMs = (long)options.StateTtl.TotalMilliseconds;
        var result = await _db.ScriptEvaluateAsync(
            luaScript,
            keys: [leaderKey],
            values: [candidateId, ttlMs]);

        return (long)result == 1;
    }

    /// <summary>
    /// Yields leadership by deleting the leader key.
    /// </summary>
    public async Task YieldAsync(
        string leaderKey,
        string candidateId,
        CancellationToken cancellationToken = default)
    {
        // Only delete if we own it (for safety)
        var value = await _db.StringGetAsync(leaderKey);
        if (value.HasValue && ((string)value!).StartsWith(candidateId + ":"))
        {
            await _db.KeyDeleteAsync(leaderKey);
        }
    }

    /// <summary>
    /// Retrieves the current leader state.
    /// </summary>
    public async Task<LeaderElectionState> GetLeaderAsync(
        string leaderKey,
        CancellationToken cancellationToken = default)
    {
        var value = await _db.StringGetAsync(leaderKey);
        var ttl = await _db.KeyTimeToLiveAsync(leaderKey);

        if (!value.HasValue)
        {
            return new LeaderElectionState
            {
                LeaderId = null,
                LeaderKey = leaderKey,
                ElectedAt = null,
                RenewalDeadline = null,
                RenewalCount = 0
            };
        }

        var parts = ((string)value!).Split(':');
        var leaderId = parts.Length > 0 ? parts[0] : null;
        var ticksStr = parts.Length > 1 ? parts[1] : "0";

        var electedAt = long.TryParse(ticksStr, out var ticks)
            ? new DateTime(ticks, DateTimeKind.Utc)
            : DateTime.UtcNow;

        var renewalDeadline = (ttl.HasValue && ttl.Value > TimeSpan.Zero)
            ? (DateTime?)DateTime.UtcNow.Add(ttl.Value)
            : null;

        return new LeaderElectionState
        {
            LeaderId = leaderId,
            LeaderKey = leaderKey,
            ElectedAt = electedAt,
            RenewalDeadline = renewalDeadline,
            RenewalCount = 0 // Redis doesn't track renewal count; this would need a separate counter
        };
    }

    /// <summary>
    /// Resets the leader election state by deleting the key.
    /// </summary>
    public async Task ResetAsync(
        string leaderKey,
        CancellationToken cancellationToken = default)
    {
        await _db.KeyDeleteAsync(leaderKey);
    }
}
