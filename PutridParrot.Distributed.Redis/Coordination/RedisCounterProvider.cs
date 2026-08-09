using PutridParrot.Distributed.Coordination;
using StackExchange.Redis;

namespace PutridParrot.Distributed.Redis.Coordination;

/// <summary>
/// Redis implementation of the distributed counter provider.
/// Supports both simple and sharded counters using atomic operations.
/// </summary>
public class RedisCounterProvider : IDistributedCounterProvider
{
    private readonly IDatabase _db;
    private const string CounterPrefix = "counter:";
    private const string ShardPrefix = "counter:shard:";

    /// <summary>
    /// Initializes a new instance of the RedisCounterProvider class.
    /// </summary>
    /// <param name="database">Redis database connection.</param>
    public RedisCounterProvider(IDatabase database)
    {
        _db = database ?? throw new ArgumentNullException(nameof(database));
    }

    public async Task EnsureInitializedAsync(CancellationToken cancellationToken = default)
    {
        // Redis doesn't require initialization; keys are created on first write
        await Task.CompletedTask;
    }

    public async Task<long> IncrementAsync(
        string counterName,
        long amount = 1,
        CounterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new CounterOptions();
        var key = GetCounterKey(counterName);

        if (options.ShardCount <= 1)
        {
            // Simple counter: direct increment
            var newValue = _db.StringIncrement(key, amount);

            // Set TTL if specified
            if (options.Ttl > TimeSpan.Zero)
            {
                _db.KeyExpire(key, options.Ttl);
            }

            return newValue;
        }
        else
        {
            // Sharded counter: increment a random shard
            var shardIndex = Random.Shared.Next(options.ShardCount);
            var shardKey = GetShardKey(counterName, shardIndex);
            _db.StringIncrement(shardKey, amount);

            // Set TTL if specified
            if (options.Ttl > TimeSpan.Zero)
            {
                _db.KeyExpire(shardKey, options.Ttl);
            }

            // Return the sum of all shards
            return await GetAsync(counterName, options, cancellationToken);
        }
    }

    public async Task<long> DecrementAsync(
        string counterName,
        long amount = 1,
        CounterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        return await IncrementAsync(counterName, -amount, options, cancellationToken);
    }

    public async Task<long> GetAsync(
        string counterName,
        CounterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new CounterOptions();

        if (options.ShardCount <= 1)
        {
            // Simple counter
            var key = GetCounterKey(counterName);
            var value = await _db.StringGetAsync(key);
            return value.HasValue ? long.Parse(value.ToString()) : options.InitialValue;
        }
        else
        {
            // Sharded counter: sum all shards
            var sum = 0L;
            for (int i = 0; i < options.ShardCount; i++)
            {
                var shardKey = GetShardKey(counterName, i);
                var value = await _db.StringGetAsync(shardKey);
                if (value.HasValue)
                {
                    sum += long.Parse(value.ToString());
                }
            }

            return sum != 0 ? sum : options.InitialValue;
        }
    }

    public async Task<long> SetAsync(
        string counterName,
        long value,
        CancellationToken cancellationToken = default)
    {
        var key = GetCounterKey(counterName);
        await _db.StringSetAsync(key, value.ToString());
        return value;
    }

    public async Task<CounterState> GetStateAsync(
        string counterName,
        CancellationToken cancellationToken = default)
    {
        var options = new CounterOptions();
        var currentValue = await GetAsync(counterName, options, cancellationToken);

        return new CounterState
        {
            CounterName = counterName,
            CurrentValue = currentValue,
            Timestamp = DateTime.UtcNow,
            ShardCount = options.ShardCount
        };
    }

    public async Task ResetAsync(
        string counterName,
        CounterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new CounterOptions();

        if (options.ShardCount <= 1)
        {
            var key = GetCounterKey(counterName);
            await _db.KeyDeleteAsync(key);
            if (options.InitialValue != 0)
            {
                await SetAsync(counterName, options.InitialValue, cancellationToken);
            }
        }
        else
        {
            // Delete all shards
            for (int i = 0; i < options.ShardCount; i++)
            {
                var shardKey = GetShardKey(counterName, i);
                await _db.KeyDeleteAsync(shardKey);
            }

            // Set initial value on first shard
            if (options.InitialValue != 0)
            {
                //var shardKey = GetShardKey(counterName, 0);
                await SetAsync(counterName, options.InitialValue, cancellationToken);
            }
        }
    }

    public async Task<bool> IncrementIfBelowAsync(
        string counterName,
        long maxValue,
        long amount = 1,
        CancellationToken cancellationToken = default)
    {
        var key = GetCounterKey(counterName);

        // Lua script for atomic check-and-increment
        var script = @"
            local current = tonumber(redis.call('GET', KEYS[1]) or 0)
            local maxVal = tonumber(ARGV[1])
            local incr = tonumber(ARGV[2])

            if current + incr <= maxVal then
                redis.call('INCRBY', KEYS[1], incr)
                return 1
            else
                return 0
            end
        ";

        var result = await _db.ScriptEvaluateAsync(script, keys: [key], values: [maxValue, amount]);
        return (long)result == 1;
    }

    private string GetCounterKey(string counterName) => $"{CounterPrefix}{counterName}";

    private string GetShardKey(string counterName, int shardIndex) => $"{ShardPrefix}{counterName}:{shardIndex}";
}
