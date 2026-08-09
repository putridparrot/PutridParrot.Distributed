# Distributed Counter / Sharded Counter

## Overview

Distributed Counter / Sharded Counter is an atomic counter abstraction for distributed systems. It allows multiple processes/machines to safely increment and decrement a shared counter, with optional sharding to reduce contention on hot keys.

### Use Cases

- **Metrics and statistics**: Track events across multiple services
- **Resource quotas**: Count resource usage against limits
- **Rate limiting**: Track request counts for rate limiting policies
- **Progress tracking**: Monitor batch job progress across workers
- **Inventory management**: Atomic stock/capacity tracking
- **Hit counters**: Analytics and popularity metrics

### Counter Types

1. **Simple Counter**: Single atomic counter, suitable for low to moderate contention
2. **Sharded Counter**: Multiple partitions (shards), summed on reads, suitable for high-contention scenarios

---

## Architecture

### Simple Counter

```
Single key in backend store
	  ↓
  counter: N
	  ↓
Fast increments/decrements via atomic operations
```

**Characteristics**:
- Minimal memory overhead
- Lower read latency (single key lookup)
- Better for low-contention scenarios
- Simpler debugging

### Sharded Counter

```
Multiple partition keys in backend store
	  ↓
counter:shard:0 = X
counter:shard:1 = Y
counter:shard:2 = Z
	  ↓
Reads: sum all shards
Writes: distribute across random shards
```

**Characteristics**:
- Reduced write contention via distribution
- Higher read latency (must sum all shards)
- Better for high-contention scenarios
- More complex but scalable

---

## API Reference

### DistributedCounter

Main facade for counter operations.

```csharp
public class DistributedCounter
{
	// Constructor
	public DistributedCounter(
		string counterName,
		IDistributedCounterProvider provider,
		CounterOptions? options = null);

	// Properties
	public string CounterName { get; }

	// Methods
	public Task<CounterResult> IncrementAsync(CancellationToken cancellationToken = default);
	public Task<CounterResult> IncrementAsync(long amount, CancellationToken cancellationToken = default);
	public Task<CounterResult> DecrementAsync(CancellationToken cancellationToken = default);
	public Task<CounterResult> DecrementAsync(long amount, CancellationToken cancellationToken = default);

	public Task<long> GetAsync(CancellationToken cancellationToken = default);
	public Task<CounterResult> GetResultAsync(CancellationToken cancellationToken = default);
	public Task<CounterResult> SetAsync(long value, CancellationToken cancellationToken = default);

	public Task<CounterState> GetStateAsync(CancellationToken cancellationToken = default);
	public Task ResetAsync(CancellationToken cancellationToken = default);

	public Task<bool> IncrementIfBelowAsync(long maxValue, long amount = 1, CancellationToken = default);
	public Task<CounterResult> IncrementIfBelowResultAsync(long maxValue, long amount = 1, CancellationToken = default);

	public Task<long> ApplyBatchAsync(IEnumerable<long> operations, CancellationToken = default);
	public Task<double> GetPercentageAsync(long maxValue, CancellationToken = default);
}
```

### CounterOptions

Configuration for counter behavior.

```csharp
public class CounterOptions
{
	// Initial value when counter is first created (default: 0)
	public long InitialValue { get; set; }

	// Maximum allowed value; prevents overflow (default: long.MaxValue)
	public long MaxValue { get; set; }

	// Number of shards for sharded counter (default: 1)
	// Higher values reduce contention but increase memory and read latency
	public int ShardCount { get; set; }

	// Clamp values at MaxValue instead of throwing (default: true)
	public bool ClampAtMax { get; set; }

	// Time-to-live for the counter (default: no expiration)
	public TimeSpan Ttl { get; set; }
}
```

### CounterState

Snapshot of counter state.

```csharp
public class CounterState
{
	public string? CounterName { get; set; }
	public long CurrentValue { get; set; }
	public DateTime Timestamp { get; set; }
	public int ShardCount { get; set; }
}
```

---

## Backend Providers

### Redis Provider

**Characteristics**:
- Fast: ~1-5ms per operation
- Non-blocking, fully concurrent
- INCR/INCRBY for simple mode
- Lua scripts for sharding and conditional operations
- No persistence (data lost on restart)

**Best For**: Development, testing, caches with rebuild capability

**Example**:
```csharp
var redis = ConnectionMultiplexer.Connect("localhost:6379");
var provider = new RedisCounterProvider(redis.GetDatabase());
var counter = new DistributedCounter("events", provider, new CounterOptions 
{ 
	ShardCount = 10  // Use sharded counter for high contention
});

await counter.IncrementAsync(5);  // Increment by 5
```

### SQL Server Provider

**Characteristics**:
- Persistent: data survives restarts
- Strong consistency with serializable transactions
- ~20-50ms per operation (network + disk)
- Query-friendly (can analyze counter data via SQL)

**Best For**: Mission-critical counters, audit trails, queries

**Example**:
```csharp
var connectionString = "Server=localhost;Database=CounterDb;Integrated Security=true;";
var provider = new SqlServerCounterProvider(connectionString);
var counter = new DistributedCounter("revenue", provider);

await counter.IncrementAsync(amount: 100);
var value = await counter.GetAsync();
```

### PostgreSQL Provider

**Characteristics**:
- Persistent: full durability
- Faster than SQL Server for upserts (INSERT...ON CONFLICT)
- ~15-30ms per operation
- Open-source alternative

**Best For**: PostgreSQL infrastructure, cost-conscious deployments

**Example**:
```csharp
var connectionString = "Host=localhost;Database=counters;Username=postgres;";
var provider = new PostgreSqlCounterProvider(connectionString);
var counter = new DistributedCounter("api-calls", provider, new CounterOptions 
{ 
	MaxValue = 10000,
	ShardCount = 5
});

var allowed = await counter.IncrementIfBelowAsync(maxValue: 10000);
if (!allowed)
{
	// Rate limit exceeded
}
```

---

## Usage Patterns

### Pattern 1: Simple Counting

```csharp
var counter = new DistributedCounter("page-views", provider);

// Increment on each page view
await counter.IncrementAsync();

// Get current count
var views = await counter.GetAsync();
Console.WriteLine($"Total page views: {views}");
```

### Pattern 2: Rate Limiting with Max Value

```csharp
var options = new CounterOptions { MaxValue = 100 };  // Max 100 requests per window
var counter = new DistributedCounter("api-quota", provider, options);

// On each request:
var allowed = await counter.IncrementIfBelowAsync(maxValue: 100, amount: 1);
if (!allowed)
{
	return new RateLimitExceeded();
}
```

### Pattern 3: Progress Tracking

```csharp
var total = 1000;
var counter = new DistributedCounter("job-progress", provider);

// Each worker increments as it completes items
for (int i = 0; i < itemsPerWorker; i++)
{
	ProcessItem(items[i]);
	await counter.IncrementAsync();

	var percentage = await counter.GetPercentageAsync(total);
	Console.WriteLine($"Progress: {percentage:F1}%");
}
```

### Pattern 4: Sharded Counter for High Contention

```csharp
// 10 shards reduces contention by 10x
var options = new CounterOptions { ShardCount = 10 };
var counter = new DistributedCounter("events", provider, options);

// Increments are distributed across 10 shards
await counter.IncrementAsync();  // Random shard +1

// Gets sum all shards
var total = await counter.GetAsync();
```

### Pattern 5: Batch Operations

```csharp
var operations = new[] { 10L, -2L, 5L };  // +10, -2, +5
var finalValue = await counter.ApplyBatchAsync(operations);
// Equivalent to: value + 10 - 2 + 5 = value + 13
```

### Pattern 6: Snapshot and Reset

```csharp
// Get current state
var state = await counter.GetStateAsync();
Console.WriteLine($"Counter: {state.CurrentValue} at {state.Timestamp}");

// Reset to initial value
await counter.ResetAsync();
```

---

## Best Practices

### Choosing Between Simple and Sharded

| Scenario | Simple | Sharded |
|----------|--------|---------|
| Low contention (<1K ops/sec) | ✓ | ✓ |
| Moderate contention (1K-10K ops/sec) | ✓ | - |
| High contention (>10K ops/sec) | ✗ | ✓ |
| Frequent reads | ✓ | ✗ |
| Rare reads | ✓ | ✓ |

### Memory and Performance Trade-offs

**Simple Counter**:
- Memory: Single key (< 1KB per counter)
- Write latency: ~1-2ms (Redis), ~20-30ms (SQL)
- Read latency: Same as write
- Best for: Most use cases

**Sharded Counter (10 shards)**:
- Memory: 10 keys (~5KB per counter)
- Write latency: ~1-2ms (distributed)
- Read latency: 10x higher (sum all shards)
- Best for: Write-heavy, read-light scenarios

### Monitoring

Track these metrics:
- **Counter value over time**: Detect trends and anomalies
- **Write throughput**: Monitor contention
- **Read latency**: Identify performance degradation
- **Max value breaches**: Alert on quota exhaustion

### Error Handling

```csharp
try
{
	var result = await counter.IncrementIfBelowResultAsync(maxValue: 1000);
	if (!result.IsSuccessful)
	{
		// Counter at or above max
		logger.LogWarning(result.Message);
	}
}
catch (Exception ex)
{
	logger.LogError(ex, "Counter operation failed");
	// Decide: retry, fallback, or fail
}
```

### Thread Safety

All counter operations are thread-safe and fully concurrent. Multiple workers can safely call increment/decrement simultaneously.

---

## Performance Characteristics

| Operation | Redis | SQL Server | PostgreSQL |
|-----------|-------|------------|-----------|
| **Increment (simple)** | 1ms | 25ms | 15ms |
| **Increment (10 shards)** | 1ms | 25ms | 15ms |
| **Decrement** | 1ms | 25ms | 15ms |
| **Get (simple)** | 0.5ms | 10ms | 8ms |
| **Get (10 shards)** | 5ms | 50ms | 40ms |
| **Set** | 1ms | 20ms | 12ms |
| **Throughput (simple)** | 100K+ ops/s | 5K ops/s | 8K ops/s |
| **Throughput (10 shards)** | 100K+ ops/s | 5K ops/s | 8K ops/s |

---

## Troubleshooting

### Symptom: Accuracy Issues with Sharded Counter

**Cause**: Sum across shards is eventually consistent (intermediate state during writes).
**Solution**: Sharded counters are approximate. For exact counts, use simple counter. Document the tolerance level.

### Symptom: Slow Reads with Many Shards

**Cause**: Reading requires summing all shard keys.
**Solution**: Reduce shard count if reads are critical. Use simple counter if reads > writes.

### Symptom: Counter Wraps Around at long.MaxValue

**Cause**: No overflow protection; behavior depends on backend.
**Solution**: Set `MaxValue` in `CounterOptions` to prevent overflow. Set `ClampAtMax = true`.

### Symptom: TTL Not Working

**Cause**: Only Redis provider supports TTL; SQL backends don't expire keys.
**Solution**: Implement manual cleanup for SQL providers, or use Redis for time-limited counters.

---

## Examples

### Example 1: Event Counter

```csharp
var counter = new DistributedCounter("user-logins", provider);

// On each login
await counter.IncrementAsync();

// Analytics query
var logins = await counter.GetAsync();
Console.WriteLine($"Total logins today: {logins}");
```

### Example 2: Resource Quota

```csharp
var options = new CounterOptions { MaxValue = 100 };  // Max 100 concurrent
var counter = new DistributedCounter("concurrent-users", provider, options);

// User connects
var allowed = await counter.IncrementIfBelowAsync(100, amount: 1);
if (allowed)
{
	// Connection established
}
else
{
	// Server at capacity
}

// User disconnects
await counter.DecrementAsync();
```

### Example 3: Sharded Event Tracking

```csharp
var options = new CounterOptions { ShardCount = 20 };  // 20 shards for high throughput
var counter = new DistributedCounter("api-requests", provider, options);

// High-concurrency request handler
await counter.IncrementAsync();

// Periodic snapshot
var state = await counter.GetStateAsync();
logger.LogMetric("ApiRequests", state.CurrentValue);
```

---

## See Also

- [Distributed Rate Limiting](RATE_LIMITER.md)
- [Distributed Semaphores](SEMAPHORE.md)
- [Distributed Queue](QUEUE.md)
