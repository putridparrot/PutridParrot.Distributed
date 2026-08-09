# Distributed Queue / Work Dispatcher

## Overview

Distributed Queue / Work Dispatcher is a producer-consumer pattern for distributed work processing. It allows multiple workers to reliably process tasks enqueued from multiple producers, with built-in support for:

- **Visibility timeouts**: Automatic item re-release if worker crashes
- **Retry semantics**: Configurable max attempts before dead lettering
- **Priority queuing**: Process higher-priority items first
- **Dead letter queue**: Failed items sent to DLQ for inspection and recovery
- **Multi-worker coordination**: Multiple workers claim and process items atomically

### Use Cases

- **Background job processing**: Async task execution across multiple servers
- **Event-driven workflows**: Process events from various sources
- **Load distribution**: Spread work across available worker instances
- **Resilient task execution**: Automatic retry with dead-letter fallback
- **Priority-based workloads**: Process urgent items before routine ones

---

## API Reference

### DistributedQueue

Main facade for queue operations.

```csharp
public class DistributedQueue
{
	// Constructor
	public DistributedQueue(
		string queueName,
		IDistributedQueueProvider provider,
		QueueOptions? options = null);

	// Properties
	public string QueueName { get; }

	// Methods
	public Task<QueueResult> EnqueueAsync(
		string payload,
		int priority = 0,
		CancellationToken cancellationToken = default);

	public Task<QueueResult> DequeueAsync(
		string workerId,
		CancellationToken cancellationToken = default);

	public Task AcknowledgeAsync(
		string workItemId,
		CancellationToken cancellationToken = default);

	public Task NackAsync(
		string workItemId,
		string? errorMessage = null,
		CancellationToken cancellationToken = default);

	public Task MoveToDeadLetterAsync(
		string workItemId,
		string reason,
		CancellationToken cancellationToken = default);

	public Task<QueueState> GetStateAsync(CancellationToken cancellationToken = default);

	public Task ResetAsync(CancellationToken cancellationToken = default);

	public Task<IEnumerable<WorkItem>> GetDeadLetterItemsAsync(
		CancellationToken cancellationToken = default);

	public Task ProcessNextAsync(
		string workerId,
		Func<WorkItem, Task> processFunc,
		CancellationToken cancellationToken = default);
}
```

### QueueOptions

Configuration for queue behavior.

```csharp
public class QueueOptions
{
	// Visibility timeout before item re-released (default: 30s)
	public TimeSpan VisibilityTimeout { get; set; }

	// Max attempts before dead letter (default: 3)
	public int MaxAttempts { get; set; }

	// Time-to-live for items (default: 1 hour)
	public TimeSpan ItemTtl { get; set; }

	// Enable priority queuing (default: false)
	public bool EnablePriorityQueue { get; set; }

	// Poll timeout for blocking dequeue (default: 1s)
	public TimeSpan PollTimeout { get; set; }
}
```

### WorkItem

Represents a work item in the queue.

```csharp
public class WorkItem
{
	public string? Id { get; set; }
	public string? Payload { get; set; }
	public WorkItemState State { get; set; }
	public DateTime EnqueuedAt { get; set; }
	public DateTime UpdatedAt { get; set; }
	public DateTime? VisibilityDeadline { get; set; }
	public int AttemptCount { get; set; }
	public int Priority { get; set; }
	public string? WorkerId { get; set; }
	public string? ErrorMessage { get; set; }
}
```

### WorkItemState

Enum representing work item states.

```csharp
public enum WorkItemState
{
	Pending = 0,       // Waiting to be claimed
	Processing = 1,    // Claimed by a worker
	Completed = 2,     // Successfully processed
	DeadLetter = 3,    // Failed too many times
	Acknowledged = 4   // Processed and confirmed
}
```

### QueueState

Current queue state snapshot.

```csharp
public class QueueState
{
	public string? QueueName { get; set; }
	public long PendingCount { get; set; }       // Items waiting to be processed
	public long ProcessingCount { get; set; }    // Items currently being processed
	public long DeadLetterCount { get; set; }    // Items that failed
	public long CompletedCount { get; set; }     // Items successfully processed
	public long TotalProcessed { get; set; }     // Total items ever enqueued
	public DateTime Timestamp { get; set; }
}
```

---

## Backend Providers

### Redis Provider

**Strengths**:
- Fast, in-memory operations (~5-10ms per operation)
- Non-blocking, highly concurrent
- Natural support for sorted sets (priority)
- Minimal infrastructure overhead

**Implementation**:
- Sorted sets for priority-ordered pending/processing queues
- String keys for work item storage
- Score = priority (pending), timestamp (processing)
- Hash-based work item serialization

**Best For**: Real-time background jobs, high-throughput processing, development/testing

### SQL Server Provider

**Strengths**:
- Persistent storage with guaranteed durability
- Transactional consistency with serializable isolation
- Query-based filtering and analytics
- Integration with existing DW/BI systems

**Implementation**:
- `WorkItems` table with state/worker tracking
- `QueueStats` table for aggregated metrics
- Serializable transactions for atomic dequeue+update
- Indexed queries on queue_name and state

**Best For**: Mission-critical systems, long-term audit trail, queues requiring strong consistency

### PostgreSQL Provider

**Strengths**:
- Open-source alternative to SQL Server
- Fast atomic INSERT...ON CONFLICT operations
- Row-level locking with SKIP LOCKED
- Cost-effective for large-scale deployments

**Implementation**:
- `work_items` table with index on (queue_name, state, priority)
- `queue_stats` table with upsert-based updates
- Snapshot isolation for concurrency
- FOR UPDATE SKIP LOCKED for non-blocking dequeue

**Best For**: PostgreSQL-standardized infrastructure, cost-conscious deployments, hybrid on-premise/cloud

---

## Usage Patterns

### Pattern 1: Fire-and-Forget Enqueueing

```csharp
// Enqueue a task
await queue.EnqueueAsync("{ \"email\": \"user@example.com\" }");

// Worker picks it up automatically
```

### Pattern 2: Priority-Based Processing

```csharp
// Urgent task
await queue.EnqueueAsync(payload, priority: 10);

// Routine task
await queue.EnqueueAsync(payload, priority: 1);

// Workers always process high-priority first
```

### Pattern 3: Automatic Retry and Backoff

```csharp
var options = new QueueOptions
{
	MaxAttempts = 5,
	VisibilityTimeout = TimeSpan.FromSeconds(30)
};

// Worker dequeues
var item = await queue.DequeueAsync(workerId);

if (processingFails)
{
	// Nack returns item to pending for retry
	await queue.NackAsync(item.Id, "Timeout waiting for service");
}
else
{
	// Success
	await queue.AcknowledgeAsync(item.Id);
}
```

### Pattern 4: Inspect Dead Letter Queue

```csharp
// Retrieve failed items
var dlqItems = await queue.GetDeadLetterItemsAsync();

foreach (var item in dlqItems)
{
	Console.WriteLine($"Failed: {item.ErrorMessage} (Attempts: {item.AttemptCount})");
}

// Optionally retry or analyze
```

### Pattern 5: Concurrent Worker Pool

```csharp
// Multiple workers processing in parallel
var workers = new[] { "worker-1", "worker-2", "worker-3" };

var tasks = workers.Select(async workerId =>
{
	while (!cancellationToken.IsCancellationRequested)
	{
		var result = await queue.DequeueAsync(workerId);
		if (result.IsSuccessful)
		{
			await ProcessAsync(result.WorkItem);
			await queue.AcknowledgeAsync(result.WorkItem.Id);
		}
	}
});

await Task.WhenAll(tasks);
```

### Pattern 6: ProcessNext Helper (Automatic Ack/Nack)

```csharp
// Simplified: automatic ack on success, nack on exception
await queue.ProcessNextAsync("worker-1", async item =>
{
	// Your processing logic here
	await SendEmailAsync(item.Payload);

	// If this throws, ProcessNext automatically nacks
	// If it succeeds, ProcessNext automatically acks
});
```

---

## Best Practices

1. **Visibility Timeout Tuning**
   - Set to 2-3x expected processing time
   - Too short: items re-processed prematurely
   - Too long: slow recovery from worker crashes

2. **Max Attempts Strategy**
   - 3-5 attempts for transient failures
   - 1 attempt for permanent failures (bad data)
   - Use error message to distinguish

3. **Payload Design**
   - Keep payloads small (< 1MB)
   - Use JSON for serialization
   - Include retry context (attempt count, error history)

4. **Worker Design**
   - Long-running vs. short-running workers
   - Graceful shutdown: finish in-flight items
   - Monitor for stuck workers (no ack/nack)

5. **Dead Letter Handling**
   - Implement alerts on DLQ growth
   - Automate analysis and recovery workflows
   - Archive DLQ items for compliance

6. **Monitoring**
   - Track pending count (backlog)
   - Track processing count (utilization)
   - Track dead-letter count (error rate)
   - Alert on high DLQ ratios or backlog buildup

---

## Troubleshooting

### Symptom: Items Stay in Processing Queue

**Cause**: Worker crashed without nacking; visibility timeout not set correctly.
**Solution**: Reduce VisibilityTimeout. Implement worker heartbeat. Check backend logs.

### Symptom: High Dead Letter Rate

**Cause**: Transient failures treated as permanent; worker misconfiguration.
**Solution**: Increase MaxAttempts. Implement exponential backoff in nack logic. Fix worker.

### Symptom: Uneven Work Distribution

**Cause**: Workers with different speed; queue state lag.
**Solution**: Use fair dequeue (round-robin). Monitor worker performance. Re-balance if needed.

### Symptom: Lost Items

**Cause**: Queue not persisting (Redis lost); worker ack before confirming.
**Solution**: Use SQL Server or PostgreSQL. Always ack after confirming successful processing.

---

## Performance Characteristics

| Operation | Redis | SQL Server | PostgreSQL |
|-----------|-------|------------|-----------|
| **Enqueue** | ~2ms | ~30ms | ~20ms |
| **Dequeue** | ~3ms | ~50ms | ~25ms |
| **Acknowledge** | ~1ms | ~20ms | ~15ms |
| **Nack** | ~2ms | ~30ms | ~20ms |
| **GetState** | ~1ms | ~15ms | ~10ms |
| **Throughput** | 100K+ items/s | 5K+ items/s | 10K+ items/s |
| **Persistence** | None (volatile) | Full (durable) | Full (durable) |

---

## Examples

### Example 1: Basic Enqueue/Dequeue

```csharp
// Enqueue
var result = await queue.EnqueueAsync("{ \"task\": \"send-email\" }");
Console.WriteLine($"Enqueued: {result.WorkItem.Id}");

// Dequeue
var dequeueResult = await queue.DequeueAsync("worker-1");
if (dequeueResult.IsSuccessful)
{
	Console.WriteLine($"Claimed: {dequeueResult.WorkItem.Payload}");

	// Process...
	await queue.AcknowledgeAsync(dequeueResult.WorkItem.Id);
}
```

### Example 2: Priority Queue

```csharp
// Enqueue with priorities
await queue.EnqueueAsync(urgentTask, priority: 100);
await queue.EnqueueAsync(normalTask, priority: 10);
await queue.EnqueueAsync(lowTask, priority: 1);

// Urgent task is always dequeued first
var first = await queue.DequeueAsync("worker-1");  // urgentTask
```

### Example 3: Retry Logic

```csharp
var options = new QueueOptions { MaxAttempts = 3 };
var item = await queue.DequeueAsync("worker-1");

try
{
	await ProcessAsync(item.Payload);
	await queue.AcknowledgeAsync(item.Id);
}
catch (TransientException ex)
{
	if (item.AttemptCount < options.MaxAttempts)
	{
		await queue.NackAsync(item.Id, ex.Message);  // Will retry
	}
}
catch (PermanentException ex)
{
	await queue.MoveToDeadLetterAsync(item.Id, ex.Message);  // Skip retry
}
```

---

## See Also

- [Distributed Rate Limiting](RATE_LIMITER.md)
- [Distributed Semaphores](SEMAPHORE.md)
- [Distributed Leader Election](LEADER_ELECTION.md)
