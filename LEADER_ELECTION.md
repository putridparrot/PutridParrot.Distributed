# Distributed Leader Election

## Overview

Distributed Leader Election coordinates a single leader among multiple competing candidates across processes, servers, or regions. Essential for scenarios requiring global coordination: distributed task scheduling, cache invalidation coordination, system configuration consensus.

### Key Characteristics

- **Single Leader**: Only one candidate holds leadership at a time
- **Automatic Renewal**: Leaders heartbeat to maintain their role; missed renewals trigger new elections
- **Candidate Queuing**: Failed candidates are implicitly waiting; they can poll to detect when a new election window opens
- **Yielding**: Leaders can voluntarily step down, allowing immediate re-election
- **TTL-Based Failover**: If a leader crashes without yielding, its term expires and a new leader is elected

---

## API Reference

### DistributedLeaderElection

Main facade for leader election operations.

```csharp
public class DistributedLeaderElection
{
	// Constructor
	public DistributedLeaderElection(
		string leaderKey,
		IDistributedLeaderElectionProvider provider,
		LeaderElectionOptions? options = null);

	// Properties
	public string LeaderKey { get; }

	// Methods
	public Task<LeaderElectionResult> CandidateAsync(
		string candidateId,
		CancellationToken cancellationToken = default);

	public Task<LeaderElectionResult> RenewAsync(
		string candidateId,
		CancellationToken cancellationToken = default);

	public Task YieldAsync(
		string candidateId,
		CancellationToken cancellationToken = default);

	public Task<LeaderElectionState> GetLeaderAsync(
		CancellationToken cancellationToken = default);

	public Task<LeaderElectionState> WaitForLeaderChangeAsync(
		CancellationToken cancellationToken = default);

	public Task ResetAsync(
		CancellationToken cancellationToken = default);
}
```

### LeaderElectionOptions

Configuration for election behavior.

```csharp
public class LeaderElectionOptions
{
	// Timeout for candidate and renewal operations (default: 30 seconds)
	public TimeSpan CandidacyTimeout { get; set; }

	// Poll interval for leader change detection (default: 100 ms)
	public TimeSpan CheckInterval { get; set; }

	// TTL for leader state before expiry triggers new election (default: 1 minute)
	public TimeSpan StateTtl { get; set; }

	// Interval at which leader should renew (default: 10 seconds)
	public TimeSpan RenewalInterval { get; set; }
}
```

**Recommended Values**:
- `CandidacyTimeout`: 30–60 seconds
- `CheckInterval`: 100–500 ms (faster detection, higher CPU)
- `StateTtl`: Must be > RenewalInterval; recommend 3× RenewalInterval
- `RenewalInterval`: < StateTtl/2 to avoid missed renewals

### LeaderElectionResult

Result of a candidacy or renewal operation.

```csharp
public class LeaderElectionResult
{
	public bool IsSuccessful { get; set; }
	public string? CandidateId { get; set; }
	public string? LeaderId { get; set; }
	public DateTime Timestamp { get; set; }
	public LeaderElectionState? State { get; set; }
	public string? Message { get; set; }
}
```

### LeaderElectionState

Current leader state snapshot.

```csharp
public class LeaderElectionState
{
	public string? LeaderId { get; set; }
	public string? LeaderKey { get; set; }
	public DateTime? ElectedAt { get; set; }
	public DateTime? RenewalDeadline { get; set; }
	public int RenewalCount { get; set; }
}
```

---

## Backend Providers

### Redis Provider

**Strengths**:
- Fast, in-memory operations
- Atomic SET NX for non-blocking candidacy
- Lua scripts for safe renewal
- Minimal latency

**Implementation**:
- Leader ID stored in string key with TTL
- Candidacy: `SET leaderKey candidateId:timestamp NX` with TTL
- Renewal: Lua script atomically checks ownership and extends TTL
- Yield: `DEL leaderKey` if owned

**Best For**: High-throughput, low-latency scenarios; distributed caches, fast failover

### SQL Server Provider

**Strengths**:
- Persistent, transactional coordination
- Serializable isolation for strict ordering
- Renewal count tracking
- Audit trail via table history

**Implementation**:
- `LeaderElectionState` table: leaderId, leaderKey, electedAt, renewalDeadline, renewalCount
- Candidacy: MERGE with serializable transaction; checks for non-expired leader
- Renewal: UPDATE with WHERE clause ensuring ownership and non-expiry
- Yield: UPDATE to clear leaderId

**Best For**: Strongly consistent elections, audit requirements, durable state

### PostgreSQL Provider

**Strengths**:
- Persistent, with row-level locking
- Atomic INSERT...ON CONFLICT for coordinated updates
- Fast renewal with index lookups

**Implementation**:
- `leader_election_state` table: leader_id, leader_key, elected_at, renewal_deadline, renewal_count
- Candidacy: INSERT...ON CONFLICT DO UPDATE with FOR UPDATE locking
- Renewal: UPDATE checking ownership and deadline
- Yield: UPDATE to clear leader_id

**Best For**: Open-source preference, distributed PostgreSQL deployments

---

## Usage Patterns

### Pattern 1: Basic Election (Fire-and-Forget Leader)

```csharp
var provider = new RedisLeaderElectionProvider(redis);
var election = new DistributedLeaderElection("job-processor", provider);

var result = await election.CandidateAsync("node-1");
if (result.IsSuccessful)
{
	Console.WriteLine("I am the leader! Processing jobs...");
	// Do leader work
}
else
{
	Console.WriteLine($"I am a follower. Current leader: {result.LeaderId}");
}
```

### Pattern 2: Leadership with Heartbeat Renewal

```csharp
var options = new LeaderElectionOptions
{
	StateTtl = TimeSpan.FromSeconds(30),
	RenewalInterval = TimeSpan.FromSeconds(10)
};

var provider = new RedisLeaderElectionProvider(redis);
var election = new DistributedLeaderElection("task-scheduler", provider, options);

var candidacy = await election.CandidateAsync("scheduler-1");
if (candidacy.IsSuccessful)
{
	// Start background renewal task
	_ = Task.Run(async () =>
	{
		while (true)
		{
			await Task.Delay(options.RenewalInterval);
			var renewal = await election.RenewAsync("scheduler-1");
			if (!renewal.IsSuccessful)
			{
				Console.WriteLine("Lost leadership!");
				break;
			}
		}
	});

	// Do leader work
	await DoLeaderWork();
}
```

### Pattern 3: Follower Waits for Leadership Window

```csharp
var election = new DistributedLeaderElection("cluster-coordinator", provider);

var state = await election.GetLeaderAsync();
if (state.LeaderId == null)
{
	// No leader; try to acquire
	var result = await election.CandidateAsync("node-2");
}
else
{
	Console.WriteLine($"Current leader: {state.LeaderId}");
	Console.WriteLine("Waiting for leadership window...");

	// Block until leader changes or yields
	var newState = await election.WaitForLeaderChangeAsync();
	Console.WriteLine("Leadership window detected!");

	// Retry candidacy
	var result = await election.CandidateAsync("node-2");
}
```

### Pattern 4: Graceful Leadership Yield

```csharp
if (isShuttingDown)
{
	Console.WriteLine("Shutting down. Yielding leadership...");
	await election.YieldAsync("scheduler-1");
	// Other candidates can immediately acquire
}
```

---

## Best Practices

1. **Renewal Timing**
   - Renew well before deadline (recommend: RenewalInterval = StateTtl / 3)
   - Use background task to avoid blocking
   - Log renewal failures immediately

2. **Candidate ID Uniqueness**
   - Use hostname + PID or cluster node ID
   - Avoid reusing IDs within the same election key's TTL

3. **StateTtl Tuning**
   - Set high enough to avoid split-brain (network jitter buffer)
   - Set low enough for acceptable failover latency
   - Recommend: 3× RenewalInterval

4. **Follower Polling**
   - Use `WaitForLeaderChangeAsync()` for efficient waiting
   - Provide timeout to avoid indefinite blocking
   - Exponential backoff if election is highly contested

5. **Error Handling**
   - On renewal failure: assume leadership lost; stop leader work
   - On candidacy failure: log current leader ID and retry
   - Gracefully yield on shutdown to speed up failover

6. **Testing**
   - Reset state before each test via `ResetAsync()`
   - Simulate network delays with option overrides
   - Verify renewal count increment on successful renewal

---

## Troubleshooting

### Symptom: Candidate Cannot Acquire Leadership

**Cause**: Another leader's term has not yet expired.
**Solution**: Verify StateTtl > expected clock skew + network latency. Check leader state with `GetLeaderAsync()`.

### Symptom: Leadership Transfers Too Slowly

**Cause**: StateTtl is too high.
**Solution**: Reduce StateTtl, but ensure it's still > network jitter window.

### Symptom: Renewal Fails Despite Active Leader

**Cause**: Clock skew between client and backend; renewal deadline already passed.
**Solution**: Verify system clocks are synchronized (NTP). Increase RenewalInterval or reduce StateTtl slightly.

### Symptom: Split Brain (Multiple Leaders)

**Cause**: Backend provider not enforcing atomicity; race condition.
**Solution**: Ensure backend is configured for serializable transactions. Verify Lua script implementation in Redis. Check SQL Server isolation level.

### Symptom: Leader Cannot Yield / New Candidate Stuck

**Cause**: `YieldAsync()` failed silently; old leader still occupies slot.
**Solution**: Use `ResetAsync()` in tests. In production, wait for StateTtl to expire.

---

## Performance Characteristics

| Operation | Redis | SQL Server | PostgreSQL |
|-----------|-------|------------|-----------|
| **Candidacy** | ~5ms | ~50ms | ~30ms |
| **Renewal** | ~3ms | ~40ms | ~25ms |
| **Get State** | ~2ms | ~10ms | ~15ms |
| **Latency Variance** | Low | Medium | Medium |
| **Throughput** | High | Medium | High |
| **Persistence** | No | Yes | Yes |

---

## Examples

### Example 1: Basic Election

```csharp
var provider = new RedisLeaderElectionProvider(redis);
var election = new DistributedLeaderElection("app-leader", provider);
var result = await election.CandidateAsync("instance-1");
Console.WriteLine(result.IsSuccessful ? "Leader!" : $"Follower (leader: {result.LeaderId})");
```

### Example 2: Leadership with Renewal

```csharp
var options = new LeaderElectionOptions { RenewalInterval = TimeSpan.FromSeconds(5) };
var election = new DistributedLeaderElection("scheduler", provider, options);

if ((await election.CandidateAsync("node-1")).IsSuccessful)
{
	for (int i = 0; i < 10; i++)
	{
		await Task.Delay(options.RenewalInterval);
		var renewal = await election.RenewAsync("node-1");
		Console.WriteLine($"Renewal {i}: {(renewal.IsSuccessful ? "OK" : "Failed")}");
	}
}
```

### Example 3: Multi-Node Cluster

```csharp
var tasks = new List<Task>();
for (int i = 1; i <= 5; i++)
{
	var node = i;
	tasks.Add(Task.Run(async () =>
	{
		var result = await election.CandidateAsync($"node-{node}");
		Console.WriteLine($"Node {node}: {(result.IsSuccessful ? "Leader" : "Follower")}");
	}));
}
await Task.WhenAll(tasks);
```

---

## See Also

- [Distributed Locks](LOCK.md)
- [Distributed Fence Tokens](FENCE_TOKEN.md)
- [Distributed Barriers](BARRIER.md)
