# Distributed Idempotency Keys

A distributed idempotency key mechanism for ensuring exactly-once semantics and preventing duplicate operations in distributed systems. Idempotency keys cache operation results and return cached results for retries, preventing duplicate side effects.

## Overview

### The Problem: Duplicate Operations

In distributed systems with retries, the same operation may be executed multiple times:

- **Network timeouts**: Client retries, not knowing if first request succeeded
- **Failed requests**: Client retries on 5xx errors
- **Browser refreshes**: User clicks submit again
- **Message delivery guarantees**: At-least-once messaging guarantees
- **Load balancer retries**: Proxy retries requests on timeout

Each retry can cause duplicate side effects: duplicate charges, duplicate messages, duplicate database inserts.

### The Solution: Idempotency Keys

Idempotency keys solve this by:
1. Client sends unique key with operation request
2. Server checks if key has been processed before
3. If yes → return cached result immediately (no duplicate execution)
4. If no → execute operation, cache result, return it
5. Future retries with same key return cached result

Result: **exactly-once semantics** despite unlimited retries.

### Common Use Cases

- **Payment processing**: Prevent duplicate charges on retry
- **API endpoints**: Ensure safe retries without side effects
- **Message queues**: Deduplication when messages are redelivered
- **Database writes**: Prevent duplicate inserts
- **Account creation**: Prevent duplicate accounts on register retry
- **Order placement**: Prevent duplicate orders

## API

### Creating an Idempotency Key Manager

```csharp
using PutridParrot.Distributed.Patterns;
using StackExchange.Redis;

// Set up provider (e.g., Redis)
var redis = ConnectionMultiplexer.Connect("localhost:6379");
var provider = new RedisIdempotencyKeyProvider(redis);

// Create idempotency key manager
var options = new IdempotencyKeyOptions
{
	ResultTtl = TimeSpan.FromHours(1),        // Cache results for 1 hour
	MaxResultSizeBytes = 1024 * 1024,         // 1MB max result
	ClaimTimeout = TimeSpan.FromSeconds(30),  // Wait 30s for concurrent execution
	RetryDelay = TimeSpan.FromMilliseconds(100)
};

var idempotency = new IdempotencyKeyProvider(provider, options);
```

### Core Operations

#### GetOrExecuteAsync
Executes an operation exactly once per idempotency key, returning cached result on retries.

```csharp
// Execute operation with idempotency
var result = await idempotency.GetOrExecuteAsync(
	idempotencyKey: "order-123-payment",
	operation: async () =>
	{
		// This code executes at most once
		var charge = await ChargeCard(amount);
		return JsonSerializer.Serialize(charge);
	}
);

// Check if result was fresh or cached
if (result.IsFromCache)
{
	Console.WriteLine("Retry detected - returned cached result");
}
else
{
	Console.WriteLine("First execution - result is fresh");
}

// Use the result
var chargeResult = result.Result;
```

**Key Behavior**:
- First call: Executes operation, caches result, returns it
- Retry with same key: Returns cached result immediately (operation not re-executed)
- Concurrent calls: Only one executes; others wait for result

#### GetCachedResultAsync
Retrieves a cached result without executing any operation.

```csharp
// Check cache without executing
var cachedResult = await idempotency.GetCachedResultAsync(idempotencyKey);

if (cachedResult is not null)
{
	Console.WriteLine($"Result already cached: {cachedResult}");
}
else
{
	Console.WriteLine("Key not found or expired");
}
```

**Returns**: Cached result string, or `null` if not found

#### StoreCachedResultAsync
Manually stores a result against an idempotency key.

```csharp
// Pre-cache a result (useful for bulk operations, imports)
bool stored = await idempotency.StoreCachedResultAsync(
	idempotencyKey: "import-batch-001",
	result: JsonSerializer.Serialize(importResult)
);

if (!stored)
{
	Console.WriteLine("Key already exists - result was not stored");
}
```

**Returns**: `true` if stored, `false` if key already existed

#### DeleteAsync
Removes an idempotency key and clears its cached result.

```csharp
// Remove key (cleanup, reset scenarios)
bool deleted = await idempotency.DeleteAsync(idempotencyKey);

if (deleted)
{
	Console.WriteLine("Key deleted successfully");
}
else
{
	Console.WriteLine("Key not found");
}
```

## Configuration

`IdempotencyKeyOptions` controls idempotency key behavior:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ResultTtl` | `TimeSpan` | 1 hour | How long cached results persist |
| `MaxResultSizeBytes` | `int` | 1MB | Maximum result size (prevents memory abuse) |
| `ClaimTimeout` | `TimeSpan` | 30s | Timeout waiting for concurrent execution |
| `RetryDelay` | `TimeSpan` | 100ms | Delay between retry checks |

## Implementations

### Redis
- **Package**: StackExchange.Redis
- **Speed**: ~1-5ms per operation
- **Persistence**: No (lost on Redis restart)
- **Storage**: Hash-based with Lua scripts
- **Best for**: High-throughput scenarios; temporary idempotency

```csharp
var redis = ConnectionMultiplexer.Connect("localhost:6379");
var provider = new RedisIdempotencyKeyProvider(redis);
```

Uses Redis hashes to store results with automatic TTL expiration. Lua scripts ensure atomic claim and result storage operations.

**Data Structure**:
- `idempotency:result` hash - Stores key → result mappings
- `idempotency:claim` hash - Tracks which keys are being processed

### SQL Server
- **Package**: Microsoft.Data.SqlClient
- **Speed**: ~5-20ms per operation
- **Persistence**: Yes (stored in `IdempotencyKeys` table)
- **Storage**: SQL table with audit trail
- **Best for**: Durable idempotency; audit requirements

```csharp
string connectionString = "Server=localhost;Database=MyDb;...";
var provider = new SqlServerIdempotencyKeyProvider(connectionString);
```

Auto-creates `IdempotencyKeys` table on first use:
```sql
CREATE TABLE IdempotencyKeys (
	IdempotencyKey NVARCHAR(256) PRIMARY KEY,
	Result NVARCHAR(MAX) NOT NULL,
	ProcessCount INT NOT NULL DEFAULT 1,
	ClaimedAt DATETIME2 NOT NULL,
	ProcessedAt DATETIME2 NOT NULL,
	ExpiresAt DATETIME2 NOT NULL
)
```

### PostgreSQL
- **Package**: Npgsql
- **Speed**: ~5-15ms per operation
- **Persistence**: Yes (stored in `idempotency_keys` table)
- **Storage**: PostgreSQL table with indexes
- **Best for**: PostgreSQL-native environments; durable idempotency

```csharp
string connectionString = "Host=localhost;Database=distributed_patterns;Username=postgres;...";
var provider = new PostgreSqlIdempotencyKeyProvider(connectionString);
```

Auto-creates `idempotency_keys` table on first use:
```sql
CREATE TABLE idempotency_keys (
	idempotency_key TEXT PRIMARY KEY,
	result TEXT NOT NULL,
	process_count INT NOT NULL DEFAULT 1,
	claimed_at TIMESTAMP NOT NULL,
	processed_at TIMESTAMP NOT NULL,
	expires_at TIMESTAMP NOT NULL
)
```

## Examples

### Example 1: Payment Processing

Prevent duplicate charges on retry:

```csharp
var idempotency = new IdempotencyKeyProvider(provider, options);

// Client sends idempotency key with request
var paymentKey = request.Headers["Idempotency-Key"]; // e.g., UUID

var result = await idempotency.GetOrExecuteAsync(
	paymentKey,
	async () =>
	{
		// Charge card (only executes once)
		var charge = await PaymentGateway.ChargeAsync(
			cardToken: request.CardToken,
			amount: request.Amount
		);

		// Cache the charge result
		return JsonSerializer.Serialize(new
		{
			TransactionId = charge.Id,
			Amount = charge.Amount,
			Status = charge.Status
		});
	}
);

// Return transaction result
return new PaymentResponse
{
	TransactionId = JsonDocument.Parse(result.Result!).RootElement.GetProperty("TransactionId").GetString(),
	Cached = result.IsFromCache
};
```

### Example 2: API Endpoint Safety

Ensure POST endpoints are safe to retry:

```csharp
[HttpPost("/orders")]
public async Task<ActionResult<OrderResponse>> CreateOrder(
	[FromBody] CreateOrderRequest request,
	[FromHeader(Name = "Idempotency-Key")] string idempotencyKey)
{
	var result = await idempotency.GetOrExecuteAsync(
		idempotencyKey,
		async () =>
		{
			// Create order (executes exactly once)
			var order = await db.Orders.InsertAsync(new Order
			{
				CustomerId = request.CustomerId,
				Items = request.Items,
				TotalAmount = request.TotalAmount
			});

			return JsonSerializer.Serialize(new
			{
				OrderId = order.Id,
				Status = order.Status,
				CreatedAt = order.CreatedAt
			});
		}
	);

	return CreatedAtAction(nameof(GetOrder), result.Result);
}
```

### Example 3: Message Queue Deduplication

Handle at-least-once message delivery:

```csharp
// Message might be redelivered by queue
public async Task HandleAccountCreatedMessage(AccountCreatedEvent message)
{
	var result = await idempotency.GetOrExecuteAsync(
		idempotencyKey: $"event-{message.EventId}",
		operation: async () =>
		{
			// Send welcome email (executes exactly once)
			await EmailService.SendWelcomeAsync(message.Email);

			// Update user profile
			await UserService.UpdateWelcomeEmailSentAsync(message.UserId);

			return JsonSerializer.Serialize(new { Success = true });
		}
	);

	if (!result.IsFromCache)
	{
		logger.LogInformation($"Welcome email sent to {message.Email}");
	}
	else
	{
		logger.LogInformation($"Welcome email already sent to {message.Email} (deduplicated)");
	}
}
```

### Example 4: Database Insert Protection

Prevent duplicate inserts:

```csharp
public async Task<User> CreateUserAsync(CreateUserRequest request)
{
	var result = await idempotency.GetOrExecuteAsync(
		idempotencyKey: $"user-{request.Email}-{request.CreatedAt}",
		operation: async () =>
		{
			// Insert user (executes exactly once)
			var user = await db.Users.InsertAsync(new User
			{
				Email = request.Email,
				Name = request.Name,
				Password = HashPassword(request.Password)
			});

			return JsonSerializer.Serialize(new
			{
				UserId = user.Id,
				Email = user.Email
			});
		}
	);

	var userData = JsonDocument.Parse(result.Result!).RootElement;
	return new User
	{
		Id = userData.GetProperty("UserId").GetInt32(),
		Email = userData.GetProperty("Email").GetString()!
	};
}
```

### Example 5: Bulk Imports

Idempotent bulk operations:

```csharp
public async Task ImportUsersAsync(List<UserImportRow> rows)
{
	var result = await idempotency.GetOrExecuteAsync(
		idempotencyKey: $"import-batch-{batchId}",
		operation: async () =>
		{
			// Import all users (executes exactly once)
			var imported = 0;
			foreach (var row in rows)
			{
				try
				{
					await db.Users.InsertAsync(new User
					{
						Email = row.Email,
						Name = row.Name
					});
					imported++;
				}
				catch (DuplicateException)
				{
					// Already exists, skip
				}
			}

			return JsonSerializer.Serialize(new { ImportedCount = imported });
		}
	);

	Console.WriteLine($"Import batch completed: {result.Result}");
}
```

## Best Practices

### 1. Use UUID for Idempotency Keys

```csharp
// ✓ GOOD: UUID guaranteed unique
var key = Guid.NewGuid().ToString();

// ✓ GOOD: Semantic key with version
var key = $"order-{customerId}-{orderDate:yyyyMMdd}-v1";

// ❌ BAD: Timestamp not unique enough
var key = DateTime.Now.Ticks.ToString();

// ❌ BAD: Non-deterministic
var key = Random.Shared.Next().ToString();
```

### 2. Set Appropriate TTL

```csharp
// ✓ GOOD: Short TTL for transient operations
var options = new IdempotencyKeyOptions
{
	ResultTtl = TimeSpan.FromMinutes(5)  // API requests
};

// ✓ GOOD: Long TTL for durable operations
var options = new IdempotencyKeyOptions
{
	ResultTtl = TimeSpan.FromDays(7)  // Payment processing
};

// ❌ BAD: Too short (might expire mid-retry)
var options = new IdempotencyKeyOptions
{
	ResultTtl = TimeSpan.FromSeconds(1)
};
```

### 3. Include Idempotency Key in Request Headers

```csharp
// Client-side
using var request = new HttpRequestMessage(HttpMethod.Post, "/orders");
request.Headers.Add("Idempotency-Key", Guid.NewGuid().ToString());
request.Content = new StringContent(JsonSerializer.Serialize(order));

var response = await client.SendAsync(request);
```

### 4. Cache Result Size Matters

```csharp
// ✓ GOOD: Reasonable result size
var result = JsonSerializer.Serialize(new
{
	OrderId = order.Id,
	Status = order.Status,
	Total = order.Total
});  // ~100 bytes

// ⚠️ CAREFUL: Large result
var result = JsonSerializer.Serialize(order.LineItems.Select(item =>
	new { item.Id, item.Description, item.LongText, item.Image }
));  // Could be multi-MB

// ❌ BAD: Never cache entire database objects
var result = JsonSerializer.Serialize(entireDatabaseRow);
```

### 5. Handle Concurrent Requests Safely

```csharp
// Multiple identical requests arrive simultaneously
var tasks = new List<Task<IdempotencyKeyResult>>();

for (int i = 0; i < 10; i++)
{
	var task = idempotency.GetOrExecuteAsync(
		"same-key-for-all",
		async () => await ExpensiveOperation()
	);
	tasks.Add(task);
}

var results = await Task.WhenAll(tasks);
// ✓ ExpensiveOperation only runs once
// ✓ All 10 tasks get the same result
```

## Comparing to Other Patterns

| Pattern | Purpose | When to Use |
|---------|---------|------------|
| **Distributed Lock** | Mutual exclusion | Protecting critical sections |
| **Idempotency Key** | Exactly-once semantics | API retries, exactly-once processing |
| **Fence Token** | Prevent stale operations | Detecting split-brain scenarios |
| **Semaphore** | Resource capacity | Limiting concurrent access |

**Key Difference**: Locks prevent concurrent execution; idempotency keys accept retries and return cached results.

## Troubleshooting

### Result Size Exceeded

```csharp
try
{
	var result = await idempotency.GetOrExecuteAsync(key, operation);
}
catch (InvalidOperationException ex) when (ex.Message.Contains("exceeds maximum size"))
{
	Console.WriteLine("Result too large to cache");
	Console.WriteLine("Solutions:");
	Console.WriteLine("1. Return smaller result (omit verbose fields)");
	Console.WriteLine("2. Increase MaxResultSizeBytes in options");
}
```

### Key Expiration During Processing

```csharp
// If ClaimTimeout is too short, key might expire before claim completes
var options = new IdempotencyKeyOptions
{
	ClaimTimeout = TimeSpan.FromSeconds(30)  // Increase if operations are slow
};
```

### Concurrent Execution Not Detected

If concurrent requests aren't detected:

```csharp
// Ensure keys are identical across retries
// ❌ WRONG: Different key per request
var key = $"{operationName}-{Guid.NewGuid()}";

// ✓ RIGHT: Same key for retries
var key = request.Headers["Idempotency-Key"];
```

### Redis Persistence Loss

Idempotency keys in Redis are not persisted across restart:

```csharp
// Solution 1: Use SQL Server or PostgreSQL for durability
var provider = new SqlServerIdempotencyKeyProvider(connectionString);

// Solution 2: Use Redis with persistence enabled
// In Redis config: appendonly yes

// Solution 3: Accept data loss (acceptable for short-lived keys)
```

## Performance Characteristics

### Latency per Operation

| Provider | First Execution | Retry (cached) | GetCached |
|----------|-----------------|----------------|-----------|
| Redis | 1-5ms | 1-5ms | 1-5ms |
| SQL Server | 5-20ms | 5-20ms | 5-20ms |
| PostgreSQL | 5-15ms | 5-15ms | 5-15ms |

### Storage Requirements

Rough estimates for 1000 cached operations:

- **Redis**: ~100KB (depends on result size)
- **SQL Server**: ~500KB (with audit fields)
- **PostgreSQL**: ~500KB (with indexes)

### Scalability

- **Redis**: Excellent (thousands of ops/sec); in-memory
- **SQL Server/PostgreSQL**: Good (hundreds of ops/sec); I/O bound

## See Also

- [Distributed Locks](LOCK.md)
- [Distributed Semaphores](SEMAPHORE.md)
- [Distributed Fence Tokens](FENCE_TOKEN.md)
- [Distributed Rate Limiting](RATE_LIMITER.md)
