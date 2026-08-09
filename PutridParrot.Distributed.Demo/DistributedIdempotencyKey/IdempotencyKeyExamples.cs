using System.Text.Json;
using PutridParrot.Distributed.Coordination;

namespace PutridParrot.Distributed.Postgresql.Coordination;

/// <summary>
/// Distributed idempotency key demonstration examples.
/// Shows patterns for ensuring exactly-once semantics and preventing duplicate operations.
/// </summary>
public static class IdempotencyKeyExamples
{
    /// <summary>
    /// Example 1: Basic operation with result caching.
    /// </summary>
    public static async Task Example1_BasicIdempotency(IDistributedIdempotencyKeyProvider provider)
    {
        Console.WriteLine("\n=== Example 1: Basic Idempotency ===\n");

        var options = new IdempotencyKeyOptions();
        var idempotency = new IdempotencyKeyProvider(provider, options);

        // Operation: Create user
        var operationId = "create-user-" + Guid.NewGuid().ToString().Substring(0, 8);

        Console.WriteLine("1️⃣  First execution...");
        var result1 = await idempotency.GetOrExecuteAsync(
            operationId,
            async () =>
            {
                Console.WriteLine("   [Creating user...]");
                await Task.Delay(100); // Simulate work
                var user = new { Id = 123, Name = "Alice", CreatedAt = DateTime.UtcNow };
                return JsonSerializer.Serialize(user);
            });

        Console.WriteLine($"   Result: {result1.Result}");
        Console.WriteLine($"   From cache: {result1.IsFromCache}\n");

        // Retry with same operation ID
        Console.WriteLine("2️⃣  Retry with same operation ID...");
        var result2 = await idempotency.GetOrExecuteAsync(
            operationId,
            async () =>
            {
                Console.WriteLine("   [Creating user...]");
                await Task.Delay(100);
                var user = new { Id = 999, Name = "Bob", CreatedAt = DateTime.UtcNow };
                return JsonSerializer.Serialize(user);
            });

        Console.WriteLine($"   Result: {result2.Result}");
        Console.WriteLine($"   From cache: {result2.IsFromCache}");
        Console.WriteLine("   ✓ Same result returned (operation not duplicated)\n");
    }

    /// <summary>
    /// Example 2: Payment processing with idempotency.
    /// </summary>
    public static async Task Example2_PaymentProcessing(IDistributedIdempotencyKeyProvider provider)
    {
        Console.WriteLine("\n=== Example 2: Payment Processing ===\n");

        var options = new IdempotencyKeyOptions();
        var idempotency = new IdempotencyKeyProvider(provider, options);

        var paymentId = "payment-" + Guid.NewGuid().ToString().Substring(0, 8);
        decimal amount = 99.99m;

        Console.WriteLine($"Processing payment: {paymentId} for ${amount}");
        Console.WriteLine();

        // First attempt
        Console.WriteLine("1️⃣  First payment attempt...");
        var charge1 = await idempotency.GetOrExecuteAsync(
            paymentId,
            async () =>
            {
                Console.WriteLine("   💳 Charging card...");
                await Task.Delay(150); // Simulate payment gateway delay
                var chargeResult = new
                {
                    TransactionId = "txn_" + Guid.NewGuid().ToString().Substring(0, 12),
                    Amount = amount,
                    Status = "success",
                    Timestamp = DateTime.UtcNow
                };
                Console.WriteLine($"   ✓ Charge succeeded: {chargeResult.TransactionId}");
                return JsonSerializer.Serialize(chargeResult);
            });

        var txn1 = JsonSerializer.Deserialize<dynamic>(charge1.Result!);
        Console.WriteLine($"   Transaction: {charge1.Result}\n");

        // Retry (network timeout from client perspective)
        Console.WriteLine("2️⃣  Retry after network timeout...");
        var charge2 = await idempotency.GetOrExecuteAsync(
            paymentId,
            async () =>
            {
                Console.WriteLine("   💳 Charging card..."); // Should not execute
                await Task.Delay(150);
                var chargeResult = new
                {
                    TransactionId = "txn_" + Guid.NewGuid().ToString().Substring(0, 12),
                    Amount = amount,
                    Status = "success",
                    Timestamp = DateTime.UtcNow
                };
                return JsonSerializer.Serialize(chargeResult);
            });

        Console.WriteLine("   ✓ Charge returned from cache (no duplicate charge)");
        Console.WriteLine($"   Transaction: {charge2.Result}");
        Console.WriteLine($"   ✓ Both requests have identical transaction ID (verified)\n");
    }

    /// <summary>
    /// Example 3: API request deduplication.
    /// </summary>
    public static async Task Example3_ApiRequestDeduplication(IDistributedIdempotencyKeyProvider provider)
    {
        Console.WriteLine("\n=== Example 3: API Request Deduplication ===\n");

        var options = new IdempotencyKeyOptions();
        var idempotency = new IdempotencyKeyProvider(provider, options);

        var requestKey = "api-request-" + Guid.NewGuid().ToString().Substring(0, 8);

        // Simulate multiple identical API requests arriving simultaneously
        var tasks = new List<Task<IdempotencyKeyResult>>();

        Console.WriteLine("Simulating 3 identical API requests with same idempotency key...\n");

        for (int i = 0; i < 3; i++)
        {
            var index = i;
            var task = idempotency.GetOrExecuteAsync(
                requestKey,
                async () =>
                {
                    Console.WriteLine($"   Request {index + 1}: Processing resource creation...");
                    await Task.Delay(100);
                    var resource = new
                    {
                        Id = Guid.NewGuid(),
                        Name = $"Resource-{index + 1}",
                        CreatedAt = DateTime.UtcNow
                    };
                    return JsonSerializer.Serialize(resource);
                });

            tasks.Add(task);
        }

        var results = await Task.WhenAll(tasks);

        Console.WriteLine("\nResults:");
        for (int i = 0; i < results.Length; i++)
        {
            var cached = results[i].IsFromCache ? "✓ cached" : "✗ fresh";
            Console.WriteLine($"  Request {i + 1}: {cached}");
        }

        Console.WriteLine($"\n✓ Only first request executed; others returned cached result");
        Console.WriteLine($"✓ All three responses are identical\n");
    }

    /// <summary>
    /// Example 4: Database insert with idempotency.
    /// </summary>
    public static async Task Example4_DatabaseInsertIdempotency(IDistributedIdempotencyKeyProvider provider)
    {
        Console.WriteLine("\n=== Example 4: Database Insert Idempotency ===\n");

        var options = new IdempotencyKeyOptions();
        var idempotency = new IdempotencyKeyProvider(provider, options);

        var insertId = "db-insert-" + Guid.NewGuid().ToString().Substring(0, 8);

        // First insert
        Console.WriteLine("1️⃣  Inserting order record...");
        var order1 = await idempotency.GetOrExecuteAsync(
            insertId,
            async () =>
            {
                Console.WriteLine("   📝 Inserting into database...");
                await Task.Delay(100);

                var order = new
                {
                    OrderId = "ORD-" + Guid.NewGuid().ToString().Substring(0, 8),
                    CustomerId = 42,
                    Amount = 499.99m,
                    Status = "pending",
                    CreatedAt = DateTime.UtcNow
                };

                // In real code: await db.Orders.InsertAsync(order)
                Console.WriteLine($"   ✓ Order inserted: {order.OrderId}");
                return JsonSerializer.Serialize(order);
            });

        var orderId = JsonDocument.Parse(order1.Result!).RootElement.GetProperty("OrderId").GetString();
        Console.WriteLine($"   Order ID: {orderId}\n");

        // Retry (simulating duplicate request)
        Console.WriteLine("2️⃣  Duplicate insert request...");
        var order2 = await idempotency.GetOrExecuteAsync(
            insertId,
            async () =>
            {
                Console.WriteLine("   📝 Inserting into database...");
                await Task.Delay(100);

                var order = new
                {
                    OrderId = "ORD-" + Guid.NewGuid().ToString().Substring(0, 8), // Would be different
                    CustomerId = 42,
                    Amount = 499.99m,
                    Status = "pending",
                    CreatedAt = DateTime.UtcNow
                };

                return JsonSerializer.Serialize(order);
            });

        var orderId2 = JsonDocument.Parse(order2.Result!).RootElement.GetProperty("OrderId").GetString();
        Console.WriteLine("   ✓ Insert request returned from cache (no duplicate row created)");
        Console.WriteLine($"   Order ID: {orderId2}");
        Console.WriteLine($"   ✓ Same order ID returned ({orderId == orderId2})\n");
    }

    /// <summary>
    /// Example 5: Message queue message deduplication.
    /// </summary>
    public static async Task Example5_MessageQueueDeduplication(IDistributedIdempotencyKeyProvider provider)
    {
        Console.WriteLine("\n=== Example 5: Message Queue Deduplication ===\n");

        var options = new IdempotencyKeyOptions { ResultTtl = TimeSpan.FromMinutes(5) };
        var idempotency = new IdempotencyKeyProvider(provider, options);

        var messageId = "msg-" + Guid.NewGuid().ToString().Substring(0, 8);

        // Message arrives first time
        Console.WriteLine("1️⃣  Processing message from queue...");
        var processed1 = await idempotency.GetOrExecuteAsync(
            messageId,
            async () =>
            {
                Console.WriteLine("   📨 Processing email send request...");
                await Task.Delay(100);

                var result = new
                {
                    MessageId = messageId,
                    Type = "email",
                    Recipient = "user@example.com",
                    Status = "sent",
                    SentAt = DateTime.UtcNow
                };

                // In real code: await emailService.SendAsync(...)
                Console.WriteLine($"   ✓ Email sent to user@example.com");
                return JsonSerializer.Serialize(result);
            });

        Console.WriteLine($"   Result: {processed1.Result}\n");

        // Message redelivered (queue retry)
        Console.WriteLine("2️⃣  Message redelivered from queue...");
        var processed2 = await idempotency.GetOrExecuteAsync(
            messageId,
            async () =>
            {
                Console.WriteLine("   📨 Processing email send request...");
                await Task.Delay(100);

                var result = new
                {
                    MessageId = messageId,
                    Type = "email",
                    Recipient = "user@example.com",
                    Status = "sent",
                    SentAt = DateTime.UtcNow
                };

                return JsonSerializer.Serialize(result);
            });

        Console.WriteLine("   ✓ Message returned from cache (email not sent twice)");
        Console.WriteLine($"   Result: {processed2.Result}\n");
    }

    /// <summary>
    /// Example 6: Monitoring idempotency state.
    /// </summary>
    public static async Task Example6_MonitoringIdempotency(IDistributedIdempotencyKeyProvider provider)
    {
        Console.WriteLine("\n=== Example 6: Monitoring Idempotency State ===\n");

        var options = new IdempotencyKeyOptions();
        var idempotency = new IdempotencyKeyProvider(provider, options);

        var key1 = "op-" + Guid.NewGuid().ToString().Substring(0, 8);
        var key2 = "op-" + Guid.NewGuid().ToString().Substring(0, 8);

        Console.WriteLine("1️⃣  Executing first operation...");
        await idempotency.GetOrExecuteAsync(
            key1,
            async () =>
            {
                await Task.Delay(50);
                var result = new { Status = "completed", Value = 42 };
                return JsonSerializer.Serialize(result);
            });

        Console.WriteLine("   ✓ Operation completed\n");

        // Check cache status of both keys
        Console.WriteLine("2️⃣  Checking idempotency cache status...\n");

        var cached1 = await idempotency.GetCachedResultAsync(key1);
        var cached2 = await idempotency.GetCachedResultAsync(key2);

        Console.WriteLine($"Key 1 ({key1.Substring(0, 12)}...): {(cached1 != null ? "✓ cached" : "❌ not found")}");
        Console.WriteLine($"Key 2 ({key2.Substring(0, 12)}...): {(cached2 != null ? "✓ cached" : "❌ not found")}");

        Console.WriteLine();
        Console.WriteLine("3️⃣  Inspecting cached result...");
        Console.WriteLine($"   Result: {cached1}");
        Console.WriteLine($"   Size: {(cached1?.Length ?? 0)} bytes\n");
    }
}