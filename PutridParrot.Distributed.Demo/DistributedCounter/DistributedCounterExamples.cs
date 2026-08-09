namespace PutridParrot.Distributed.Demo.DistributedCounter;

/// <summary>
/// Interactive examples demonstrating distributed counter patterns.
/// </summary>
public class DistributedCounterExamples
{
    private readonly Coordination.DistributedCounter _counter;

    /// <summary>
    /// Initializes a new instance of the DistributedCounterExamples class.
    /// </summary>
    /// <param name="counter">The distributed counter instance.</param>
    public DistributedCounterExamples(Coordination.DistributedCounter counter)
    {
        _counter = counter;
    }

    /// <summary>
    /// Example 1: Basic increment and decrement operations.
    /// </summary>
    public async Task Example1_BasicOperationsAsync()
    {
        Console.WriteLine("\n=== Example 1: Basic Increment/Decrement ===");

        // Reset counter
        await _counter.ResetAsync();
        Console.WriteLine($"Counter reset to 0");

        // Increment
        for (int i = 0; i < 5; i++)
        {
            var result = await _counter.IncrementAsync();
            Console.WriteLine($"  Increment: {result.Value}");
        }

        var currentValue = await _counter.GetAsync();
        Console.WriteLine($"Current value: {currentValue}");

        // Decrement
        for (int i = 0; i < 2; i++)
        {
            var result = await _counter.DecrementAsync();
            Console.WriteLine($"  Decrement: {result.Value}");
        }

        currentValue = await _counter.GetAsync();
        Console.WriteLine($"Final value: {currentValue}");
    }

    /// <summary>
    /// Example 2: Increment by larger amounts.
    /// </summary>
    public async Task Example2_BatchIncrementsAsync()
    {
        Console.WriteLine("\n=== Example 2: Batch Increments ===");

        await _counter.ResetAsync();
        Console.WriteLine("Counter reset to 0");

        // Increment by various amounts
        var amounts = new[] { 10, 5, 3, 7 };
        Console.WriteLine("\nIncrementing by: " + string.Join(", ", amounts));

        foreach (var amount in amounts)
        {
            var result = await _counter.IncrementAsync(amount);
            Console.WriteLine($"  +{amount} → {result.Value}");
        }

        var finalValue = await _counter.GetAsync();
        Console.WriteLine($"\nFinal value: {finalValue} (sum of {amounts.Sum()})");
    }

    /// <summary>
    /// Example 3: Conditional increment with max value.
    /// </summary>
    public async Task Example3_ConditionalIncrementAsync()
    {
        Console.WriteLine("\n=== Example 3: Conditional Increment (Max Value) ===");

        await _counter.ResetAsync();
        Console.WriteLine("Counter reset to 0, max value: 10");

        // Increment up to max
        for (int i = 0; i < 12; i++)
        {
            var result = await _counter.IncrementIfBelowResultAsync(maxValue: 10, amount: 1);
            Console.WriteLine($"  Attempt {i + 1}: {(result.IsSuccessful ? "✓ incremented" : "✗ rejected")} → {result.Value}");
        }

        var state = await _counter.GetStateAsync();
        Console.WriteLine($"\nFinal state: {state.CurrentValue} (clamped at max 10)");
    }

    /// <summary>
    /// Example 4: Set and reset operations.
    /// </summary>
    public async Task Example4_SetAndResetAsync()
    {
        Console.WriteLine("\n=== Example 4: Set and Reset ===");

        await _counter.ResetAsync();
        Console.WriteLine("Counter reset to 0");

        // Set to specific value
        await _counter.SetAsync(100);
        var value = await _counter.GetAsync();
        Console.WriteLine($"After Set(100): {value}");

        // Increment
        await _counter.IncrementAsync(50);
        value = await _counter.GetAsync();
        Console.WriteLine($"After Increment(50): {value}");

        // Reset again
        await _counter.ResetAsync();
        value = await _counter.GetAsync();
        Console.WriteLine($"After Reset(): {value}");
    }

    /// <summary>
    /// Example 5: Counter state snapshots and percentages.
    /// </summary>
    public async Task Example5_StateAndPercentageAsync()
    {
        Console.WriteLine("\n=== Example 5: Counter State and Percentage ===");

        await _counter.ResetAsync();

        // Simulate progress tracking
        var maxValue = 100;
        Console.WriteLine($"Processing 100 items (0-100):");

        for (int i = 0; i < 5; i++)
        {
            await _counter.IncrementAsync(20);
            var percentage = await _counter.GetPercentageAsync(maxValue);
            var state = await _counter.GetStateAsync();

            Console.WriteLine($"  Progress: {state.CurrentValue}/{maxValue} ({percentage:F1}%)");
            await Task.Delay(100);
        }

        Console.WriteLine("✓ 100% complete");
    }

    /// <summary>
    /// Example 6: Multiple concurrent workers simulating resource counting.
    /// </summary>
    public async Task Example6_ConcurrentWorkersAsync()
    {
        Console.WriteLine("\n=== Example 6: Concurrent Worker Simulation ===");

        await _counter.ResetAsync();

        var workerCount = 3;
        var itemsPerWorker = 4;

        Console.WriteLine($"Simulating {workerCount} workers processing {itemsPerWorker} items each");
        Console.WriteLine("(Each worker increments counter by 1 per item)\n");

        var tasks = Enumerable.Range(0, workerCount).Select(async workerId =>
        {
            for (int i = 0; i < itemsPerWorker; i++)
            {
                var result = await _counter.IncrementAsync();
                Console.WriteLine($"  Worker-{workerId + 1}: Item {i + 1} → Counter: {result.Value}");
                await Task.Delay(Random.Shared.Next(10, 50));
            }
        });

        await Task.WhenAll(tasks);

        var finalState = await _counter.GetStateAsync();
        Console.WriteLine($"\nFinal state: {finalState.CurrentValue} items processed");
        Console.WriteLine($"Expected: {workerCount * itemsPerWorker}");
        Console.WriteLine($"Match: {finalState.CurrentValue == workerCount * itemsPerWorker}");
    }

    /// <summary>
    /// Example 7: Rate limiting with counter (sliding window).
    /// </summary>
    public async Task Example7_RateLimitingAsync()
    {
        Console.WriteLine("\n=== Example 7: Rate Limiting with Counter ===");

        await _counter.ResetAsync();

        var maxRequestsPerWindow = 5;
        Console.WriteLine($"Rate limit: {maxRequestsPerWindow} requests per window\n");

        // Simulate incoming requests
        var requestCount = 8;
        for (int i = 0; i < requestCount; i++)
        {
            var canProcess = await _counter.IncrementIfBelowAsync(maxRequestsPerWindow, amount: 1);

            if (canProcess)
            {
                Console.WriteLine($"  Request {i + 1}: ✓ ALLOWED");
            }
            else
            {
                Console.WriteLine($"  Request {i + 1}: ✗ RATE LIMITED");
            }
        }

        var state = await _counter.GetStateAsync();
        Console.WriteLine($"\nProcessed: {state.CurrentValue}/{maxRequestsPerWindow}");
    }

    /// <summary>
    /// Example 8: Batch operations.
    /// </summary>
    public async Task Example8_BatchOperationsAsync()
    {
        Console.WriteLine("\n=== Example 8: Batch Operations ===");

        await _counter.ResetAsync();
        Console.WriteLine("Counter reset to 0");

        // Define batch operations (positive = increment, negative = decrement)
        var operations = new[] { 5L, 3L, -2L, 10L, -1L };
        Console.WriteLine($"\nApplying operations: {string.Join(", ", operations.Select(op => op > 0 ? $"+{op}" : op.ToString()))}");

        var finalValue = await _counter.ApplyBatchAsync(operations);

        Console.WriteLine($"Final value: {finalValue}");
        Console.WriteLine($"Expected: {operations.Sum()} (5 + 3 - 2 + 10 - 1 = 15)");
        Console.WriteLine($"Match: {finalValue == operations.Sum()}");
    }
}