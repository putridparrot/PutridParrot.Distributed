
namespace PutridParrot.Distributed.Demo.DistributedQueue;

/// <summary>
/// Interactive examples demonstrating distributed queue / work dispatcher patterns.
/// </summary>
public class DistributedQueueExamples
{
    private readonly Coordination.DistributedQueue _queue;

    /// <summary>
    /// Initializes a new instance of the DistributedQueueExamples class.
    /// </summary>
    /// <param name="queue">The distributed queue instance.</param>
    public DistributedQueueExamples(Coordination.DistributedQueue queue)
    {
        _queue = queue;
    }

    /// <summary>
    /// Example 1: Basic enqueue and dequeue operations.
    /// </summary>
    public async Task Example1_BasicEnqueueDequeueAsync()
    {
        Console.WriteLine("\n=== Example 1: Basic Enqueue/Dequeue ===");

        // Enqueue several work items
        Console.WriteLine("Enqueueing 3 work items...");
        for (int i = 1; i <= 3; i++)
        {
            var result = await _queue.EnqueueAsync($"{{\"task\": \"process-{i}\"}}", priority: 0);
            Console.WriteLine($"  ✓ Enqueued: {result.WorkItem?.Id} (Payload: {result.WorkItem?.Payload})");
        }

        // Show queue state
        var state = await _queue.GetStateAsync();
        Console.WriteLine($"\nQueue State: Pending={state.PendingCount}, Processing={state.ProcessingCount}");

        // Dequeue and process
        Console.WriteLine("\nDequeueing work items...");
        for (int i = 0; i < 3; i++)
        {
            var dequeueResult = await _queue.DequeueAsync($"worker-1");
            if (dequeueResult.IsSuccessful && dequeueResult.WorkItem != null)
            {
                Console.WriteLine($"  ✓ Dequeued: {dequeueResult.WorkItem.Id} (Worker: {dequeueResult.WorkItem.WorkerId}, Attempt: {dequeueResult.WorkItem.AttemptCount})");
                await _queue.AcknowledgeAsync(dequeueResult.WorkItem.Id!);
            }
        }

        state = await _queue.GetStateAsync();
        Console.WriteLine($"\nFinal Queue State: Pending={state.PendingCount}, Processing={state.ProcessingCount}, Completed={state.CompletedCount}");
    }

    /// <summary>
    /// Example 2: Priority-based queue processing.
    /// </summary>
    public async Task Example2_PriorityQueueAsync()
    {
        Console.WriteLine("\n=== Example 2: Priority Queue Processing ===");

        // Enqueue items with different priorities
        Console.WriteLine("Enqueueing items with different priorities...");
        var tasks = new[] { "low-priority-task", "normal-task", "high-priority-task", "normal-task-2" };
        var priorities = new[] { 1, 5, 10, 5 };

        for (int i = 0; i < tasks.Length; i++)
        {
            var result = await _queue.EnqueueAsync($"{{\"task\": \"{tasks[i]}\"}}", priority: priorities[i]);
            Console.WriteLine($"  ✓ Enqueued: {tasks[i]} (Priority: {priorities[i]})");
        }

        // Dequeue items (should come in priority order)
        Console.WriteLine("\nDequeueing items (in priority order)...");
        for (int i = 0; i < tasks.Length; i++)
        {
            var dequeueResult = await _queue.DequeueAsync($"worker-{i % 2 + 1}");
            if (dequeueResult.IsSuccessful && dequeueResult.WorkItem != null)
            {
                Console.WriteLine($"  ✓ Dequeued: {dequeueResult.WorkItem.Payload} (Priority: {dequeueResult.WorkItem.Priority})");
                await _queue.AcknowledgeAsync(dequeueResult.WorkItem.Id!);
            }
        }
    }

    /// <summary>
    /// Example 3: Retry and dead letter queue handling.
    /// </summary>
    public async Task Example3_RetryAndDeadLetterAsync()
    {
        Console.WriteLine("\n=== Example 3: Retry and Dead Letter Queue ===");

        // Enqueue a work item
        var enqueueResult = await _queue.EnqueueAsync("{\"task\": \"failing-task\"}");
        var workItemId = enqueueResult.WorkItem!.Id!;
        Console.WriteLine($"Enqueued work item: {workItemId}");

        // Attempt to process and fail multiple times
        Console.WriteLine("\nAttempting to process (will fail)...");
        for (int attempt = 0; attempt < 4; attempt++)
        {
            var dequeueResult = await _queue.DequeueAsync($"worker-1");
            if (dequeueResult.IsSuccessful && dequeueResult.WorkItem != null)
            {
                Console.WriteLine($"  Attempt {dequeueResult.WorkItem.AttemptCount}: Processing failed, nacking...");
                await _queue.NackAsync(dequeueResult.WorkItem.Id!, "Simulated processing failure");

                var state = await _queue.GetStateAsync();
                Console.WriteLine($"    Queue state: Pending={state.PendingCount}, DeadLetter={state.DeadLetterCount}");

                if (state.DeadLetterCount > 0)
                {
                    Console.WriteLine("    ✓ Item moved to dead letter queue");
                    break;
                }
            }
        }

        // Inspect dead letter queue
        var dlqItems = await _queue.GetDeadLetterItemsAsync();
        Console.WriteLine($"\nDead Letter Queue contains {dlqItems.Count()} items:");
        foreach (var item in dlqItems)
        {
            Console.WriteLine($"  - {item.Id}: {item.ErrorMessage} (Attempts: {item.AttemptCount})");
        }
    }

    /// <summary>
    /// Example 4: Multiple workers processing concurrently.
    /// </summary>
    public async Task Example4_MultipleWorkersAsync()
    {
        Console.WriteLine("\n=== Example 4: Multiple Workers Processing Concurrently ===");

        // Enqueue batch of work items
        Console.WriteLine("Enqueueing 10 work items...");
        for (int i = 1; i <= 10; i++)
        {
            await _queue.EnqueueAsync($"{{\"id\": {i}, \"task\": \"batch-job-{i}\"}}");
        }

        var state = await _queue.GetStateAsync();
        Console.WriteLine($"Queue state: {state.PendingCount} pending items");

        // Simulate 3 workers processing concurrently
        Console.WriteLine("\nProcessing with 3 workers...");
        var workers = new[] { "worker-1", "worker-2", "worker-3" };
        var workTasks = new List<Task>();

        for (int i = 0; i < 10; i++)
        {
            var workerIndex = i % workers.Length;
            var dequeueResult = await _queue.DequeueAsync(workers[workerIndex]);

            if (dequeueResult.IsSuccessful && dequeueResult.WorkItem != null)
            {
                Console.WriteLine($"  {workers[workerIndex]}: Processing {dequeueResult.WorkItem.Payload}");

                // Simulate async work
                await Task.Delay(50);
                await _queue.AcknowledgeAsync(dequeueResult.WorkItem.Id!);
                Console.WriteLine($"  {workers[workerIndex]}: ✓ Completed");
            }
        }

        state = await _queue.GetStateAsync();
        Console.WriteLine($"\nFinal state: Pending={state.PendingCount}, Completed={state.CompletedCount}");
    }

    /// <summary>
    /// Example 5: Visibility timeout and re-processing.
    /// </summary>
    public async Task Example5_VisibilityTimeoutAsync()
    {
        Console.WriteLine("\n=== Example 5: Visibility Timeout and Re-Processing ===");

        // Enqueue a work item
        var enqueueResult = await _queue.EnqueueAsync("{\"task\": \"timeout-test\"}");
        var workItemId = enqueueResult.WorkItem!.Id!;
        Console.WriteLine($"Enqueued: {workItemId}");

        // Dequeue but don't acknowledge (simulating a hung worker)
        Console.WriteLine("\nWorker-1 dequeues but doesn't acknowledge...");
        var dequeueResult1 = await _queue.DequeueAsync("worker-1");
        if (dequeueResult1.IsSuccessful && dequeueResult1.WorkItem != null)
        {
            Console.WriteLine($"  Dequeued at: {dequeueResult1.WorkItem.UpdatedAt}");
            Console.WriteLine($"  Visibility deadline: {dequeueResult1.WorkItem.VisibilityDeadline}");
            Console.WriteLine("  (Simulating hung worker - not acknowledging)");
        }

        var state = await _queue.GetStateAsync();
        Console.WriteLine($"\nQueue state: Pending={state.PendingCount}, Processing={state.ProcessingCount}");

        // Wait for visibility timeout
        Console.WriteLine("\nWaiting for visibility timeout (5 seconds)...");
        await Task.Delay(5000);

        // Another worker can now dequeue the same item
        Console.WriteLine("Worker-2 attempts to dequeue...");
        var dequeueResult2 = await _queue.DequeueAsync("worker-2");
        if (dequeueResult2.IsSuccessful && dequeueResult2.WorkItem != null)
        {
            Console.WriteLine($"  ✓ Re-dequeued after timeout (Attempt: {dequeueResult2.WorkItem.AttemptCount})");
            await _queue.AcknowledgeAsync(dequeueResult2.WorkItem.Id!);
        }
        else
        {
            Console.WriteLine("  (Note: Visibility timeout behavior depends on backend)");
        }
    }

    /// <summary>
    /// Example 6: ProcessNext helper for automatic ack/nack.
    /// </summary>
    public async Task Example6_ProcessNextHelperAsync()
    {
        Console.WriteLine("\n=== Example 6: ProcessNext Helper for Automatic Ack/Nack ===");

        // Enqueue some work items
        Console.WriteLine("Enqueueing 3 work items...");
        await _queue.EnqueueAsync("{\"id\": 1, \"value\": 10}");
        await _queue.EnqueueAsync("{\"id\": 2, \"value\": 20}");
        await _queue.EnqueueAsync("{\"id\": 3, \"value\": 0}"); // This will fail division

        // Process using the helper method
        Console.WriteLine("\nProcessing with automatic ack/nack...");
        for (int i = 0; i < 3; i++)
        {
            await _queue.ProcessNextAsync("worker-1", async workItem =>
            {
                Console.WriteLine($"  Processing: {workItem.Payload}");

                // Simulate processing
                if (workItem.Payload!.Contains("\"value\": 0"))
                {
                    throw new InvalidOperationException("Cannot divide by zero");
                }

                await Task.Delay(100);
                Console.WriteLine($"    ✓ Successfully processed");
            });
        }

        // Check final state
        var state = await _queue.GetStateAsync();
        Console.WriteLine($"\nFinal state: Completed={state.CompletedCount}, DeadLetter={state.DeadLetterCount}");
    }
}

