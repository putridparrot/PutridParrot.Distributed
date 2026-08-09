using System;
using System.Collections.Generic;
using System.Text;
using PutridParrot.Distributed.Coordination;

namespace PutridParrot.Distributed.Demo.DistributedSemaphore;

/// <summary>
/// Distributed semaphore demonstration examples.
/// Shows usage patterns for connection pooling, resource limiting, and concurrent job processing.
/// </summary>
public static class SemaphoreExamples
{
    /// <summary>
    /// Example 1: Basic semaphore usage with 3 permits (like a connection pool).
    /// </summary>
    public static async Task Example1_BasicSemaphore(IDistributedSemaphoreProvider provider)
    {
        Console.WriteLine("Example 1: Basic Semaphore (3-Slot Connection Pool)");
        Console.WriteLine("---------------------------------------------------");
        Console.WriteLine("Simulating a connection pool with 3 available connections\n");

        var options = new DistributedSemaphoreOptions
        {
            MaxPermits = 3
        };

        var semaphore = new Coordination.DistributedSemaphore(provider, "connection-pool", options);

        Console.WriteLine("Attempting to acquire 5 connections...\n");

        for (int i = 1; i <= 5; i++)
        {
            bool acquired = await semaphore.TryAcquireAsync();
            Console.WriteLine($"  Connection {i}: {(acquired ? "✓ Acquired" : "❌ Pool exhausted")}");
        }

        var available = await semaphore.GetAvailablePermitsAsync();
        Console.WriteLine($"\n✓ Available connections: {available}");

        Console.WriteLine("\nReleasing 2 connections...");
        await semaphore.ReleaseAsync(2);

        available = await semaphore.GetAvailablePermitsAsync();
        Console.WriteLine($"✓ Available connections: {available}\n");

        await semaphore.ResetAsync();
    }

    /// <summary>
    /// Example 2: Simulating concurrent job processing with worker limit.
    /// </summary>
    public static async Task Example2_ConcurrentJobProcessing(IDistributedSemaphoreProvider provider)
    {
        Console.WriteLine("Example 2: Concurrent Job Processing (Max 2 Workers)");
        Console.WriteLine("-----------------------------------------------------");
        Console.WriteLine("Submitting 6 jobs with max 2 concurrent workers\n");

        var options = new DistributedSemaphoreOptions
        {
            MaxPermits = 2
        };

        var semaphore = new Coordination.DistributedSemaphore(provider, "job-workers", options);

        var jobTasks = Enumerable.Range(1, 6).Select(async jobId =>
        {
            bool acquired = await semaphore.TryAcquireAsync();

            if (acquired)
            {
                Console.WriteLine($"[Job {jobId}] ✓ Started (worker acquired)");

                // Simulate job work
                await Task.Delay(500);

                await semaphore.ReleaseAsync();
                Console.WriteLine($"[Job {jobId}] ✓ Completed (worker released)");
            }
            else
            {
                Console.WriteLine($"[Job {jobId}] ❌ Could not start (no workers available)");
            }
        });

        await Task.WhenAll(jobTasks);
        Console.WriteLine("\n✓ All jobs processed");

        await semaphore.ResetAsync();
    }

    /// <summary>
    /// Example 3: Multi-permit acquisition (batched resource allocation).
    /// </summary>
    public static async Task Example3_MultipermitAllocation(IDistributedSemaphoreProvider provider)
    {
        Console.WriteLine("Example 3: Multi-Permit Allocation (Batch Resources)");
        Console.WriteLine("---------------------------------------------------");
        Console.WriteLine("Semaphore: 100 units total, allocating in batches\n");

        var options = new DistributedSemaphoreOptions
        {
            MaxPermits = 100
        };

        var semaphore = new Coordination.DistributedSemaphore(provider, "resource-units", options);

        var allocations = new[]
        {
            ("Process A", 30),
            ("Process B", 25),
            ("Process C", 15),
            ("Process D", 40)  // This will fail - only 30 left
        };

        foreach (var (name, requested) in allocations)
        {
            bool acquired = await semaphore.TryAcquireAsync(requested);
            Console.WriteLine($"  {name,-12} requested {requested,3} units: {(acquired ? "✓ Allocated" : "❌ Insufficient")}");
        }

        var available = await semaphore.GetAvailablePermitsAsync();
        Console.WriteLine($"\n✓ Remaining units: {available}");

        Console.WriteLine("\nReleasing Process B's 25 units...");
        await semaphore.ReleaseAsync(25);

        available = await semaphore.GetAvailablePermitsAsync();
        Console.WriteLine($"✓ Available units: {available}\n");

        await semaphore.ResetAsync();
    }

    /// <summary>
    /// Example 4: Timeout-based acquisition with retries.
    /// </summary>
    public static async Task Example4_AcquisitionWithTimeout(IDistributedSemaphoreProvider provider)
    {
        Console.WriteLine("Example 4: Acquisition With Timeout");
        Console.WriteLine("-----------------------------------");
        Console.WriteLine("Semaphore: 2 permits, 5 concurrent attempts with 2-second timeout\n");

        var options = new DistributedSemaphoreOptions
        {
            MaxPermits = 2,
            AcquireTimeout = TimeSpan.FromSeconds(2),
            RetryDelay = TimeSpan.FromMilliseconds(100)
        };

        var semaphore = new Coordination.DistributedSemaphore(provider, "limited-resources", options);

        // First, acquire both permits
        Console.WriteLine("Acquiring both permits first...");
        await semaphore.TryAcquireAsync();
        await semaphore.TryAcquireAsync();
        Console.WriteLine("✓ Both permits acquired\n");

        // Now try to acquire with timeout
        var attempts = Enumerable.Range(1, 3).Select(async attemptId =>
        {
            Console.WriteLine($"Attempt {attemptId}: Trying to acquire (2s timeout)...");
            var start = DateTime.UtcNow;

            bool acquired = await semaphore.TryAcquireAsync();
            var elapsed = DateTime.UtcNow - start;

            Console.WriteLine($"  Result: {(acquired ? "✓ Acquired" : "❌ Timed out")} ({elapsed.TotalSeconds:F2}s)\n");
        });

        await Task.WhenAll(attempts);

        await semaphore.ResetAsync();
    }

    /// <summary>
    /// Example 5: Semaphore as a license seat manager.
    /// </summary>
    public static async Task Example5_LicenseSeatManagement(IDistributedSemaphoreProvider provider)
    {
        Console.WriteLine("Example 5: License Seat Management");
        Console.WriteLine("---------------------------------");
        Console.WriteLine("Managing 5 concurrent user licenses across instances\n");

        var options = new DistributedSemaphoreOptions
        {
            MaxPermits = 5
        };

        var semaphore = new Coordination.DistributedSemaphore(provider, "user-licenses", options);

        var users = new[] { "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace" };

        foreach (var user in users)
        {
            bool acquired = await semaphore.TryAcquireAsync();

            if (acquired)
            {
                Console.WriteLine($"  {user,-10} ✓ License allocated");
            }
            else
            {
                Console.WriteLine($"  {user,-10} ❌ No licenses available");
            }
        }

        var available = await semaphore.GetAvailablePermitsAsync();
        Console.WriteLine($"\n✓ Licenses remaining: {available}");

        Console.WriteLine("\nSimulating user logoff...");
        Console.WriteLine("  Alice logs off (1 license released)");
        await semaphore.ReleaseAsync();

        available = await semaphore.GetAvailablePermitsAsync();
        Console.WriteLine($"✓ Licenses available: {available}");

        Console.WriteLine("\n  Frank can now login...");
        bool frankAcquired = await semaphore.TryAcquireAsync();
        Console.WriteLine($"  Frank: {(frankAcquired ? "✓ License acquired" : "❌ Still unavailable")}\n");

        await semaphore.ResetAsync();
    }

    /// <summary>
    /// Example 6: Monitored semaphore with utilization tracking.
    /// </summary>
    public static async Task Example6_MonitoredUtilization(IDistributedSemaphoreProvider provider)
    {
        Console.WriteLine("Example 6: Monitored Utilization (Health Check)");
        Console.WriteLine("-----------------------------------------------");
        Console.WriteLine("Tracking semaphore usage patterns with status snapshots\n");

        var options = new DistributedSemaphoreOptions
        {
            MaxPermits = 10
        };

        var semaphore = new Coordination.DistributedSemaphore(provider, "monitored-resource", options);

        Console.WriteLine("Checkpoint 1: Initial state");
        PrintSemaphoreStatus(await semaphore.GetAvailablePermitsAsync(), 10);

        Console.WriteLine("\nCheckpoint 2: After acquiring 3 permits");
        await semaphore.TryAcquireAsync(3);
        PrintSemaphoreStatus(await semaphore.GetAvailablePermitsAsync(), 10);

        Console.WriteLine("\nCheckpoint 3: After acquiring 4 more permits");
        await semaphore.TryAcquireAsync(4);
        PrintSemaphoreStatus(await semaphore.GetAvailablePermitsAsync(), 10);

        Console.WriteLine("\nCheckpoint 4: After releasing 2 permits");
        await semaphore.ReleaseAsync(2);
        PrintSemaphoreStatus(await semaphore.GetAvailablePermitsAsync(), 10);

        Console.WriteLine("\nCheckpoint 5: After reset");
        await semaphore.ResetAsync();
        PrintSemaphoreStatus(await semaphore.GetAvailablePermitsAsync(), 10);
    }

    /// <summary>
    /// Helper to print semaphore status with visual utilization bar.
    /// </summary>
    private static void PrintSemaphoreStatus(long available, long max)
    {
        long used = max - available;
        double utilization = (double)used / max * 100;

        Console.WriteLine($"  Available: {available:D2}/{max}");
        Console.WriteLine($"  Utilization: {utilization:F1}% [{new string('█', (int)(utilization / 10)):,12}         ]");
    }
}

