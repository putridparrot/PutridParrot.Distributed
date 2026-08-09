using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using PutridParrot.Distributed.Coordination;
using PutridParrot.Distributed.Redis.Coordination;

namespace PutridParrot.Distributed.Demo.DistributedSemaphore;

public static class RunSemaphoreDemo
{
    public static async Task RunAsync(IConfiguration configuration)
    {
        Console.WriteLine("=== Distributed Semaphore Examples ===\n");
        Console.WriteLine("Choose a semaphore example:");
        Console.WriteLine("1. Basic Semaphore (3-slot connection pool)");
        Console.WriteLine("2. Concurrent Job Processing (max 2 workers)");
        Console.WriteLine("3. Multi-Permit Allocation (batch resources)");
        Console.WriteLine("4. Acquisition With Timeout");
        Console.WriteLine("5. License Seat Management");
        Console.WriteLine("6. Monitored Utilization");
        Console.Write("\nEnter choice (1-6): ");

        var exampleChoice = Console.ReadLine();
        Console.WriteLine();

        IDistributedSemaphoreProvider? provider = null;

        try
        {
            // For demo purposes, use a temporary in-memory Redis if available,
            // otherwise skip. You could also prompt for provider selection.
            const string redisConnectionString = "localhost:6379";

            try
            {
                var redis = ConnectionMultiplexer.Connect(redisConnectionString);
                provider = new RedisSemaphoreProvider(redis);
                Console.WriteLine("Connected to Redis for semaphore demo.\n");
            }
            catch
            {
                Console.WriteLine("⚠️  Redis not available. For this demo, ensure Redis is running at localhost:6379");
                Console.WriteLine("Examples will demonstrate the API (they won't persist without a backend).\n");
                return;
            }

            switch (exampleChoice)
            {
                case "1":
                    await SemaphoreExamples.Example1_BasicSemaphore(provider);
                    break;
                case "2":
                    await SemaphoreExamples.Example2_ConcurrentJobProcessing(provider);
                    break;
                case "3":
                    await SemaphoreExamples.Example3_MultipermitAllocation(provider);
                    break;
                case "4":
                    await SemaphoreExamples.Example4_AcquisitionWithTimeout(provider);
                    break;
                case "5":
                    await SemaphoreExamples.Example5_LicenseSeatManagement(provider);
                    break;
                case "6":
                    await SemaphoreExamples.Example6_MonitoredUtilization(provider);
                    break;
                default:
                    Console.WriteLine("Invalid choice.");
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error running semaphore demo: {ex.Message}");
        }

    }
}   
