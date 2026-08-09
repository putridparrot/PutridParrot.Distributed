using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using PutridParrot.Distributed.Redis.Coordination;

namespace PutridParrot.Distributed.Demo.DistributedQueue;

public static class RunQueueDemo
{
    public static async Task RunAsync(IConfiguration configuration)
    {
        try
        {
            Console.WriteLine("Choose example:");
            Console.WriteLine("1. Basic Enqueue/Dequeue");
            Console.WriteLine("2. Priority Queue Processing");
            Console.WriteLine("3. Retry and Dead Letter Queue");
            Console.WriteLine("4. Multiple Workers");
            Console.WriteLine("5. Visibility Timeout");
            Console.WriteLine("6. ProcessNext Helper");
            Console.Write("\nEnter choice (1-6): ");

            var exampleChoice = Console.ReadLine();

            // For simplicity, use Redis provider as default
            try
            {
                var redis = ConnectionMultiplexer.Connect("localhost:6379");
                var db = redis.GetDatabase();
                var provider = new RedisQueueProvider(db);
                var queue = new Coordination.DistributedQueue("demo-queue", provider);

                await queue.ResetAsync();

                var examples = new DistributedQueueExamples(queue);

                switch (exampleChoice)
                {
                    case "1":
                        await examples.Example1_BasicEnqueueDequeueAsync();
                        break;
                    case "2":
                        await examples.Example2_PriorityQueueAsync();
                        break;
                    case "3":
                        await examples.Example3_RetryAndDeadLetterAsync();
                        break;
                    case "4":
                        await examples.Example4_MultipleWorkersAsync();
                        break;
                    case "5":
                        await examples.Example5_VisibilityTimeoutAsync();
                        break;
                    case "6":
                        await examples.Example6_ProcessNextHelperAsync();
                        break;
                    default:
                        Console.WriteLine("Invalid choice. Running Example 1...");
                        await examples.Example1_BasicEnqueueDequeueAsync();
                        break;
                }

                Console.WriteLine("✓ Queue demo completed");
            }
            catch
            {
                Console.WriteLine("⚠️  Redis not available. Ensure Redis is running at localhost:6379");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error running Queue demo: {ex.Message}");
        }
    }
}
