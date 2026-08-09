using Microsoft.Extensions.Configuration;
using PutridParrot.Distributed.Redis.Coordination;
using StackExchange.Redis;

namespace PutridParrot.Distributed.Demo.DistributedCounter;

internal class RunCounterDemo
{
    public async static Task RunAsync(IConfiguration configuration)
    {
        try
        {
            Console.WriteLine("Choose example:");
            Console.WriteLine("1. Basic Increment/Decrement");
            Console.WriteLine("2. Batch Increments");
            Console.WriteLine("3. Conditional Increment");
            Console.WriteLine("4. Set and Reset");
            Console.WriteLine("5. State and Percentage");
            Console.WriteLine("6. Concurrent Workers");
            Console.WriteLine("7. Rate Limiting");
            Console.WriteLine("8. Batch Operations");
            Console.Write("\nEnter choice (1-8): ");

            var exampleChoice = Console.ReadLine();

            // For simplicity, use Redis provider as default
            try
            {
                var redis = ConnectionMultiplexer.Connect("localhost:6379");
                var db = redis.GetDatabase();
                var provider = new RedisCounterProvider(db);
                var counter = new Coordination.DistributedCounter("demo-counter", provider);

                await counter.ResetAsync();

                var examples = new DistributedCounterExamples(counter);

                switch (exampleChoice)
                {
                    case "1":
                        await examples.Example1_BasicOperationsAsync();
                        break;
                    case "2":
                        await examples.Example2_BatchIncrementsAsync();
                        break;
                    case "3":
                        await examples.Example3_ConditionalIncrementAsync();
                        break;
                    case "4":
                        await examples.Example4_SetAndResetAsync();
                        break;
                    case "5":
                        await examples.Example5_StateAndPercentageAsync();
                        break;
                    case "6":
                        await examples.Example6_ConcurrentWorkersAsync();
                        break;
                    case "7":
                        await examples.Example7_RateLimitingAsync();
                        break;
                    case "8":
                        await examples.Example8_BatchOperationsAsync();
                        break;
                    default:
                        Console.WriteLine("Invalid choice. Running Example 1...");
                        await examples.Example1_BasicOperationsAsync();
                        break;
                }

                Console.WriteLine("✓ Counter demo completed");
            }
            catch
            {
                Console.WriteLine("⚠️  Redis not available. Ensure Redis is running at localhost:6379");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error running Counter demo: {ex.Message}");
        }
    }
}