using Microsoft.Extensions.Configuration;
using PutridParrot.Distributed.Coordination;
using PutridParrot.Distributed.Postgresql.Coordination;
using PutridParrot.Distributed.Redis.Coordination;
using StackExchange.Redis;

namespace PutridParrot.Distributed.Demo.DistributedIdempotencyKey;

public static class RunIdempotencyKeyDemo
{
    public static async Task RunAsync(IConfiguration configuration)
    {
        Console.WriteLine("=== Distributed Idempotency Key Examples ===\n");
        Console.WriteLine("Choose an idempotency key example:");
        Console.WriteLine("1. Basic Idempotency");
        Console.WriteLine("2. Payment Processing");
        Console.WriteLine("3. API Request Deduplication");
        Console.WriteLine("4. Database Insert Idempotency");
        Console.WriteLine("5. Message Queue Deduplication");
        Console.WriteLine("6. Monitoring Idempotency State");
        Console.Write("\nEnter choice (1-6): ");

        var exampleChoice = Console.ReadLine();
        Console.WriteLine();

        IDistributedIdempotencyKeyProvider? provider = null;

        try
        {
            const string redisConnectionString = "localhost:6379";

            try
            {
                var redis = ConnectionMultiplexer.Connect(redisConnectionString);
                provider = new RedisIdempotencyKeyProvider(redis);
                Console.WriteLine("Connected to Redis for idempotency key demo.\n");
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
                    await IdempotencyKeyExamples.Example1_BasicIdempotency(provider);
                    break;
                case "2":
                    await IdempotencyKeyExamples.Example2_PaymentProcessing(provider);
                    break;
                case "3":
                    await IdempotencyKeyExamples.Example3_ApiRequestDeduplication(provider);
                    break;
                case "4":
                    await IdempotencyKeyExamples.Example4_DatabaseInsertIdempotency(provider);
                    break;
                case "5":
                    await IdempotencyKeyExamples.Example5_MessageQueueDeduplication(provider);
                    break;
                case "6":
                    await IdempotencyKeyExamples.Example6_MonitoringIdempotency(provider);
                    break;
                default:
                    Console.WriteLine("Invalid choice.");
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error running idempotency key demo: {ex.Message}");
        }
    }
}