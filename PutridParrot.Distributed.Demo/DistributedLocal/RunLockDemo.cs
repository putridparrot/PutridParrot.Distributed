using Microsoft.Extensions.Configuration;
using PutridParrot.Distributed.Coordination;
using PutridParrot.Distributed.Redis.Coordination;
using StackExchange.Redis;

namespace PutridParrot.Distributed.Demo.DistributedLocal;

internal class RunLockDemo
{
    public static async Task RunAsync(IConfiguration configuration)
    {
        Console.WriteLine("Choose a cache provider:");
        Console.WriteLine("1. Redis");
        Console.WriteLine("2. SQL Server");
        Console.WriteLine("3. PostgreSQL");
        Console.Write("\nEnter choice (1-3): ");

        var providerChoice = Console.ReadLine();
        Console.WriteLine();

        switch (providerChoice)
        {
            case "1":
                await RunRedisExamples();
                break;
            case "2":
                await RunSqlServerExamples();
                break;
            case "3":
                await RunPostgreSqlExamples();
                break;
            default:
                Console.WriteLine("Invalid choice");
                break;
        }

        Console.WriteLine("\nPress any key to exit...");
        Console.ReadKey();
    }

    static async Task RunRedisExamples()
    {
        Console.WriteLine("=== Redis Provider ===\n");

        const string redisConnectionString = "localhost:6379";

        try
        {
            Console.WriteLine($"Connecting to Redis at {redisConnectionString}...");
            var redis = await ConnectionMultiplexer.ConnectAsync(redisConnectionString);
            Console.WriteLine("✓ Connected to Redis\n");

            var cacheProvider = new RedisCacheProvider(redis);

            Console.WriteLine("Choose an example to run:");
            Console.WriteLine("1. Basic Lock Usage");
            Console.WriteLine("2. Lock with Timeout and Retry");
            Console.WriteLine("3. Auto-Extension Demo");
            Console.WriteLine("4. Manual Extension Demo");
            Console.WriteLine("5. Multiple Competing Instances");
            Console.WriteLine("6. Factory Pattern");
            Console.WriteLine("0. Run All Examples");
            Console.Write("\nEnter choice (0-6): ");

            var choice = Console.ReadLine();
            Console.WriteLine();

            switch (choice)
            {
                case "1":
                    await Example1_BasicLockUsage(cacheProvider);
                    break;
                case "2":
                    await Example2_LockWithTimeoutAndRetry(cacheProvider);
                    break;
                case "3":
                    await Example3_AutoExtension(cacheProvider);
                    break;
                case "4":
                    await Example4_ManualExtension(cacheProvider);
                    break;
                case "5":
                    await Example5_CompetingInstances(cacheProvider);
                    break;
                case "6":
                    await Example6_FactoryPattern(cacheProvider);
                    break;
                case "0":
                    await Example1_BasicLockUsage(cacheProvider);
                    Console.WriteLine("\n" + new string('-', 60) + "\n");
                    await Example2_LockWithTimeoutAndRetry(cacheProvider);
                    Console.WriteLine("\n" + new string('-', 60) + "\n");
                    await Example3_AutoExtension(cacheProvider);
                    Console.WriteLine("\n" + new string('-', 60) + "\n");
                    await Example4_ManualExtension(cacheProvider);
                    Console.WriteLine("\n" + new string('-', 60) + "\n");
                    await Example5_CompetingInstances(cacheProvider);
                    Console.WriteLine("\n" + new string('-', 60) + "\n");
                    await Example6_FactoryPattern(cacheProvider);
                    break;
                default:
                    Console.WriteLine("Invalid choice");
                    break;
            }

            await redis.CloseAsync();
            Console.WriteLine("\n✓ Redis connection closed");
        }
        catch (RedisConnectionException ex)
        {
            Console.WriteLine($"\n❌ Could not connect to Redis: {ex.Message}");
            Console.WriteLine("\nMake sure Redis is running on localhost:6379");
            Console.WriteLine("You can start Redis using Docker:");
            Console.WriteLine("  docker run -d -p 6379:6379 redis");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ Error: {ex.Message}");
        }
    }

    // ==================== SQL SERVER EXAMPLES ====================

    static async Task RunSqlServerExamples()
    {
        Console.WriteLine("=== SQL Server Provider ===\n");

        Console.WriteLine("Choose an example to run:");
        Console.WriteLine("7. Basic SQL Server Lock");
        Console.WriteLine("9. SQL Server Competing Instances");
        Console.WriteLine("0. Run All SQL Server Examples");
        Console.Write("\nEnter choice (0, 7, 9): ");

        var choice = Console.ReadLine();
        Console.WriteLine();

        switch (choice)
        {
            case "7":
                await SqlExamples.Example7_SqlServerLock();
                break;
            case "9":
                await SqlExamples.Example9_SqlServerCompetingInstances();
                break;
            case "0":
                await SqlExamples.Example7_SqlServerLock();
                Console.WriteLine("\n" + new string('-', 60) + "\n");
                await SqlExamples.Example9_SqlServerCompetingInstances();
                break;
            default:
                Console.WriteLine("Invalid choice");
                break;
        }
    }

    // ==================== POSTGRESQL EXAMPLES ====================

    static async Task RunPostgreSqlExamples()
    {
        Console.WriteLine("=== PostgreSQL Provider ===\n");

        Console.WriteLine("Choose an example to run:");
        Console.WriteLine("8. Basic PostgreSQL Lock");
        Console.WriteLine("10. PostgreSQL Competing Instances");
        Console.WriteLine("11. Cross-Provider Comparison");
        Console.WriteLine("0. Run All PostgreSQL Examples");
        Console.Write("\nEnter choice (0, 8, 10, 11): ");

        var choice = Console.ReadLine();
        Console.WriteLine();

        switch (choice)
        {
            case "8":
                await SqlExamples.Example8_PostgreSqlLock();
                break;
            case "10":
                await SqlExamples.Example10_PostgreSqlCompetingInstances();
                break;
            case "11":
                await SqlExamples.Example11_CrossProviderComparison();
                break;
            case "0":
                await SqlExamples.Example8_PostgreSqlLock();
                Console.WriteLine("\n" + new string('-', 60) + "\n");
                await SqlExamples.Example10_PostgreSqlCompetingInstances();
                Console.WriteLine("\n" + new string('-', 60) + "\n");
                await SqlExamples.Example11_CrossProviderComparison();
                break;
            default:
                Console.WriteLine("Invalid choice");
                break;
        }
    }


    // Example 1: Basic lock usage with using statement
    static async Task Example1_BasicLockUsage(IDistributedCacheProvider cacheProvider)
    {
        Console.WriteLine("Example 1: Basic Lock Usage");
        Console.WriteLine("---------------------------");

        await using var lockInstance = new DistributedLock(cacheProvider, "basic-lock");

        Console.WriteLine("Attempting to acquire lock...");
        if (await lockInstance.TryAcquireAsync())
        {
            Console.WriteLine("✓ Lock acquired!");
            Console.WriteLine("  Lock Key: " + lockInstance.LockKey);
            Console.WriteLine("  Is Acquired: " + lockInstance.IsAcquired);

            Console.WriteLine("\nPerforming critical work for 2 seconds...");
            await Task.Delay(2000);

            Console.WriteLine("✓ Work completed");
            // Lock is automatically released when disposed
        }
        else
        {
            Console.WriteLine("❌ Could not acquire lock");
        }
    }

    // Example 2: Lock with timeout and automatic retry
    static async Task Example2_LockWithTimeoutAndRetry(IDistributedCacheProvider cacheProvider)
    {
        Console.WriteLine("Example 2: Lock with Timeout and Retry");
        Console.WriteLine("-------------------------------------");

        var options = new DistributedLockOptions
        {
            DefaultExpiry = TimeSpan.FromSeconds(30),
            AcquireTimeout = TimeSpan.FromSeconds(5), // Wait up to 5 seconds
            RetryDelay = TimeSpan.FromMilliseconds(100), // Retry every 100ms
            AutoExtendInterval = TimeSpan.FromSeconds(10)
        };

        await using var lockInstance = new DistributedLock(cacheProvider, "retry-lock", options);

        Console.WriteLine("Attempting to acquire lock with 5-second timeout...");
        try
        {
            await lockInstance.AcquireAsync();
            Console.WriteLine("✓ Lock acquired!");

            Console.WriteLine("Performing work for 3 seconds...");
            await Task.Delay(3000);

            Console.WriteLine("✓ Work completed");
        }
        catch (TimeoutException)
        {
            Console.WriteLine("❌ Failed to acquire lock within 5 seconds");
        }
    }

    // Example 3: Auto-extension demo
    static async Task Example3_AutoExtension(IDistributedCacheProvider cacheProvider)
    {
        Console.WriteLine("Example 3: Auto-Extension Demo");
        Console.WriteLine("------------------------------");

        var options = new DistributedLockOptions
        {
            DefaultExpiry = TimeSpan.FromSeconds(5), // Short expiry
            AutoExtendInterval = TimeSpan.FromSeconds(2), // Extend every 2 seconds
            AcquireTimeout = null
        };

        await using var lockInstance = new DistributedLock(cacheProvider, "auto-extend-lock", options);

        Console.WriteLine("Lock configured with 5-second expiry and 2-second auto-extension");

        if (await lockInstance.TryAcquireAsync())
        {
            Console.WriteLine("✓ Lock acquired!");
            Console.WriteLine("Performing long-running work for 10 seconds...");
            Console.WriteLine("(Lock will be automatically extended every 2 seconds)");

            for (int i = 0; i < 10; i++)
            {
                await Task.Delay(1000);
                Console.WriteLine($"  Working... {i + 1}s (Lock still held: {lockInstance.IsAcquired})");
            }

            Console.WriteLine("✓ Work completed - lock was automatically extended!");
        }
    }

    // Example 4: Manual extension
    static async Task Example4_ManualExtension(IDistributedCacheProvider cacheProvider)
    {
        Console.WriteLine("Example 4: Manual Extension Demo");
        Console.WriteLine("-------------------------------");

        var options = new DistributedLockOptions
        {
            DefaultExpiry = TimeSpan.FromSeconds(5),
            AutoExtendInterval = null // Disable auto-extension
        };

        await using var lockInstance = new DistributedLock(cacheProvider, "manual-extend-lock", options);

        Console.WriteLine("Lock configured with 5-second expiry (no auto-extension)");

        if (await lockInstance.TryAcquireAsync())
        {
            Console.WriteLine("✓ Lock acquired!");

            Console.WriteLine("Working for 3 seconds...");
            await Task.Delay(3000);

            Console.WriteLine("Manually extending lock by 5 seconds...");
            if (await lockInstance.ExtendAsync(TimeSpan.FromSeconds(5)))
            {
                Console.WriteLine("✓ Lock extended!");

                Console.WriteLine("Working for another 3 seconds...");
                await Task.Delay(3000);

                Console.WriteLine("✓ Work completed");
            }
            else
            {
                Console.WriteLine("❌ Failed to extend lock");
            }
        }
    }

    // Example 5: Multiple competing instances
    static async Task Example5_CompetingInstances(IDistributedCacheProvider cacheProvider)
    {
        Console.WriteLine("Example 5: Multiple Competing Instances");
        Console.WriteLine("---------------------------------------");
        Console.WriteLine("Simulating 5 concurrent instances competing for the same lock...\n");

        var tasks = Enumerable.Range(1, 5).Select(async instanceId =>
        {
            var options = new DistributedLockOptions
            {
                AcquireTimeout = TimeSpan.FromSeconds(10),
                RetryDelay = TimeSpan.FromMilliseconds(50),
                DefaultExpiry = TimeSpan.FromSeconds(3)
            };

            await using var lockInstance = new DistributedLock(
                cacheProvider,
                "shared-resource-lock",
                options);

            Console.WriteLine($"[Instance {instanceId}] Attempting to acquire lock...");

            if (await lockInstance.TryAcquireAsync())
            {
                Console.WriteLine($"[Instance {instanceId}] ✓ Lock acquired! Working for 1 second...");
                await Task.Delay(1000);

                await lockInstance.ReleaseAsync();
                Console.WriteLine($"[Instance {instanceId}] ✓ Lock released");
            }
            else
            {
                Console.WriteLine($"[Instance {instanceId}] ❌ Failed to acquire lock within timeout");
            }
        });

        await Task.WhenAll(tasks);
        Console.WriteLine("\n✓ All instances completed");
    }

    // Example 6: Factory pattern
    static async Task Example6_FactoryPattern(IDistributedCacheProvider cacheProvider)
    {
        Console.WriteLine("Example 6: Factory Pattern");
        Console.WriteLine("-------------------------");

        // Create a factory with default options
        var factory = new DistributedLockFactory(
            cacheProvider,
            new DistributedLockOptions
            {
                DefaultExpiry = TimeSpan.FromSeconds(30),
                AutoExtendInterval = TimeSpan.FromSeconds(10),
                AcquireTimeout = TimeSpan.FromSeconds(5)
            });

        Console.WriteLine("Created factory with default options");
        Console.WriteLine("Creating locks for different resources...\n");

        // Create multiple locks from the factory
        await using var lock1 = factory.CreateLock("resource-1");
        await using var lock2 = factory.CreateLock("resource-2");

        Console.WriteLine("Acquiring lock for resource-1...");
        if (await lock1.TryAcquireAsync())
        {
            Console.WriteLine("✓ Resource-1 lock acquired");

            Console.WriteLine("Acquiring lock for resource-2...");
            if (await lock2.TryAcquireAsync())
            {
                Console.WriteLine("✓ Resource-2 lock acquired");

                Console.WriteLine("\nBoth locks held simultaneously!");
                Console.WriteLine($"  Lock 1 - Key: {lock1.LockKey}, Acquired: {lock1.IsAcquired}");
                Console.WriteLine($"  Lock 2 - Key: {lock2.LockKey}, Acquired: {lock2.IsAcquired}");

                await Task.Delay(1000);

                Console.WriteLine("\n✓ Work completed");
            }
        }
    }
}