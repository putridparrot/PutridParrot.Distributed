using PutridParrot.Distributed.Coordination;
using PutridParrot.Distributed.Postgresql.Coordination;
using PutridParrot.Distributed.Redis.Coordination;
using PutridParrot.Distributed.SqlServer.Coordination;

namespace PutridParrot.Distributed.Demo.DistributedLocal;

/// <summary>
/// SQL-based distributed lock examples for SQL Server and PostgreSQL.
/// </summary>
public static class SqlExamples
{
    // Example 7: SQL Server distributed lock
    public static async Task Example7_SqlServerLock()
    {
        Console.WriteLine("Example 7: SQL Server Distributed Lock");
        Console.WriteLine("--------------------------------------");
        Console.WriteLine("This example uses SQL Server's sp_getapplock for distributed locking.");
        Console.WriteLine();

        // Connection string for SQL Server
        const string connectionString = "Server=localhost;Database=TestDB;Integrated Security=true;TrustServerCertificate=true;";

        Console.WriteLine($"Connection: {connectionString}");
        Console.WriteLine();

        try
        {
            var cacheProvider = new SqlServerCacheProvider(connectionString);

            await using var lockInstance = new DistributedLock(
                cacheProvider,
                "sql-server-lock",
                new DistributedLockOptions
                {
                    DefaultExpiry = TimeSpan.FromSeconds(10),
                    AutoExtendInterval = null // SQL locks maintained by connection
                });

            Console.WriteLine("Attempting to acquire SQL Server lock...");

            if (await lockInstance.TryAcquireAsync())
            {
                Console.WriteLine("✓ SQL Server lock acquired!");
                Console.WriteLine("  Lock Key: " + lockInstance.LockKey);

                Console.WriteLine("\nPerforming database work for 3 seconds...");
                await Task.Delay(3000);

                Console.WriteLine("✓ Work completed");
            }
            else
            {
                Console.WriteLine("❌ Could not acquire SQL Server lock");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ Error: {ex.Message}");
            Console.WriteLine("\nTroubleshooting:");
            Console.WriteLine("- Ensure SQL Server is running");
            Console.WriteLine("- Verify connection string is correct");
            Console.WriteLine("- Check database exists and is accessible");
            Console.WriteLine("- For Windows Auth: Ensure current user has access");
            Console.WriteLine("\nQuick Setup:");
            Console.WriteLine("  CREATE DATABASE TestDB;");
        }
    }

    // Example 8: PostgreSQL distributed lock
    public static async Task Example8_PostgreSqlLock()
    {
        Console.WriteLine("Example 8: PostgreSQL Distributed Lock");
        Console.WriteLine("--------------------------------------");
        Console.WriteLine("This example uses PostgreSQL's pg_try_advisory_lock for distributed locking.");
        Console.WriteLine();

        // Connection string for PostgreSQL
        const string connectionString = "Host=localhost;Database=testdb;Username=postgres;Password=postgres;";

        Console.WriteLine($"Connection: Host=localhost;Database=testdb;Username=postgres");
        Console.WriteLine();

        try
        {
            var cacheProvider = new PostgreSqlCacheProvider(connectionString);

            await using var lockInstance = new DistributedLock(
                cacheProvider,
                "postgresql-lock",
                new DistributedLockOptions
                {
                    DefaultExpiry = TimeSpan.FromSeconds(10),
                    AutoExtendInterval = null // Advisory locks maintained by connection
                });

            Console.WriteLine("Attempting to acquire PostgreSQL advisory lock...");

            if (await lockInstance.TryAcquireAsync())
            {
                Console.WriteLine("✓ PostgreSQL advisory lock acquired!");
                Console.WriteLine("  Lock Key: " + lockInstance.LockKey);

                Console.WriteLine("\nPerforming database work for 3 seconds...");
                await Task.Delay(3000);

                Console.WriteLine("✓ Work completed");
            }
            else
            {
                Console.WriteLine("❌ Could not acquire PostgreSQL lock");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ Error: {ex.Message}");
            Console.WriteLine("\nTroubleshooting:");
            Console.WriteLine("- Ensure PostgreSQL is running");
            Console.WriteLine("- Verify connection string is correct");
            Console.WriteLine("- Check database exists and is accessible");
            Console.WriteLine("- Verify username and password");
            Console.WriteLine("\nQuick Setup with Docker:");
            Console.WriteLine("  docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres");
        }
    }

    // Example 9: SQL Server competing instances
    public static async Task Example9_SqlServerCompetingInstances()
    {
        Console.WriteLine("Example 9: SQL Server - Multiple Competing Instances");
        Console.WriteLine("---------------------------------------------------");
        Console.WriteLine("Simulating 3 instances competing for the same SQL Server lock...\n");

        const string connectionString = "Server=localhost;Database=TestDB;Integrated Security=true;TrustServerCertificate=true;";

        try
        {
            var tasks = Enumerable.Range(1, 3).Select(async instanceId =>
            {
                var cacheProvider = new SqlServerCacheProvider(connectionString);

                var options = new DistributedLockOptions
                {
                    AcquireTimeout = TimeSpan.FromSeconds(10),
                    RetryDelay = TimeSpan.FromMilliseconds(100),
                    DefaultExpiry = TimeSpan.FromSeconds(3)
                };

                await using var lockInstance = new DistributedLock(
                    cacheProvider,
                    "shared-sql-lock",
                    options);

                Console.WriteLine($"[SQL Instance {instanceId}] Attempting to acquire lock...");

                if (await lockInstance.TryAcquireAsync())
                {
                    Console.WriteLine($"[SQL Instance {instanceId}] ✓ Lock acquired! Working for 1 second...");
                    await Task.Delay(1000);

                    await lockInstance.ReleaseAsync();
                    Console.WriteLine($"[SQL Instance {instanceId}] ✓ Lock released");
                }
                else
                {
                    Console.WriteLine($"[SQL Instance {instanceId}] ❌ Failed to acquire lock within timeout");
                }
            });

            await Task.WhenAll(tasks);
            Console.WriteLine("\n✓ All SQL instances completed");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ Error: {ex.Message}");
        }
    }

    // Example 10: PostgreSQL competing instances
    public static async Task Example10_PostgreSqlCompetingInstances()
    {
        Console.WriteLine("Example 10: PostgreSQL - Multiple Competing Instances");
        Console.WriteLine("----------------------------------------------------");
        Console.WriteLine("Simulating 3 instances competing for the same PostgreSQL advisory lock...\n");

        const string connectionString = "Host=localhost;Database=testdb;Username=postgres;Password=postgres;";

        try
        {
            var tasks = Enumerable.Range(1, 3).Select(async instanceId =>
            {
                var cacheProvider = new PostgreSqlCacheProvider(connectionString);

                var options = new DistributedLockOptions
                {
                    AcquireTimeout = TimeSpan.FromSeconds(10),
                    RetryDelay = TimeSpan.FromMilliseconds(100),
                    DefaultExpiry = TimeSpan.FromSeconds(3)
                };

                await using var lockInstance = new DistributedLock(
                    cacheProvider,
                    "shared-pg-lock",
                    options);

                Console.WriteLine($"[PG Instance {instanceId}] Attempting to acquire lock...");

                if (await lockInstance.TryAcquireAsync())
                {
                    Console.WriteLine($"[PG Instance {instanceId}] ✓ Lock acquired! Working for 1 second...");
                    await Task.Delay(1000);

                    await lockInstance.ReleaseAsync();
                    Console.WriteLine($"[PG Instance {instanceId}] ✓ Lock released");
                }
                else
                {
                    Console.WriteLine($"[PG Instance {instanceId}] ❌ Failed to acquire lock within timeout");
                }
            });

            await Task.WhenAll(tasks);
            Console.WriteLine("\n✓ All PostgreSQL instances completed");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ Error: {ex.Message}");
        }
    }

    // Example 11: Cross-provider comparison
    public static async Task Example11_CrossProviderComparison()
    {
        Console.WriteLine("Example 11: Cross-Provider Comparison");
        Console.WriteLine("-------------------------------------");
        Console.WriteLine("Demonstrating the same lock logic with different providers.\n");

        var providers = new Dictionary<string, IDistributedCacheProvider>();

        // Add available providers
        try
        {
            providers["Redis"] = new RedisCacheProvider(
                await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync("localhost:6379"));
            Console.WriteLine("✓ Redis provider available");
        }
        catch
        {
            Console.WriteLine("⚠ Redis not available");
        }

        try
        {
            providers["SQL Server"] = new SqlServerCacheProvider(
                "Server=localhost;Database=TestDB;Integrated Security=true;TrustServerCertificate=true;");
            Console.WriteLine("✓ SQL Server provider available");
        }
        catch
        {
            Console.WriteLine("⚠ SQL Server not available");
        }

        try
        {
            providers["PostgreSQL"] = new PostgreSqlCacheProvider(
                "Host=localhost;Database=testdb;Username=postgres;Password=postgres;");
            Console.WriteLine("✓ PostgreSQL provider available");
        }
        catch
        {
            Console.WriteLine("⚠ PostgreSQL not available");
        }

        Console.WriteLine();

        foreach (var (providerName, provider) in providers)
        {
            Console.WriteLine($"Testing with {providerName}...");

            try
            {
                await using var lockInstance = new DistributedLock(provider, "test-lock");

                if (await lockInstance.TryAcquireAsync(TimeSpan.FromSeconds(5)))
                {
                    Console.WriteLine($"  ✓ {providerName} lock acquired");
                    await Task.Delay(1000);
                    await lockInstance.ReleaseAsync();
                    Console.WriteLine($"  ✓ {providerName} lock released");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  ❌ {providerName} error: {ex.Message}");
            }

            Console.WriteLine();
        }
    }
}