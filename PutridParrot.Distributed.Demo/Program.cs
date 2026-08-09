using Microsoft.Extensions.Configuration;
using PutridParrot.Distributed.Demo.DistributedCounter;
using PutridParrot.Distributed.Demo.DistributedIdempotencyKey;
using PutridParrot.Distributed.Demo.DistributedLeaderElection;
using PutridParrot.Distributed.Demo.DistributedLocal;
using PutridParrot.Distributed.Demo.DistributedQueue;
using PutridParrot.Distributed.Demo.DistributedSemaphore;

// Build configuration
var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production"}.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables()
    .Build();

Console.WriteLine("=== Distributed Patterns Demo ===\n");
Console.WriteLine("Choose what to demo:");
Console.WriteLine("1. Distributed Locks");
Console.WriteLine("2. Distributed Rate Limiting");
Console.WriteLine("3. Distributed Semaphores");
Console.WriteLine("4. Distributed Fence Tokens");
Console.WriteLine("5. Distributed Idempotency Keys");
Console.WriteLine("6. Distributed Read-Write Locks");
Console.WriteLine("7. Distributed Barriers");
Console.WriteLine("8. Distributed Leader Election");
Console.WriteLine("9. Distributed Token Bucket");
Console.WriteLine("10. Distributed Queue / Work Dispatcher");
Console.WriteLine("11. Distributed Counter / Sharded Counter");
Console.WriteLine("12. Distributed Backoff / Retry Coordination");
Console.WriteLine("13. Distributed Ephemeral Sessions");
Console.WriteLine("14. Distributed Saga Coordinator");
Console.WriteLine("15. Distributed Workflow Checkpointing");
Console.WriteLine("16. Distributed Outbox Pattern");
Console.WriteLine("17. Distributed Inbox Pattern");
Console.WriteLine("18. Consistent Hashing");
Console.WriteLine("19. Correlation IDs & Distributed Tracing");
Console.WriteLine("20. Request Coalescing");
Console.WriteLine("21. Connection Pooling");
Console.WriteLine("22. Combined Quick-Wins Demo");
Console.WriteLine("23. Quorum-Based Decisions");
Console.WriteLine("24. Deadlock Detection");
Console.WriteLine("25. Partition Tolerance (CAP Theorem)");
Console.WriteLine("26. Advanced Patterns Integration");
Console.Write("\nEnter choice (1-26): ");

var patternChoice = Console.ReadLine();
Console.WriteLine();

if (patternChoice == "1")
{
    await RunLockDemo.RunAsync(configuration);
    Console.WriteLine("\nPress any key to exit...");
    Console.ReadKey();
    return;
}

//if (patternChoice == "2")
//{
//    await RunRateLimitingDemo.RunAsync(configuration);
//    Console.WriteLine("\nPress any key to exit...");
//    Console.ReadKey();
//    return;
//}

if (patternChoice == "3")
{
    await RunSemaphoreDemo.RunAsync(configuration);
    Console.WriteLine("\nPress any key to exit...");
    Console.ReadKey();
    return;
}

//if (patternChoice == "4")
//{
//    await RunFenceTokenDemo.RunAsync(configuration);
//    Console.WriteLine("\nPress any key to exit...");
//    Console.ReadKey();
//    return;
//}

if (patternChoice == "5")
{
    await RunIdempotencyKeyDemo.RunAsync(configuration);
    Console.WriteLine("\nPress any key to exit...");
    Console.ReadKey();
    return;
}

//if (patternChoice == "6")
//{
//    await RunIdempotencyKeyDemo.RunAsync(configuration);
//    Console.WriteLine("\nPress any key to exit...");
//    Console.ReadKey();
//    return;
//}

//if (patternChoice == "7")
//{
//    await RunIdempotencyKeyDemo.RunAsync(configuration);
//    Console.WriteLine("\nPress any key to exit...");
//    Console.ReadKey();
//    return;
//}

if (patternChoice == "8")
{
    await RunLeaderElectionDemo.RunAsync(configuration);
    Console.WriteLine("\nPress any key to exit...");
    Console.ReadKey();
    return;
}

//if (patternChoice == "9")
//{
//    await RunTokenBucketDemo.RunAsync(configuration);
//    Console.WriteLine("\nPress any key to exit...");
//    Console.ReadKey();
//    return;
//}

if (patternChoice == "10")
{
    await RunQueueDemo.RunAsync(configuration);
    Console.WriteLine("\nPress any key to exit...");
    Console.ReadKey();
    return;
}

if (patternChoice == "11")
{
    await RunCounterDemo.RunAsync(configuration);
    Console.WriteLine("\nPress any key to exit...");
    Console.ReadKey();
    return;
}

