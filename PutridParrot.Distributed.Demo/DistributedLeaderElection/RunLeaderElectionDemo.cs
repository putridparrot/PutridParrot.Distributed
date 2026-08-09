using Microsoft.Extensions.Configuration;
using PutridParrot.Distributed.Coordination;
using PutridParrot.Distributed.Redis.Coordination;
using StackExchange.Redis;

namespace PutridParrot.Distributed.Demo.DistributedLeaderElection;

internal static class RunLeaderElectionDemo
{
    public static async Task RunAsync(IConfiguration configuration)
    {
        Console.WriteLine("=== Distributed Leader Election Examples ===\n");
        Console.WriteLine("Choose a Leader Election example:");
        Console.WriteLine("1. Basic Leader Election");
        Console.WriteLine("2. Leadership Heartbeat and Renewal");
        Console.WriteLine("3. Multiple Competitors");
        Console.WriteLine("4. Leadership Transfer");
        Console.WriteLine("5. Follower Discovers Leader Change");
        Console.WriteLine("6. Leader State Monitoring");
        Console.Write("\nEnter choice (1-6): ");

        var exampleChoice = Console.ReadLine();
        Console.WriteLine();

        try
        {
            var redisConnectionString = configuration.GetConnectionString("Redis") ?? "localhost:6379";

            try
            {
                var redis = ConnectionMultiplexer.Connect(redisConnectionString);

                switch (exampleChoice)
                {
                    case "1":
                        await LeaderElectionExamples.Example1_BasicElectionAsync(redis);
                        break;
                    case "2":
                        await LeaderElectionExamples.Example2_LeadershipHeartbeatAsync(redis);
                        break;
                    case "3":
                        await LeaderElectionExamples.Example3_MultipleCompetitorsAsync(redis);
                        break;
                    case "4":
                        await LeaderElectionExamples.Example4_LeadershipTransferAsync(redis);
                        break;
                    case "5":
                        await LeaderElectionExamples.Example5_FollowerDiscoveryAsync(redis);
                        break;
                    case "6":
                        await LeaderElectionExamples.Example6_StateMonitoringAsync(redis);
                        break;
                    default:
                        Console.WriteLine("Invalid choice. Running Example 1...");
                        await LeaderElectionExamples.Example1_BasicElectionAsync(redis);
                        break;
                }

                Console.WriteLine("✓ Leader Election demo completed");
            }
            catch
            {
                Console.WriteLine($"⚠️  Redis not available. Ensure Redis is running at {redisConnectionString}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error running Leader Election demo: {ex.Message}");
        }
    }

}

/// <summary>
/// Interactive examples demonstrating distributed leader election patterns.
/// </summary>
public static class LeaderElectionExamples
{
    /// <summary>
    /// Example 1: Basic leader election with a single candidate.
    /// </summary>
    public static async Task Example1_BasicElectionAsync(IConnectionMultiplexer redis)
    {
        var provider = new RedisLeaderElectionProvider(redis);
        var election = new Coordination.DistributedLeaderElection("game-server-leader", provider);

        Console.WriteLine("\n=== Example 1: Basic Leader Election ===");
        Console.WriteLine("Candidate 'server-1' attempting to become leader...");

        var result = await election.CandidateAsync("server-1");
        Console.WriteLine($"Result: {result}");

        var state = await election.GetLeaderAsync();
        Console.WriteLine($"Current leader: {state.LeaderId}");
        Console.WriteLine($"Elected at: {state.ElectedAt}");
        Console.WriteLine($"Renewal deadline: {state.RenewalDeadline}");
    }

    /// <summary>
    /// Example 2: Leadership heartbeat and renewal.
    /// </summary>
    public static async Task Example2_LeadershipHeartbeatAsync(IConnectionMultiplexer redis)
    {
        var options = new LeaderElectionOptions
        {
            StateTtl = TimeSpan.FromSeconds(5),
            RenewalInterval = TimeSpan.FromSeconds(2),
            CheckInterval = TimeSpan.FromMilliseconds(500)
        };

        var provider = new RedisLeaderElectionProvider(redis);
        var election = new Coordination.DistributedLeaderElection("heartbeat-election", provider, options);

        Console.WriteLine("\n=== Example 2: Leadership Heartbeat and Renewal ===");

        // Acquire leadership
        var candidacyResult = await election.CandidateAsync("leader-1");
        Console.WriteLine($"Initial candidacy: {candidacyResult.IsSuccessful}");

        // Simulate heartbeat renewal
        for (int i = 1; i <= 3; i++)
        {
            await Task.Delay(options.RenewalInterval);

            var renewalResult = await election.RenewAsync("leader-1");
            Console.WriteLine($"Renewal #{i}: {(renewalResult.IsSuccessful ? "Success" : "Failed")}");

            var state = await election.GetLeaderAsync();
            Console.WriteLine($"  Renewal count: {state.RenewalCount}, Deadline: {state.RenewalDeadline}");
        }
    }

    /// <summary>
    /// Example 3: Multiple candidates competing for leadership.
    /// </summary>
    public static async Task Example3_MultipleCompetitorsAsync(IConnectionMultiplexer redis)
    {
        var options = new LeaderElectionOptions { StateTtl = TimeSpan.FromSeconds(10) };
        var provider = new RedisLeaderElectionProvider(redis);
        var election = new Coordination.DistributedLeaderElection("competition-election", provider, options);

        Console.WriteLine("\n=== Example 3: Multiple Competitors ===");

        // Reset first
        await election.ResetAsync();

        // First candidate wins
        var result1 = await election.CandidateAsync("candidate-1");
        Console.WriteLine($"Candidate-1: {(result1.IsSuccessful ? "Leader!" : "Lost")}");

        // Other candidates try
        var result2 = await election.CandidateAsync("candidate-2");
        Console.WriteLine($"Candidate-2: {(result2.IsSuccessful ? "Leader!" : "Lost")} (current leader: {result2.LeaderId})");

        var result3 = await election.CandidateAsync("candidate-3");
        Console.WriteLine($"Candidate-3: {(result3.IsSuccessful ? "Leader!" : "Lost")} (current leader: {result3.LeaderId})");
    }

    /// <summary>
    /// Example 4: Leadership transfer on yield.
    /// </summary>
    public static async Task Example4_LeadershipTransferAsync(IConnectionMultiplexer redis)
    {
        var options = new LeaderElectionOptions { StateTtl = TimeSpan.FromSeconds(10) };
        var provider = new RedisLeaderElectionProvider(redis);
        var election = new Coordination.DistributedLeaderElection("transfer-election", provider, options);

        Console.WriteLine("\n=== Example 4: Leadership Transfer ===");

        // Reset and establish first leader
        await election.ResetAsync();
        var result1 = await election.CandidateAsync("leader-1");
        Console.WriteLine($"Leader-1 acquired leadership: {result1.IsSuccessful}");

        // Leader-1 yields
        Console.WriteLine("Leader-1 yielding leadership...");
        await election.YieldAsync("leader-1");

        var state = await election.GetLeaderAsync();
        Console.WriteLine($"Leadership released. Current leader: {state.LeaderId}");

        // New candidate can now acquire
        var result2 = await election.CandidateAsync("leader-2");
        Console.WriteLine($"Leader-2 acquired leadership: {result2.IsSuccessful}");

        state = await election.GetLeaderAsync();
        Console.WriteLine($"New leader: {state.LeaderId}");
    }

    /// <summary>
    /// Example 5: Follower discovers leader change.
    /// </summary>
    public static async Task Example5_FollowerDiscoveryAsync(IConnectionMultiplexer redis)
    {
        var options = new LeaderElectionOptions
        {
            StateTtl = TimeSpan.FromSeconds(5),
            CheckInterval = TimeSpan.FromMilliseconds(200)
        };

        var provider = new RedisLeaderElectionProvider(redis);
        var election = new Coordination.DistributedLeaderElection("discovery-election", provider, options);

        Console.WriteLine("\n=== Example 5: Follower Discovers Leadership Change ===");

        // Reset and establish first leader
        await election.ResetAsync();
        var result = await election.CandidateAsync("leader-1");
        Console.WriteLine($"Initial leader: leader-1 ({result.IsSuccessful})");

        // Spawn a task to wait for leader change
        var changeTask = Task.Run(async () =>
        {
            Console.WriteLine("Follower waiting for leader change...");
            try
            {
                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
                var newState = await election.WaitForLeaderChangeAsync(cts.Token);
                Console.WriteLine($"Follower detected change! New leader: {newState.LeaderId}");
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Follower timeout waiting for change.");
            }
        });

        // After 3 seconds, leader yields
        await Task.Delay(TimeSpan.FromSeconds(3));
        Console.WriteLine("Leader yielding...");
        await election.YieldAsync("leader-1");

        // Wait for follower to detect change
        await changeTask;
    }

    /// <summary>
    /// Example 6: Leader state monitoring.
    /// </summary>
    public static async Task Example6_StateMonitoringAsync(IConnectionMultiplexer redis)
    {
        var options = new LeaderElectionOptions { StateTtl = TimeSpan.FromSeconds(15) };
        var provider = new RedisLeaderElectionProvider(redis);
        var election = new Coordination.DistributedLeaderElection("monitoring-election", provider, options);

        Console.WriteLine("\n=== Example 6: Leader State Monitoring ===");

        // Reset
        await election.ResetAsync();

        // Acquire leadership
        await election.CandidateAsync("monitor-leader");

        // Poll state over time
        for (int i = 0; i < 4; i++)
        {
            var state = await election.GetLeaderAsync();

            Console.WriteLine($"\nPoll #{i + 1}:");
            Console.WriteLine($"  Leader ID: {state.LeaderId}");
            Console.WriteLine($"  Elected At: {state.ElectedAt}");
            Console.WriteLine($"  Renewal Deadline: {state.RenewalDeadline}");
            Console.WriteLine($"  Renewal Count: {state.RenewalCount}");

            if (i < 3)
            {
                await Task.Delay(TimeSpan.FromSeconds(3));
                if (i == 1)
                {
                    Console.WriteLine("  -> Renewing leadership...");
                    await election.RenewAsync("monitor-leader");
                }
            }
        }
    }
}
