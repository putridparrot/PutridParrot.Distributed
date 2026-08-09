namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Options for distributed leader election.
/// </summary>
public class LeaderElectionOptions
{
    /// <summary>
    /// Gets or sets the timeout for leader candidacy and renewal operations.
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan CandidacyTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the interval at which to poll for leadership changes.
    /// Default: 100 milliseconds.
    /// </summary>
    public TimeSpan CheckInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Gets or sets the time-to-live for the leader state in the backend.
    /// Default: 1 minute.
    /// </summary>
    public TimeSpan StateTtl { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Gets or sets the interval at which the leader should renew its term.
    /// Recommended to be less than StateTtl to avoid missed renewals.
    /// Default: 10 seconds.
    /// </summary>
    public TimeSpan RenewalInterval { get; set; } = TimeSpan.FromSeconds(10);
}