namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Represents the state of a leader election.
/// </summary>
public class LeaderElectionState
{
    /// <summary>
    /// Gets the current leader ID, or null if no leader is elected.
    /// </summary>
    public string? LeaderId { get; set; }

    /// <summary>
    /// Gets the key associated with the leader.
    /// </summary>
    public string? LeaderKey { get; set; }

    /// <summary>
    /// Gets when the current leader was elected.
    /// </summary>
    public DateTime? ElectedAt { get; set; }

    /// <summary>
    /// Gets when the current leader's term will expire if not renewed.
    /// </summary>
    public DateTime? RenewalDeadline { get; set; }

    /// <summary>
    /// Gets the number of times the current leader has been renewed.
    /// </summary>
    public int RenewalCount { get; set; }
}