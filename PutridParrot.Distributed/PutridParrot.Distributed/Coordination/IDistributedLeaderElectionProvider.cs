namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Backend provider contract for distributed leader election.
/// Manages leader candidacy, renewal, and discovery across multiple processes/servers.
/// </summary>
public interface IDistributedLeaderElectionProvider
{
    /// <summary>
    /// Attempts to become a candidate for leadership.
    /// Returns true if leadership was acquired, false if another candidate is already leader.
    /// </summary>
    /// <param name="leaderKey">Unique key identifying this election.</param>
    /// <param name="candidateId">Unique ID for this candidate.</param>
    /// <param name="options">Election options including timeout and renewal interval.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> CandidateAsync(
        string leaderKey,
        string candidateId,
        LeaderElectionOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Renews the current leader's term, extending the leadership deadline.
    /// Returns true if renewal was successful, false if leadership was lost.
    /// </summary>
    /// <param name="leaderKey">Unique key identifying this election.</param>
    /// <param name="candidateId">ID of the current leader.</param>
    /// <param name="options">Election options including renewal interval.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> RenewAsync(
        string leaderKey,
        string candidateId,
        LeaderElectionOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Yields leadership, allowing another candidate to be elected.
    /// </summary>
    /// <param name="leaderKey">Unique key identifying this election.</param>
    /// <param name="candidateId">ID of the current leader.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task YieldAsync(
        string leaderKey,
        string candidateId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the current leader state without attempting to acquire leadership.
    /// </summary>
    /// <param name="leaderKey">Unique key identifying this election.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LeaderElectionState> GetLeaderAsync(
        string leaderKey,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Resets the leader election state, clearing all leadership information.
    /// Useful for testing and resetting stuck elections.
    /// </summary>
    /// <param name="leaderKey">Unique key identifying this election.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ResetAsync(
        string leaderKey,
        CancellationToken cancellationToken = default);
}