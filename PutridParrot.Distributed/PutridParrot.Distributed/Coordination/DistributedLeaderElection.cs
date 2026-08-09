namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Main facade for distributed leader election.
/// Provides high-level operations for candidates to vie for leadership, renew their term, and yield to others.
/// </summary>
public class DistributedLeaderElection
{
    private readonly IDistributedLeaderElectionProvider _provider;
    private readonly LeaderElectionOptions _options;

    /// <summary>
    /// Gets the unique key identifying this leader election.
    /// </summary>
    public string LeaderKey { get; }

    /// <summary>
    /// Initializes a new instance of the DistributedLeaderElection class.
    /// </summary>
    /// <param name="leaderKey">Unique key identifying this election.</param>
    /// <param name="provider">Backend provider for leader election operations.</param>
    /// <param name="options">Election options.</param>
    /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
    public DistributedLeaderElection(
        string leaderKey,
        IDistributedLeaderElectionProvider provider,
        LeaderElectionOptions? options = null)
    {
        LeaderKey = leaderKey ?? throw new ArgumentNullException(nameof(leaderKey));
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _options = options ?? new LeaderElectionOptions();
    }

    /// <summary>
    /// Attempts to acquire leadership for the given candidate.
    /// </summary>
    /// <param name="candidateId">Unique ID for this candidate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A LeaderElectionResult where IsSuccessful is true if leadership was acquired.
    /// </returns>
    public async Task<LeaderElectionResult> CandidateAsync(
        string candidateId,
        CancellationToken cancellationToken = default)
    {
        var isLeader = await _provider.CandidateAsync(
            LeaderKey,
            candidateId,
            _options,
            cancellationToken);

        var state = await _provider.GetLeaderAsync(LeaderKey, cancellationToken);

        return new LeaderElectionResult
        {
            IsSuccessful = isLeader,
            CandidateId = candidateId,
            LeaderId = state.LeaderId,
            Timestamp = DateTime.UtcNow,
            State = state,
            Message = isLeader
                ? $"Candidate {candidateId} successfully acquired leadership."
                : $"Candidate {candidateId} could not acquire leadership; {state.LeaderId} is the current leader."
        };
    }

    /// <summary>
    /// Renews the current leader's term, extending the leadership deadline.
    /// </summary>
    /// <param name="candidateId">ID of the current leader.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A LeaderElectionResult where IsSuccessful is true if renewal succeeded.
    /// </returns>
    public async Task<LeaderElectionResult> RenewAsync(
        string candidateId,
        CancellationToken cancellationToken = default)
    {
        var isRenewed = await _provider.RenewAsync(
            LeaderKey,
            candidateId,
            _options,
            cancellationToken);

        var state = await _provider.GetLeaderAsync(LeaderKey, cancellationToken);

        return new LeaderElectionResult
        {
            IsSuccessful = isRenewed,
            CandidateId = candidateId,
            LeaderId = state.LeaderId,
            Timestamp = DateTime.UtcNow,
            State = state,
            Message = isRenewed
                ? $"Leader {candidateId} successfully renewed leadership (renewal #{state.RenewalCount})."
                : $"Leader {candidateId} could not renew leadership; lost to {state.LeaderId}."
        };
    }

    /// <summary>
    /// Yields leadership, allowing another candidate to be elected.
    /// </summary>
    /// <param name="candidateId">ID of the current leader.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task YieldAsync(
        string candidateId,
        CancellationToken cancellationToken = default)
    {
        await _provider.YieldAsync(LeaderKey, candidateId, cancellationToken);
    }

    /// <summary>
    /// Gets the current leader state without attempting to acquire or renew leadership.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The current leader state.</returns>
    public async Task<LeaderElectionState> GetLeaderAsync(CancellationToken cancellationToken = default)
    {
        return await _provider.GetLeaderAsync(LeaderKey, cancellationToken);
    }

    /// <summary>
    /// Resets the leader election state, clearing all leadership information.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task ResetAsync(CancellationToken cancellationToken = default)
    {
        await _provider.ResetAsync(LeaderKey, cancellationToken);
    }

    /// <summary>
    /// Waits for leadership to become available (leader yields or term expires).
    /// Useful for followers to detect when an election window opens.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new leader state when a change is detected.</returns>
    public async Task<LeaderElectionState> WaitForLeaderChangeAsync(CancellationToken cancellationToken = default)
    {
        var lastState = await GetLeaderAsync(cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(_options.CheckInterval, cancellationToken);

            var currentState = await GetLeaderAsync(cancellationToken);
            if (currentState.LeaderId != lastState.LeaderId || currentState.RenewalDeadline != lastState.RenewalDeadline)
            {
                return currentState;
            }
        }

        throw new OperationCanceledException("Leader election wait was cancelled.");
    }
}