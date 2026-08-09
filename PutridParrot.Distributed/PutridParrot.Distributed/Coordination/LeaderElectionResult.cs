namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Result of a leader election operation.
/// </summary>
public class LeaderElectionResult
{
    /// <summary>
    /// Gets a value indicating whether the operation was successful.
    /// For candidacy: true if leadership was acquired.
    /// For renewal: true if the renewal succeeded (leadership retained).
    /// </summary>
    public bool IsSuccessful { get; set; }

    /// <summary>
    /// Gets the ID of the candidate.
    /// </summary>
    public string? CandidateId { get; set; }

    /// <summary>
    /// Gets the ID of the current leader (may differ from CandidateId if candidacy failed).
    /// </summary>
    public string? LeaderId { get; set; }

    /// <summary>
    /// Gets the time when this result was generated.
    /// </summary>
    public DateTime Timestamp { get; set; }

    /// <summary>
    /// Gets the current leader state.
    /// </summary>
    public LeaderElectionState? State { get; set; }

    /// <summary>
    /// Gets a message describing the result.
    /// </summary>
    public string? Message { get; set; }

    /// <summary>
    /// Returns a string representation of this result.
    /// </summary>
    public override string ToString()
    {
        return IsSuccessful ? 
            $"LeaderElectionResult {{ IsSuccessful=true, CandidateId={CandidateId}, LeaderId={LeaderId} }}" : 
            $"LeaderElectionResult {{ IsSuccessful=false, CandidateId={CandidateId}, LeaderId={LeaderId}, Message={Message} }}";
    }
}