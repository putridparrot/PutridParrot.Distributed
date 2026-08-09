namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Result of a queue operation.
/// </summary>
public class QueueResult
{
    /// <summary>
    /// Gets a value indicating whether the operation was successful.
    /// </summary>
    public bool IsSuccessful { get; set; }

    /// <summary>
    /// Gets the work item affected by this operation.
    /// </summary>
    public WorkItem? WorkItem { get; set; }

    /// <summary>
    /// Gets the current queue state.
    /// </summary>
    public QueueState? State { get; set; }

    /// <summary>
    /// Gets a message describing the result.
    /// </summary>
    public string? Message { get; set; }

    /// <summary>
    /// Gets the time when this result was generated.
    /// </summary>
    public DateTime Timestamp { get; set; }

    /// <summary>
    /// Returns a string representation of this result.
    /// </summary>
    public override string ToString()
    {
        return IsSuccessful ? 
            $"QueueResult {{ IsSuccessful=true, WorkItem.Id={WorkItem?.Id} }}" : 
            $"QueueResult {{ IsSuccessful=false, Message={Message} }}";
    }
}