namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Represents the state of a distributed queue.
/// </summary>
public class QueueState
{
    /// <summary>
    /// Gets the name of the queue.
    /// </summary>
    public string? QueueName { get; set; }

    /// <summary>
    /// Gets the number of pending items.
    /// </summary>
    public long PendingCount { get; set; }

    /// <summary>
    /// Gets the number of items currently being processed.
    /// </summary>
    public long ProcessingCount { get; set; }

    /// <summary>
    /// Gets the number of items in the dead letter queue.
    /// </summary>
    public long DeadLetterCount { get; set; }

    /// <summary>
    /// Gets the number of completed items.
    /// </summary>
    public long CompletedCount { get; set; }

    /// <summary>
    /// Gets the total number of items ever in this queue.
    /// </summary>
    public long TotalProcessed { get; set; }

    /// <summary>
    /// Gets when the queue state was retrieved.
    /// </summary>
    public DateTime Timestamp { get; set; }
}