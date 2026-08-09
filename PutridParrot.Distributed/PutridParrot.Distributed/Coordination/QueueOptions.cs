namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Options for distributed queue operations.
/// </summary>
public class QueueOptions
{
    /// <summary>
    /// Gets or sets the visibility timeout for dequeued items.
    /// If the item is not acknowledged within this time, it becomes available for other workers.
    /// Default: 30 seconds.
    /// </summary>
    public TimeSpan VisibilityTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the maximum number of attempts before moving to dead letter queue.
    /// Default: 3.
    /// </summary>
    public int MaxAttempts { get; set; } = 3;

    /// <summary>
    /// Gets or sets the time-to-live for items in the queue before they expire.
    /// Default: 1 hour.
    /// </summary>
    public TimeSpan ItemTtl { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Gets or sets whether to support priority queues (higher priority items dequeued first).
    /// Default: false.
    /// </summary>
    public bool EnablePriorityQueue { get; set; } = false;

    /// <summary>
    /// Gets or sets the poll timeout when waiting for items (for blocking dequeue).
    /// Default: 1 second.
    /// </summary>
    public TimeSpan PollTimeout { get; set; } = TimeSpan.FromSeconds(1);
}