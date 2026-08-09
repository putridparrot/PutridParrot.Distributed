namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Backend provider contract for distributed queue operations.
/// Manages work item storage, retrieval, and state transitions across multiple processes/servers.
/// </summary>
public interface IDistributedQueueProvider
{
    /// <summary>
    /// Enqueues a work item to the distributed queue.
    /// </summary>
    /// <param name="queueName">Name of the queue.</param>
    /// <param name="workItem">Work item to enqueue.</param>
    /// <param name="options">Queue options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The enqueued work item with ID assigned.</returns>
    Task<WorkItem> EnqueueAsync(
        string queueName,
        WorkItem workItem,
        QueueOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Dequeues the next available work item from the queue.
    /// Marks the item as processing with a visibility timeout.
    /// </summary>
    /// <param name="queueName">Name of the queue.</param>
    /// <param name="workerId">Identifier of the worker claiming this work.</param>
    /// <param name="options">Queue options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The next work item, or null if queue is empty.</returns>
    Task<WorkItem?> DequeueAsync(
        string queueName,
        string workerId,
        QueueOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledges successful processing of a work item and removes it from the queue.
    /// </summary>
    /// <param name="queueName">Name of the queue.</param>
    /// <param name="workItemId">ID of the work item to acknowledge.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task AcknowledgeAsync(
        string queueName,
        string workItemId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Negatively acknowledges (nacks) a work item, returning it to pending state for retry.
    /// </summary>
    /// <param name="queueName">Name of the queue.</param>
    /// <param name="workItemId">ID of the work item.</param>
    /// <param name="errorMessage">Optional error message.</param>
    /// <param name="options">Queue options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task NackAsync(
        string queueName,
        string workItemId,
        string? errorMessage,
        QueueOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves a work item to the dead letter queue after max retries exceeded.
    /// </summary>
    /// <param name="queueName">Name of the queue.</param>
    /// <param name="workItemId">ID of the work item.</param>
    /// <param name="reason">Reason for moving to DLQ.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MoveToDeadLetterAsync(
        string queueName,
        string workItemId,
        string reason,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the current state of the queue (pending count, processing count, etc).
    /// </summary>
    /// <param name="queueName">Name of the queue.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Queue state with item counts.</returns>
    Task<QueueState> GetStateAsync(
        string queueName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Resets the queue, clearing all items.
    /// </summary>
    /// <param name="queueName">Name of the queue.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ResetAsync(
        string queueName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves items from the dead letter queue for inspection/recovery.
    /// </summary>
    /// <param name="queueName">Name of the queue.</param>
    /// <param name="options">Queue options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Dead letter work items.</returns>
    Task<IEnumerable<WorkItem>> GetDeadLetterItemsAsync(
        string queueName,
        QueueOptions options,
        CancellationToken cancellationToken = default);
}