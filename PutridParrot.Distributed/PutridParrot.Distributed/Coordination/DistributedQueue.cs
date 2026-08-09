
namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Main facade for distributed queue operations.
/// Provides high-level operations for enqueueing and dequeueing work across multiple workers.
/// </summary>
public class DistributedQueue
{
    private readonly IDistributedQueueProvider _provider;
    private readonly QueueOptions _options;

    /// <summary>
    /// Gets the name of this queue.
    /// </summary>
    public string QueueName { get; }

    /// <summary>
    /// Initializes a new instance of the DistributedQueue class.
    /// </summary>
    /// <param name="queueName">Unique name for this queue.</param>
    /// <param name="provider">Backend provider for queue operations.</param>
    /// <param name="options">Queue options.</param>
    /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
    public DistributedQueue(
        string queueName,
        IDistributedQueueProvider provider,
        QueueOptions? options = null)
    {
        QueueName = queueName ?? throw new ArgumentNullException(nameof(queueName));
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _options = options ?? new QueueOptions();
    }

    /// <summary>
    /// Enqueues a work item to the queue.
    /// </summary>
    /// <param name="payload">The work item payload (typically JSON).</param>
    /// <param name="priority">Optional priority (higher = more urgent).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The enqueued work item with assigned ID.</returns>
    public async Task<QueueResult> EnqueueAsync(
        string payload,
        int priority = 0,
        CancellationToken cancellationToken = default)
    {
        var workItem = new WorkItem
        {
            Id = Guid.NewGuid().ToString(),
            Payload = payload,
            State = WorkItemState.Pending,
            EnqueuedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
            AttemptCount = 0,
            Priority = priority
        };

        var result = await _provider.EnqueueAsync(QueueName, workItem, _options, cancellationToken);
        var state = await GetStateAsync(cancellationToken);

        return new QueueResult
        {
            IsSuccessful = true,
            WorkItem = result,
            State = state,
            Timestamp = DateTime.UtcNow,
            Message = $"Work item {result.Id} enqueued successfully"
        };
    }

    /// <summary>
    /// Dequeues the next available work item for processing.
    /// </summary>
    /// <param name="workerId">Identifier of the worker claiming this work.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A queue result with the dequeued work item, or null if queue is empty.</returns>
    public async Task<QueueResult> DequeueAsync(
        string workerId,
        CancellationToken cancellationToken = default)
    {
        var workItem = await _provider.DequeueAsync(QueueName, workerId, _options, cancellationToken);
        var state = await GetStateAsync(cancellationToken);

        if (workItem == null)
        {
            return new QueueResult
            {
                IsSuccessful = false,
                WorkItem = null,
                State = state,
                Timestamp = DateTime.UtcNow,
                Message = "Queue is empty"
            };
        }

        return new QueueResult
        {
            IsSuccessful = true,
            WorkItem = workItem,
            State = state,
            Timestamp = DateTime.UtcNow,
            Message = $"Dequeued work item {workItem.Id} (attempt {workItem.AttemptCount})"
        };
    }

    /// <summary>
    /// Acknowledges successful processing of a work item.
    /// </summary>
    /// <param name="workItemId">ID of the work item.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task AcknowledgeAsync(
        string workItemId,
        CancellationToken cancellationToken = default)
    {
        await _provider.AcknowledgeAsync(QueueName, workItemId, cancellationToken);
    }

    /// <summary>
    /// Negatively acknowledges a work item, returning it to the queue for retry.
    /// </summary>
    /// <param name="workItemId">ID of the work item.</param>
    /// <param name="errorMessage">Optional error message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task NackAsync(
        string workItemId,
        string? errorMessage = null,
        CancellationToken cancellationToken = default)
    {
        await _provider.NackAsync(QueueName, workItemId, errorMessage, _options, cancellationToken);
    }

    /// <summary>
    /// Moves a work item to the dead letter queue.
    /// </summary>
    /// <param name="workItemId">ID of the work item.</param>
    /// <param name="reason">Reason for moving to DLQ.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task MoveToDeadLetterAsync(
        string workItemId,
        string reason,
        CancellationToken cancellationToken = default)
    {
        await _provider.MoveToDeadLetterAsync(QueueName, workItemId, reason, cancellationToken);
    }

    /// <summary>
    /// Gets the current state of the queue.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Queue state with item counts.</returns>
    public async Task<QueueState> GetStateAsync(CancellationToken cancellationToken = default)
    {
        return await _provider.GetStateAsync(QueueName, cancellationToken);
    }

    /// <summary>
    /// Resets the queue, clearing all items.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task ResetAsync(CancellationToken cancellationToken = default)
    {
        await _provider.ResetAsync(QueueName, cancellationToken);
    }

    /// <summary>
    /// Gets items from the dead letter queue.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Dead letter work items.</returns>
    public async Task<IEnumerable<WorkItem>> GetDeadLetterItemsAsync(
        CancellationToken cancellationToken = default)
    {
        return await _provider.GetDeadLetterItemsAsync(QueueName, _options, cancellationToken);
    }

    /// <summary>
    /// Processes a dequeued work item with automatic retry and dead letter handling.
    /// </summary>
    /// <param name="workerId">Worker identifier.</param>
    /// <param name="processFunc">Function to process the work item.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task ProcessNextAsync(
        string workerId,
        Func<WorkItem, Task> processFunc,
        CancellationToken cancellationToken = default)
    {
        var dequeueResult = await DequeueAsync(workerId, cancellationToken);
        if (!dequeueResult.IsSuccessful || dequeueResult.WorkItem == null)
        {
            return;
        }

        var workItem = dequeueResult.WorkItem;

        try
        {
            await processFunc(workItem);
            await AcknowledgeAsync(workItem.Id!, cancellationToken);
        }
        catch (Exception ex)
        {
            if (workItem.AttemptCount >= _options.MaxAttempts)
            {
                await MoveToDeadLetterAsync(workItem.Id!, $"Max attempts exceeded: {ex.Message}", cancellationToken);
            }
            else
            {
                await NackAsync(workItem.Id!, ex.Message, cancellationToken);
            }
        }
    }
}

/// <summary>
/// Enum representing the state of a work item in the queue.
/// </summary>
public enum WorkItemState
{
    /// <summary>
    /// Work item is pending processing.
    /// </summary>
    Pending = 0,

    /// <summary>
    /// Work item is currently being processed.
    /// </summary>
    Processing = 1,

    /// <summary>
    /// Work item has been successfully processed.
    /// </summary>
    Completed = 2,

    /// <summary>
    /// Work item has failed and moved to dead letter queue.
    /// </summary>
    DeadLetter = 3,

    /// <summary>
    /// Work item has been acknowledged and is waiting for confirmation.
    /// </summary>
    Acknowledged = 4
}

/// <summary>
/// Represents a work item in the distributed queue.
/// </summary>
public class WorkItem
{
    /// <summary>
    /// Gets the unique identifier for this work item.
    /// </summary>
    public string? Id { get; set; }

    /// <summary>
    /// Gets the payload/data of the work item.
    /// </summary>
    public string? Payload { get; set; }

    /// <summary>
    /// Gets the current state of the work item.
    /// </summary>
    public WorkItemState State { get; set; }

    /// <summary>
    /// Gets when this work item was enqueued.
    /// </summary>
    public DateTime EnqueuedAt { get; set; }

    /// <summary>
    /// Gets when this work item was last updated.
    /// </summary>
    public DateTime UpdatedAt { get; set; }

    /// <summary>
    /// Gets when the visibility timeout expires (item can be claimed again).
    /// </summary>
    public DateTime? VisibilityDeadline { get; set; }

    /// <summary>
    /// Gets the number of times this work item has been attempted.
    /// </summary>
    public int AttemptCount { get; set; }

    /// <summary>
    /// Gets optional priority (higher value = higher priority).
    /// </summary>
    public int Priority { get; set; }

    /// <summary>
    /// Gets the worker that currently holds this work item (if processing).
    /// </summary>
    public string? WorkerId { get; set; }

    /// <summary>
    /// Gets optional error message if work item failed.
    /// </summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// Returns a string representation of this work item.
    /// </summary>
    public override string ToString()
    {
        return $"WorkItem {{ Id={Id}, State={State}, Attempts={AttemptCount}, Priority={Priority} }}";
    }
}