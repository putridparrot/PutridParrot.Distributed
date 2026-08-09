using PutridParrot.Distributed.Coordination;
using StackExchange.Redis;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PutridParrot.Distributed.Redis.Coordination;

/// <summary>
/// Redis implementation of the distributed queue provider using hash-based storage.
/// </summary>
public class RedisQueueProvider : IDistributedQueueProvider
{
    private readonly IDatabase _db;
    private readonly string _pendingSetKey;
    private readonly string _processingSetKey;
    private readonly string _deadLetterSetKey;
    private readonly string _itemHashPrefix;
    private readonly string _stateKey;

    /// <summary>
    /// Initializes a new instance of the RedisQueueProvider class.
    /// </summary>
    /// <param name="database">Redis database connection.</param>
    public RedisQueueProvider(IDatabase database)
    {
        _db = database ?? throw new ArgumentNullException(nameof(database));

        // These are virtual queue names in Redis; the actual queue name is passed at operation time
        _pendingSetKey = "queue:pending";
        _processingSetKey = "queue:processing";
        _deadLetterSetKey = "queue:deadletter";
        _itemHashPrefix = "queue:item:";
        _stateKey = "queue:state";
    }

    public async Task<WorkItem> EnqueueAsync(
        string queueName,
        WorkItem workItem,
        QueueOptions options,
        CancellationToken cancellationToken = default)
    {
        var itemKey = $"{_itemHashPrefix}{workItem.Id}";
        var json = JsonSerializer.Serialize(workItem, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        });

        var pendingKey = $"{queueName}:{_pendingSetKey}";

        // Store the work item and add to pending queue
        await _db.StringSetAsync(itemKey, json, options.ItemTtl);
        _db.SortedSetAdd(pendingKey, workItem.Id, workItem.Priority);

        // Update state counters
        _db.StringIncrement($"{queueName}:{_stateKey}:total");

        return workItem;
    }

    public async Task<WorkItem?> DequeueAsync(
        string queueName,
        string workerId,
        QueueOptions options,
        CancellationToken cancellationToken = default)
    {
        var pendingKey = $"{queueName}:{_pendingSetKey}";
        var processingKey = $"{queueName}:{_processingSetKey}";

        // Pop the highest priority item from pending (use Range instead of PopByRank)
        var entries = _db.SortedSetRangeByRank(pendingKey, 0, 0, order: Order.Descending);

        if (entries.Length == 0)
        {
            return null;
        }

        var workItemId = entries[0].ToString();

        // Remove from pending
        _db.SortedSetRemove(pendingKey, workItemId);

        var itemKey = $"{_itemHashPrefix}{workItemId}";

        // Retrieve the work item
        var json = await _db.StringGetAsync(itemKey);
        if (!json.HasValue)
        {
            return null;
        }

        var workItem = JsonSerializer.Deserialize<WorkItem>(json.ToString(), new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        if (workItem == null)
        {
            return null;
        }

        // Update work item state
        workItem.State = WorkItemState.Processing;
        workItem.WorkerId = workerId;
        workItem.AttemptCount++;
        workItem.VisibilityDeadline = DateTime.UtcNow.Add(options.VisibilityTimeout);
        workItem.UpdatedAt = DateTime.UtcNow;

        // Store updated item and add to processing
        var updatedJson = JsonSerializer.Serialize(workItem, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        });
        await _db.StringSetAsync(itemKey, updatedJson, options.ItemTtl);
        _db.SortedSetAdd(processingKey, workItemId, DateTime.UtcNow.Ticks);

        return workItem;
    }

    public async Task AcknowledgeAsync(
        string queueName,
        string workItemId,
        CancellationToken cancellationToken = default)
    {
        var itemKey = $"{_itemHashPrefix}{workItemId}";
        var processingKey = $"{queueName}:{_processingSetKey}";

        // Remove from processing and delete the work item
        _db.SortedSetRemove(processingKey, workItemId);
        await _db.KeyDeleteAsync(itemKey);

        // Update state counters
        _db.StringIncrement($"{queueName}:{_stateKey}:completed");
    }

    public async Task NackAsync(
        string queueName,
        string workItemId,
        string? errorMessage,
        QueueOptions options,
        CancellationToken cancellationToken = default)
    {
        var itemKey = $"{_itemHashPrefix}{workItemId}";
        var processingKey = $"{queueName}:{_processingSetKey}";
        var pendingKey = $"{queueName}:{_pendingSetKey}";

        // Retrieve the work item
        var json = await _db.StringGetAsync(itemKey);
        if (!json.HasValue)
        {
            return;
        }

        var workItem = JsonSerializer.Deserialize<WorkItem>(json.ToString(), new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        if (workItem == null)
        {
            return;
        }

        // Remove from processing
        _db.SortedSetRemove(processingKey, workItemId);

        // Check if max attempts exceeded
        if (workItem.AttemptCount >= options.MaxAttempts)
        {
            await MoveToDeadLetterAsync(queueName, workItemId, errorMessage ?? "Max attempts exceeded", cancellationToken);
        }
        else
        {
            // Return to pending with reset visibility
            workItem.State = WorkItemState.Pending;
            workItem.WorkerId = null;
            workItem.VisibilityDeadline = null;
            workItem.ErrorMessage = errorMessage;
            workItem.UpdatedAt = DateTime.UtcNow;

            var updatedJson = JsonSerializer.Serialize(workItem, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            });
            await _db.StringSetAsync(itemKey, updatedJson, options.ItemTtl);
            _db.SortedSetAdd(pendingKey, workItemId, workItem.Priority);
        }
    }

    public async Task MoveToDeadLetterAsync(
        string queueName,
        string workItemId,
        string reason,
        CancellationToken cancellationToken = default)
    {
        var itemKey = $"{_itemHashPrefix}{workItemId}";
        var processingKey = $"{queueName}:{_processingSetKey}";
        var deadLetterKey = $"{queueName}:{_deadLetterSetKey}";

        // Retrieve and update the work item
        var json = await _db.StringGetAsync(itemKey);
        if (!json.HasValue)
        {
            return;
        }

        var workItem = JsonSerializer.Deserialize<WorkItem>(json.ToString(), new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });

        if (workItem == null)
        {
            return;
        }

        workItem.State = WorkItemState.DeadLetter;
        workItem.ErrorMessage = reason;
        workItem.UpdatedAt = DateTime.UtcNow;

        var updatedJson = JsonSerializer.Serialize(workItem, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        });

        // Remove from processing, add to dead letter
        _db.SortedSetRemove(processingKey, workItemId);
        await _db.StringSetAsync(itemKey, updatedJson, TimeSpan.FromDays(7)); // DLQ retention
        _db.SortedSetAdd(deadLetterKey, workItemId, DateTime.UtcNow.Ticks);

        _db.StringIncrement($"{queueName}:{_stateKey}:deadletter");
    }

    public async Task<QueueState> GetStateAsync(
        string queueName,
        CancellationToken cancellationToken = default)
    {
        var pendingKey = $"{queueName}:{_pendingSetKey}";
        var processingKey = $"{queueName}:{_processingSetKey}";
        var deadLetterKey = $"{queueName}:{_deadLetterSetKey}";

        var pendingCount = _db.SortedSetLength(pendingKey);
        var processingCount = _db.SortedSetLength(processingKey);
        var deadLetterCount = _db.SortedSetLength(deadLetterKey);

        var completedVal = await _db.StringGetAsync($"{queueName}:{_stateKey}:completed");
        var totalVal = await _db.StringGetAsync($"{queueName}:{_stateKey}:total");

        return new QueueState
        {
            QueueName = queueName,
            PendingCount = pendingCount,
            ProcessingCount = processingCount,
            DeadLetterCount = deadLetterCount,
            CompletedCount = completedVal.HasValue ? long.Parse(completedVal.ToString()) : 0,
            TotalProcessed = totalVal.HasValue ? long.Parse(totalVal.ToString()) : 0,
            Timestamp = DateTime.UtcNow
        };
    }

    public async Task ResetAsync(
        string queueName,
        CancellationToken cancellationToken = default)
    {
        var pendingKey = $"{queueName}:{_pendingSetKey}";
        var processingKey = $"{queueName}:{_processingSetKey}";
        var deadLetterKey = $"{queueName}:{_deadLetterSetKey}";

        // Get all work items to delete
        var allItems = new List<string>();
        allItems.AddRange(_db.SortedSetRangeByRank(pendingKey).Select(x => x.ToString()));
        allItems.AddRange(_db.SortedSetRangeByRank(processingKey).Select(x => x.ToString()));
        allItems.AddRange(_db.SortedSetRangeByRank(deadLetterKey).Select(x => x.ToString()));

        // Delete all keys
        foreach (var itemId in allItems)
        {
            await _db.KeyDeleteAsync($"{_itemHashPrefix}{itemId}");
        }

        _db.KeyDelete(pendingKey);
        _db.KeyDelete(processingKey);
        _db.KeyDelete(deadLetterKey);
        _db.KeyDelete($"{queueName}:{_stateKey}:total");
        _db.KeyDelete($"{queueName}:{_stateKey}:completed");
        _db.KeyDelete($"{queueName}:{_stateKey}:deadletter");
    }

    public async Task<IEnumerable<WorkItem>> GetDeadLetterItemsAsync(
        string queueName,
        QueueOptions options,
        CancellationToken cancellationToken = default)
    {
        var deadLetterKey = $"{queueName}:{_deadLetterSetKey}";
        var items = new List<WorkItem>();

        var deadLetterIds = _db.SortedSetRangeByRank(deadLetterKey);
        foreach (var idEntry in deadLetterIds)
        {
            var itemId = idEntry.ToString();
            var json = await _db.StringGetAsync($"{_itemHashPrefix}{itemId}");

            if (json.HasValue)
            {
                var workItem = JsonSerializer.Deserialize<WorkItem>(json.ToString(), new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });

                if (workItem != null)
                {
                    items.Add(workItem);
                }
            }
        }

        return items;
    }
}
