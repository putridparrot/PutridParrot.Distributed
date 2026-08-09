namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Distributed idempotency key manager.
/// 
/// Ensures exactly-once semantics for operations by caching results and returning cached
/// results for retries. This prevents duplicate side effects (charges, messages, database
/// inserts, etc.) when operations are retried due to network failures.
/// 
/// Usage:
/// <code>
/// var provider = new RedisIdempotencyKeyProvider(redis);
/// var idempotency = new IdempotencyKeyProvider(provider, options);
/// 
/// // Check if operation was already done
/// var result = await idempotency.GetOrExecuteAsync(
///     idempotencyKey: "order-123-payment",
///     operation: async () => {
///         var charge = await ChargeCard(amount);
///         return JsonSerializer.Serialize(charge);
///     }
/// );
/// 
/// if (result.IsFromCache)
///     Console.WriteLine("Returning cached result (retry detected)");
/// else
///     Console.WriteLine("First execution of this operation");
/// </code>
/// </summary>
public class IdempotencyKeyProvider
{
    private readonly IDistributedIdempotencyKeyProvider _provider;
    private readonly IdempotencyKeyOptions _options;

    public IdempotencyKeyProvider(
        IDistributedIdempotencyKeyProvider provider,
        IdempotencyKeyOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        _provider = provider;
        _options = options ?? new IdempotencyKeyOptions();
    }

    /// <summary>
    /// Executes an operation exactly once per idempotency key.
    /// Returns cached result on retries with the same key.
    /// </summary>
    /// <param name="idempotencyKey">Unique operation identifier (e.g., UUID)</param>
    /// <param name="operation">Async operation that produces a result</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Result wrapper indicating if result was cached or freshly computed</returns>
    public async Task<IdempotencyKeyResult> GetOrExecuteAsync(
        string idempotencyKey,
        Func<Task<string>> operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNullOrEmpty(idempotencyKey);
        ArgumentNullException.ThrowIfNull(operation);

        // Check for cached result first (fast path for retries)
        var cached = await _provider.GetCachedResultAsync(idempotencyKey, cancellationToken);
        if (cached is not null)
        {
            return new IdempotencyKeyResult(isFromCache: true, result: cached);
        }

        // Try to claim this key for processing
        bool claimed = await _provider.TryClaimAsync(idempotencyKey, cancellationToken);

        if (!claimed)
        {
            // Another client is processing or just finished processing this key
            // Wait for result to be cached
            var waitResult = await WaitForResultAsync(idempotencyKey, cancellationToken);
            if (waitResult is not null)
            {
                return new IdempotencyKeyResult(isFromCache: true, result: waitResult);
            }

            // Timeout waiting for result
            throw new TimeoutException(
                $"Timeout waiting for idempotency key '{idempotencyKey}' to be processed");
        }

        // We have the claim - execute the operation
        try
        {
            var result = await operation();

            // Validate result size
            if (result is not null)
            {
                var resultSize = System.Text.Encoding.UTF8.GetByteCount(result);
                if (resultSize > _options.MaxResultSizeBytes)
                {
                    throw new InvalidOperationException(
                        $"Operation result exceeds maximum size: {resultSize} > {_options.MaxResultSizeBytes}");
                }
            }

            // Store result for future retries
            await _provider.StoreCachedResultAsync(
                idempotencyKey,
                result ?? string.Empty,
                _options.ResultTtl,
                cancellationToken);

            return new IdempotencyKeyResult(isFromCache: false, result: result);
        }
        catch (Exception ex)
        {
            // Don't cache errors - allow retry with same key
            // In a real system, you might want to cache error results separately
            throw;
        }
    }

    /// <summary>
    /// Gets a cached result without executing any operation.
    /// Returns null if not found or expired.
    /// </summary>
    /// <param name="idempotencyKey">Key to look up</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Cached result or null if not found</returns>
    public async Task<string?> GetCachedResultAsync(
        string idempotencyKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNullOrEmpty(idempotencyKey);
        return await _provider.GetCachedResultAsync(idempotencyKey, cancellationToken);
    }

    /// <summary>
    /// Manually stores a result against an idempotency key.
    /// Useful for pre-caching or external result sources.
    /// </summary>
    /// <param name="idempotencyKey">Key for this operation</param>
    /// <param name="result">Serialized result</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if stored, false if key already existed</returns>
    public async Task<bool> StoreCachedResultAsync(
        string idempotencyKey,
        string result,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNullOrEmpty(idempotencyKey);
        ArgumentNullException.ThrowIfNull(result);

        var resultSize = System.Text.Encoding.UTF8.GetByteCount(result);
        if (resultSize > _options.MaxResultSizeBytes)
        {
            throw new InvalidOperationException(
                $"Result exceeds maximum size: {resultSize} > {_options.MaxResultSizeBytes}");
        }

        return await _provider.StoreCachedResultAsync(
            idempotencyKey,
            result,
            _options.ResultTtl,
            cancellationToken);
    }

    /// <summary>
    /// Deletes an idempotency key (cleanup).
    /// </summary>
    /// <param name="idempotencyKey">Key to delete</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if deleted, false if not found</returns>
    public async Task<bool> DeleteAsync(
        string idempotencyKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNullOrEmpty(idempotencyKey);
        return await _provider.DeleteAsync(idempotencyKey, cancellationToken);
    }

    /// <summary>
    /// Waits for a result to be cached (when another client is processing the same key).
    /// </summary>
    private async Task<string?> WaitForResultAsync(
        string idempotencyKey,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.Add(_options.ClaimTimeout);

        while (DateTime.UtcNow < deadline)
        {
            await Task.Delay(_options.RetryDelay, cancellationToken);

            var result = await _provider.GetCachedResultAsync(idempotencyKey, cancellationToken);
            if (result is not null)
            {
                return result;
            }
        }

        return null; // Timeout
    }
}