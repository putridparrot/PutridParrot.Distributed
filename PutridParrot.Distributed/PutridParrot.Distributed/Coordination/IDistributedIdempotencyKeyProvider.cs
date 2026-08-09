namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Interface for distributed idempotency key providers.
/// 
/// An idempotency key ensures that an operation can be executed multiple times with the same key
/// without producing duplicate side effects. The first execution produces a result that is cached;
/// subsequent executions with the same key return the cached result immediately.
/// 
/// This pattern is essential for APIs, payment processing, and distributed systems where
/// exactly-once semantics are required despite retries or network failures.
/// </summary>
public interface IDistributedIdempotencyKeyProvider
{
    /// <summary>
    /// Checks if an idempotency key has been processed before and returns the cached result if so.
    /// </summary>
    /// <param name="idempotencyKey">Unique key for this operation (e.g., UUID, checksum)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The cached result if found, null if this is a new operation</returns>
    Task<string?> GetCachedResultAsync(string idempotencyKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Stores the result of an operation against an idempotency key.
    /// Subsequent calls to GetCachedResultAsync with the same key will return this result.
    /// </summary>
    /// <param name="idempotencyKey">Unique key for this operation</param>
    /// <param name="result">Serialized result (typically JSON)</param>
    /// <param name="ttl">Time-to-live for the cached result</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if stored successfully, false if key was already present</returns>
    Task<bool> StoreCachedResultAsync(
        string idempotencyKey,
        string result,
        TimeSpan? ttl = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to claim an idempotency key for processing.
    /// Only one caller should proceed if this returns true; others get the cached result.
    /// </summary>
    /// <param name="idempotencyKey">Unique key for this operation</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if this caller can proceed with processing, false if already processed</returns>
    Task<bool> TryClaimAsync(string idempotencyKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the count of operations processed with this key (should be 0 or 1 for proper idempotency).
    /// </summary>
    /// <param name="idempotencyKey">Unique key for this operation</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Number of times this key has been processed</returns>
    Task<int> GetProcessCountAsync(string idempotencyKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes an idempotency key and its cached result (cleanup).
    /// </summary>
    /// <param name="idempotencyKey">Unique key to delete</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task<bool> DeleteAsync(string idempotencyKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resets all idempotency keys (used for cleanup or testing).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    Task ResetAsync(CancellationToken cancellationToken = default);
}