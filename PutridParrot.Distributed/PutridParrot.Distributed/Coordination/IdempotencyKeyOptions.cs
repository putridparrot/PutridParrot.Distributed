namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Configuration options for distributed idempotency keys.
/// </summary>
public class IdempotencyKeyOptions
{
    /// <summary>
    /// Time-to-live for cached results. After this duration, the idempotency key expires
    /// and the same key can be used for a new operation.
    /// Default: 1 hour
    /// </summary>
    public TimeSpan ResultTtl { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Maximum size (in bytes) for a cached result. Larger results will be rejected.
    /// Default: 1MB
    /// </summary>
    public int MaxResultSizeBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// Timeout for claiming an idempotency key. If another client is processing the same
    /// key, subsequent clients wait up to this timeout for the result.
    /// Default: 30 seconds
    /// </summary>
    public TimeSpan ClaimTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Delay between retry attempts when checking for a cached result.
    /// Default: 100ms
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);
}