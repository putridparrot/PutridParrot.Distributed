namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Result wrapper for idempotency key operations.
/// Indicates whether the operation was newly executed or returned from cache.
/// </summary>
public class IdempotencyKeyResult
{
    /// <summary>
    /// True if this is a cached result from a previous execution (operation already run).
    /// False if this is a new operation (first time this key is being processed).
    /// </summary>
    public bool IsFromCache { get; set; }

    /// <summary>
    /// The operation result (typically JSON serialized).
    /// Will be null if the operation execution failed and no result was cached.
    /// </summary>
    public string? Result { get; set; }

    /// <summary>
    /// Optional error message if operation claimed but processing failed.
    /// </summary>
    public string? Error { get; set; }

    /// <summary>
    /// Timestamp when result was first cached.
    /// </summary>
    public DateTime? CachedAt { get; set; }

    public IdempotencyKeyResult()
    {
    }

    public IdempotencyKeyResult(bool isFromCache, string? result = null)
    {
        IsFromCache = isFromCache;
        Result = result;
    }

    public override string ToString()
    {
        var status = IsFromCache ? "cached" : "fresh";
        return $"[{status}] {(Result?.Length ?? 0)} bytes";
    }
}