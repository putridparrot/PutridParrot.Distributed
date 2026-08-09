namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Configuration options for distributed semaphores.
/// </summary>
public class DistributedSemaphoreOptions
{
    /// <summary>
    /// Maximum number of permits available in the semaphore.
    /// This is the total capacity.
    /// Default: 1 (behaves like a lock)
    /// </summary>
    public long MaxPermits { get; set; } = 1;

    /// <summary>
    /// Maximum time to wait for permit acquisition.
    /// Set to null for single non-blocking attempt.
    /// Default: null (non-blocking)
    /// </summary>
    public TimeSpan? AcquireTimeout { get; set; }

    /// <summary>
    /// Delay between retry attempts when acquiring permits.
    /// Only used if AcquireTimeout is set.
    /// Default: 100ms
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);
}