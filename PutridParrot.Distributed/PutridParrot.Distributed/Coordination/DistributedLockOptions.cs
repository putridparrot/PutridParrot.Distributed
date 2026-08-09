namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Configuration options for the distributed lock.
/// </summary>
public class DistributedLockOptions
{
    /// <summary>
    /// The default lock expiration time if not specified.
    /// </summary>
    public TimeSpan DefaultExpiry { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The interval at which to automatically extend the lock while it's held.
    /// Set to null to disable automatic extension.
    /// </summary>
    public TimeSpan? AutoExtendInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// The maximum amount of time to wait when trying to acquire a lock.
    /// Set to null for no timeout (single attempt only).
    /// </summary>
    public TimeSpan? AcquireTimeout { get; set; }

    /// <summary>
    /// The delay between retry attempts when waiting to acquire a lock.
    /// Only used if AcquireTimeout is set.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);
}