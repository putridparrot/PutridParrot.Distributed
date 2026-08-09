namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Interface for distributed semaphore providers.
/// Implementations manage concurrent access to a limited number of resources across multiple processes/servers.
/// </summary>
public interface IDistributedSemaphoreProvider
{
    /// <summary>
    /// Attempts to acquire one or more permits from the semaphore.
    /// </summary>
    /// <param name="key">Unique semaphore key</param>
    /// <param name="permitsRequested">Number of permits to acquire (default 1)</param>
    /// <param name="maxPermits">Total permits available in the semaphore</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Number of permits acquired (0 if unsuccessful)</returns>
    Task<long> TryAcquirePermitsAsync(
        string key,
        long permitsRequested,
        long maxPermits,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases one or more permits back to the semaphore.
    /// </summary>
    /// <param name="key">Unique semaphore key</param>
    /// <param name="permitsToRelease">Number of permits to release</param>
    /// <param name="maxPermits">Total permits available in the semaphore</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if release was successful</returns>
    Task<bool> ReleasePermitsAsync(
        string key,
        long permitsToRelease,
        long maxPermits,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the number of available permits without acquiring them.
    /// </summary>
    /// <param name="key">Unique semaphore key</param>
    /// <param name="maxPermits">Total permits available in the semaphore</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Number of available permits</returns>
    Task<long> GetAvailablePermitsAsync(
        string key,
        long maxPermits,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Resets the semaphore to full capacity.
    /// </summary>
    /// <param name="key">Unique semaphore key</param>
    /// <param name="maxPermits">Total permits to reset to</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if reset was successful</returns>
    Task<bool> ResetAsync(
        string key,
        long maxPermits,
        CancellationToken cancellationToken = default);
}