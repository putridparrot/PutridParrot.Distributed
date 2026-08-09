namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Interface for cache providers that support distributed locking operations.
/// Implement this interface to integrate with Redis, ValKey, or other distributed cache systems.
/// </summary>
public interface IDistributedCacheProvider
{
    /// <summary>
    /// Attempts to acquire a lock by setting a key with a value if it doesn't exist.
    /// </summary>
    /// <param name="key">The lock key</param>
    /// <param name="value">The lock value (typically a unique identifier)</param>
    /// <param name="expiry">The lock expiration time</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if the lock was acquired, false otherwise</returns>
    Task<bool> TryAcquireLockAsync(string key, string value, TimeSpan expiry, CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases a lock by deleting the key only if it matches the expected value.
    /// </summary>
    /// <param name="key">The lock key</param>
    /// <param name="value">The expected lock value</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if the lock was released, false if the key didn't exist or value didn't match</returns>
    Task<bool> ReleaseLockAsync(string key, string value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Extends the expiration time of an existing lock if the value matches.
    /// </summary>
    /// <param name="key">The lock key</param>
    /// <param name="value">The expected lock value</param>
    /// <param name="expiry">The new expiration time</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if the lock expiration was extended, false otherwise</returns>
    Task<bool> ExtendLockAsync(string key, string value, TimeSpan expiry, CancellationToken cancellationToken = default);
}