namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Factory for creating distributed lock instances.
/// </summary>
public class DistributedLockFactory
{
    private readonly IDistributedCacheProvider _cacheProvider;
    private readonly DistributedLockOptions _defaultOptions;

    /// <summary>
    /// Creates a new distributed lock factory.
    /// </summary>
    /// <param name="cacheProvider">The cache provider to use for all locks created by this factory</param>
    /// <param name="defaultOptions">Optional default options for all locks</param>
    public DistributedLockFactory(
        IDistributedCacheProvider cacheProvider,
        DistributedLockOptions? defaultOptions = null)
    {
        _cacheProvider = cacheProvider ?? throw new ArgumentNullException(nameof(cacheProvider));
        _defaultOptions = defaultOptions ?? new DistributedLockOptions();
    }

    /// <summary>
    /// Creates a new distributed lock.
    /// </summary>
    /// <param name="lockKey">The unique key for the lock</param>
    /// <param name="options">Optional options specific to this lock (overrides default options)</param>
    /// <returns>A new distributed lock instance</returns>
    public DistributedLock CreateLock(string lockKey, DistributedLockOptions? options = null)
    {
        return new DistributedLock(_cacheProvider, lockKey, options ?? _defaultOptions);
    }
}

