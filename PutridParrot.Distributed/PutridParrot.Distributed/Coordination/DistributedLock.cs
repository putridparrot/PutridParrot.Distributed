namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Distributed lock implementation that works with any cache provider implementing IDistributedCacheProvider.
/// Provides thread-safe locking across multiple processes or servers.
/// </summary>
public class DistributedLock : IAsyncDisposable
{
    private readonly IDistributedCacheProvider _cacheProvider;
    private readonly string _lockKey;
    private readonly string _lockValue;
    private readonly DistributedLockOptions _options;
    private readonly CancellationTokenSource _extensionCts;
    private Task? _extensionTask;
    private bool _isAcquired;

    /// <summary>
    /// Gets whether the lock is currently acquired.
    /// </summary>
    public bool IsAcquired => _isAcquired;

    /// <summary>
    /// Gets the lock key.
    /// </summary>
    public string LockKey => _lockKey;

    /// <summary>
    /// Creates a new distributed lock instance.
    /// </summary>
    /// <param name="cacheProvider">The cache provider to use for locking operations</param>
    /// <param name="lockKey">The unique key for this lock</param>
    /// <param name="options">Optional configuration options</param>
    public DistributedLock(
        IDistributedCacheProvider cacheProvider,
        string lockKey,
        DistributedLockOptions? options = null)
    {
        _cacheProvider = cacheProvider ?? throw new ArgumentNullException(nameof(cacheProvider));
        _lockKey = lockKey ?? throw new ArgumentNullException(nameof(lockKey));
        _lockValue = Guid.NewGuid().ToString("N");
        _options = options ?? new DistributedLockOptions();
        _extensionCts = new CancellationTokenSource();
    }

    /// <summary>
    /// Attempts to acquire the distributed lock.
    /// </summary>
    /// <param name="expiry">Optional lock expiration time. If not specified, uses DefaultExpiry from options.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if the lock was acquired, false otherwise</returns>
    public async Task<bool> TryAcquireAsync(TimeSpan? expiry = null, CancellationToken cancellationToken = default)
    {
        if (_isAcquired)
        {
            throw new InvalidOperationException("Lock is already acquired.");
        }

        var lockExpiry = expiry ?? _options.DefaultExpiry;
        var acquireTimeout = _options.AcquireTimeout;

        if (acquireTimeout.HasValue)
        {
            return await TryAcquireWithRetryAsync(lockExpiry, acquireTimeout.Value, cancellationToken);
        }

        return await AcquireLockAsync(lockExpiry, cancellationToken);
    }

    /// <summary>
    /// Acquires the distributed lock, waiting until it becomes available or the timeout expires.
    /// </summary>
    /// <param name="expiry">Optional lock expiration time. If not specified, uses DefaultExpiry from options.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if the lock was acquired within the timeout period</returns>
    /// <exception cref="TimeoutException">Thrown if the lock could not be acquired within the timeout period (only when AcquireTimeout is set)</exception>
    public async Task AcquireAsync(TimeSpan? expiry = null, CancellationToken cancellationToken = default)
    {
        var acquired = await TryAcquireAsync(expiry, cancellationToken);

        if (!acquired && _options.AcquireTimeout.HasValue)
        {
            throw new TimeoutException($"Failed to acquire lock '{_lockKey}' within the timeout period of {_options.AcquireTimeout.Value}.");
        }

        if (!acquired)
        {
            throw new InvalidOperationException($"Failed to acquire lock '{_lockKey}'.");
        }
    }

    /// <summary>
    /// Releases the distributed lock.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if the lock was released, false if it was not held or already expired</returns>
    public async Task<bool> ReleaseAsync(CancellationToken cancellationToken = default)
    {
        if (!_isAcquired)
        {
            return false;
        }

        await StopAutoExtensionAsync();

        var released = await _cacheProvider.ReleaseLockAsync(_lockKey, _lockValue, cancellationToken);
        _isAcquired = false;

        return released;
    }

    /// <summary>
    /// Manually extends the lock expiration time.
    /// </summary>
    /// <param name="expiry">The new expiration time</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if the lock was extended, false otherwise</returns>
    public async Task<bool> ExtendAsync(TimeSpan expiry, CancellationToken cancellationToken = default)
    {
        if (!_isAcquired)
        {
            return false;
        }

        return await _cacheProvider.ExtendLockAsync(_lockKey, _lockValue, expiry, cancellationToken);
    }

    private async Task<bool> AcquireLockAsync(TimeSpan expiry, CancellationToken cancellationToken)
    {
        var acquired = await _cacheProvider.TryAcquireLockAsync(_lockKey, _lockValue, expiry, cancellationToken);

        if (acquired)
        {
            _isAcquired = true;
            StartAutoExtension(expiry);
        }

        return acquired;
    }

    private async Task<bool> TryAcquireWithRetryAsync(TimeSpan expiry, TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        while (!linkedCts.Token.IsCancellationRequested)
        {
            if (await AcquireLockAsync(expiry, linkedCts.Token))
            {
                return true;
            }

            try
            {
                await Task.Delay(_options.RetryDelay, linkedCts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        return false;
    }

    private void StartAutoExtension(TimeSpan expiry)
    {
        if (!_options.AutoExtendInterval.HasValue)
        {
            return;
        }

        _extensionTask = Task.Run(async () =>
        {
            while (!_extensionCts.Token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_options.AutoExtendInterval.Value, _extensionCts.Token);

                    if (!_extensionCts.Token.IsCancellationRequested)
                    {
                        var extended = await _cacheProvider.ExtendLockAsync(_lockKey, _lockValue, expiry, _extensionCts.Token);

                        if (!extended)
                        {
                            _isAcquired = false;
                            break;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }, _extensionCts.Token);
    }

    private async Task StopAutoExtensionAsync()
    {
        if (_extensionTask != null)
        {
            _extensionCts.Cancel();

            try
            {
                await _extensionTask;
            }
            catch (OperationCanceledException)
            {
                // Expected when cancelling
            }
        }
    }

    /// <summary>
    /// Disposes the distributed lock, releasing it if still held.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_isAcquired)
        {
            await ReleaseAsync();
        }

        _extensionCts.Dispose();

        GC.SuppressFinalize(this);
    }
}