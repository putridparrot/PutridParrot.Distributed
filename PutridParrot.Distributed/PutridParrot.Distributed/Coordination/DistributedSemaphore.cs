namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Distributed semaphore for controlling concurrent access to a limited number of resources.
/// A semaphore maintains a count of available "permits" that processes can acquire and release.
/// </summary>
public class DistributedSemaphore
{
    private readonly IDistributedSemaphoreProvider _provider;
    private readonly string _key;
    private readonly DistributedSemaphoreOptions _options;

    /// <summary>
    /// Gets the semaphore key.
    /// </summary>
    public string SemaphoreKey => _key;

    /// <summary>
    /// Gets whether the semaphore was successfully initialized.
    /// </summary>
    public bool IsInitialized { get; private set; }

    /// <summary>
    /// Creates a new distributed semaphore.
    /// </summary>
    /// <param name="provider">The semaphore provider implementation</param>
    /// <param name="key">Unique key for this semaphore</param>
    /// <param name="options">Configuration options (optional)</param>
    public DistributedSemaphore(
        IDistributedSemaphoreProvider provider,
        string key,
        DistributedSemaphoreOptions? options = null)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _key = key ?? throw new ArgumentNullException(nameof(key));
        _options = options ?? new DistributedSemaphoreOptions();
    }

    /// <summary>
    /// Attempts to acquire one or more permits from the semaphore.
    /// Non-blocking if no timeout is configured.
    /// </summary>
    /// <param name="permitsRequested">Number of permits to acquire (default 1)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if permits were acquired, false otherwise</returns>
    public async Task<bool> TryAcquireAsync(long permitsRequested = 1, CancellationToken cancellationToken = default)
    {
        if (permitsRequested <= 0)
        {
            throw new ArgumentException("Permits requested must be greater than 0", nameof(permitsRequested));
        }

        if (permitsRequested > _options.MaxPermits)
        {
            throw new ArgumentException("Cannot request more permits than the semaphore has", nameof(permitsRequested));
        }

        if (_options.AcquireTimeout == null)
        {
            // Non-blocking attempt
            var acquired = await _provider.TryAcquirePermitsAsync(_key, permitsRequested, _options.MaxPermits, cancellationToken);
            return acquired > 0;
        }

        // Blocking with timeout
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        while (stopwatch.Elapsed < _options.AcquireTimeout)
        {
            var acquired = await _provider.TryAcquirePermitsAsync(_key, permitsRequested, _options.MaxPermits, cancellationToken);
            if (acquired > 0)
            {
                return true;
            }

            await Task.Delay(_options.RetryDelay, cancellationToken);
        }

        return false;
    }

    /// <summary>
    /// Acquires one or more permits, throwing TimeoutException if not acquired within timeout.
    /// Requires AcquireTimeout to be configured.
    /// </summary>
    /// <param name="permitsRequested">Number of permits to acquire (default 1)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Task representing the acquisition</returns>
    /// <exception cref="TimeoutException">Thrown if permits not acquired within timeout</exception>
    /// <exception cref="InvalidOperationException">Thrown if AcquireTimeout not configured</exception>
    public async Task AcquireAsync(long permitsRequested = 1, CancellationToken cancellationToken = default)
    {
        if (_options.AcquireTimeout == null)
        {
            throw new InvalidOperationException("AcquireTimeout must be set to use AcquireAsync");
        }

        if (!await TryAcquireAsync(permitsRequested, cancellationToken))
        {
            throw new TimeoutException(
                $"Failed to acquire {permitsRequested} permits from semaphore '{_key}' within {_options.AcquireTimeout.Value.TotalSeconds}s");
        }
    }

    /// <summary>
    /// Releases one or more permits back to the semaphore.
    /// </summary>
    /// <param name="permitsToRelease">Number of permits to release (default 1)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if release was successful</returns>
    public async Task<bool> ReleaseAsync(long permitsToRelease = 1, CancellationToken cancellationToken = default)
    {
        if (permitsToRelease <= 0)
        {
            throw new ArgumentException("Permits to release must be greater than 0", nameof(permitsToRelease));
        }

        return await _provider.ReleasePermitsAsync(_key, permitsToRelease, _options.MaxPermits, cancellationToken);
    }

    /// <summary>
    /// Gets the number of available permits without acquiring them.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Number of available permits</returns>
    public async Task<long> GetAvailablePermitsAsync(CancellationToken cancellationToken = default)
    {
        return await _provider.GetAvailablePermitsAsync(_key, _options.MaxPermits, cancellationToken);
    }

    /// <summary>
    /// Resets the semaphore to full capacity.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if reset was successful</returns>
    public async Task<bool> ResetAsync(CancellationToken cancellationToken = default)
    {
        IsInitialized = false;
        return await _provider.ResetAsync(_key, _options.MaxPermits, cancellationToken);
    }
}