using System;
using System.Collections.Generic;
using System.Text;

namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Main facade for distributed counter operations.
/// Provides atomic increment, decrement, and snapshot operations across distributed systems.
/// </summary>
public class DistributedCounter
{
    private readonly IDistributedCounterProvider _provider;
    private readonly CounterOptions _options;

    /// <summary>
    /// Gets the name of this counter.
    /// </summary>
    public string CounterName { get; }

    /// <summary>
    /// Initializes a new instance of the DistributedCounter class.
    /// </summary>
    /// <param name="counterName">Unique name for this counter.</param>
    /// <param name="provider">Backend provider for counter operations.</param>
    /// <param name="options">Counter options.</param>
    /// <exception cref="ArgumentNullException">Thrown when provider is null.</exception>
    public DistributedCounter(
        string counterName,
        IDistributedCounterProvider provider,
        CounterOptions? options = null)
    {
        CounterName = counterName ?? throw new ArgumentNullException(nameof(counterName));
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _options = options ?? new CounterOptions();
    }

    /// <summary>
    /// Increments the counter by one.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new counter value.</returns>
    public async Task<CounterResult> IncrementAsync(CancellationToken cancellationToken = default)
    {
        return await IncrementAsync(1, cancellationToken);
    }

    /// <summary>
    /// Increments the counter by a specified amount.
    /// </summary>
    /// <param name="amount">Amount to increment.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new counter value.</returns>
    public async Task<CounterResult> IncrementAsync(long amount, CancellationToken cancellationToken = default)
    {
        var newValue = await _provider.IncrementAsync(CounterName, amount, _options, cancellationToken);

        return new CounterResult
        {
            IsSuccessful = true,
            Value = newValue,
            AmountApplied = amount,
            Timestamp = DateTime.UtcNow,
            Message = $"Counter incremented by {amount}"
        };
    }

    /// <summary>
    /// Decrements the counter by one.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new counter value.</returns>
    public async Task<CounterResult> DecrementAsync(CancellationToken cancellationToken = default)
    {
        return await DecrementAsync(1, cancellationToken);
    }

    /// <summary>
    /// Decrements the counter by a specified amount.
    /// </summary>
    /// <param name="amount">Amount to decrement.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new counter value.</returns>
    public async Task<CounterResult> DecrementAsync(long amount, CancellationToken cancellationToken = default)
    {
        var newValue = await _provider.DecrementAsync(CounterName, amount, _options, cancellationToken);

        return new CounterResult
        {
            IsSuccessful = true,
            Value = newValue,
            AmountApplied = amount,
            Timestamp = DateTime.UtcNow,
            Message = $"Counter decremented by {amount}"
        };
    }

    /// <summary>
    /// Gets the current counter value.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The current counter value.</returns>
    public async Task<long> GetAsync(CancellationToken cancellationToken = default)
    {
        return await _provider.GetAsync(CounterName, _options, cancellationToken);
    }

    /// <summary>
    /// Gets the current counter value as a result object.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Counter result with value and metadata.</returns>
    public async Task<CounterResult> GetResultAsync(CancellationToken cancellationToken = default)
    {
        var value = await _provider.GetAsync(CounterName, _options, cancellationToken);

        return new CounterResult
        {
            IsSuccessful = true,
            Value = value,
            Timestamp = DateTime.UtcNow,
            Message = "Counter retrieved successfully"
        };
    }

    /// <summary>
    /// Sets the counter to a specific value.
    /// </summary>
    /// <param name="value">The value to set.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The set value.</returns>
    public async Task<CounterResult> SetAsync(long value, CancellationToken cancellationToken = default)
    {
        var actualValue = await _provider.SetAsync(CounterName, value, cancellationToken);

        return new CounterResult
        {
            IsSuccessful = true,
            Value = actualValue,
            AmountApplied = actualValue,
            Timestamp = DateTime.UtcNow,
            Message = $"Counter set to {actualValue}"
        };
    }

    /// <summary>
    /// Gets a snapshot of the current counter state.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Counter state snapshot.</returns>
    public async Task<CounterState> GetStateAsync(CancellationToken cancellationToken = default)
    {
        return await _provider.GetStateAsync(CounterName, cancellationToken);
    }

    /// <summary>
    /// Resets the counter to its initial value.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task ResetAsync(CancellationToken cancellationToken = default)
    {
        await _provider.ResetAsync(CounterName, _options, cancellationToken);
    }

    /// <summary>
    /// Atomically increments the counter if it's below the specified max value.
    /// </summary>
    /// <param name="maxValue">Maximum allowed value.</param>
    /// <param name="amount">Amount to increment (default: 1).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if incremented, false if at or above maxValue.</returns>
    public async Task<bool> IncrementIfBelowAsync(
        long maxValue,
        long amount = 1,
        CancellationToken cancellationToken = default)
    {
        return await _provider.IncrementIfBelowAsync(CounterName, maxValue, amount, cancellationToken);
    }

    /// <summary>
    /// Atomically increments and returns the result if below max, otherwise returns false with current value.
    /// </summary>
    /// <param name="maxValue">Maximum allowed value.</param>
    /// <param name="amount">Amount to increment (default: 1).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Result with success flag and current value.</returns>
    public async Task<CounterResult> IncrementIfBelowResultAsync(
        long maxValue,
        long amount = 1,
        CancellationToken cancellationToken = default)
    {
        var success = await _provider.IncrementIfBelowAsync(CounterName, maxValue, amount, cancellationToken);
        var currentValue = await GetAsync(cancellationToken);

        return new CounterResult
        {
            IsSuccessful = success,
            Value = currentValue,
            AmountApplied = success ? amount : 0,
            Timestamp = DateTime.UtcNow,
            Message = success
                ? $"Counter incremented by {amount} (now {currentValue})"
                : $"Counter already at or above max ({currentValue} >= {maxValue})"
        };
    }

    /// <summary>
    /// Performs multiple atomic operations in sequence (useful for batch updates).
    /// </summary>
    /// <param name="operations">Operations to perform (positive = increment, negative = decrement).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Final counter value after all operations.</returns>
    public async Task<long> ApplyBatchAsync(
        IEnumerable<long> operations,
        CancellationToken cancellationToken = default)
    {
        var result = 0L;
        foreach (var op in operations)
        {
            if (op > 0)
            {
                var incrementResult = await IncrementAsync(op, cancellationToken);
                result = incrementResult.Value;
            }
            else if (op < 0)
            {
                var decrementResult = await DecrementAsync(-op, cancellationToken);
                result = decrementResult.Value;
            }
        }

        return result;
    }

    /// <summary>
    /// Gets the counter value as a percentage of the max value.
    /// </summary>
    /// <param name="maxValue">The max value to use as denominator.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Percentage (0-100).</returns>
    public async Task<double> GetPercentageAsync(long maxValue, CancellationToken cancellationToken = default)
    {
        if (maxValue <= 0)
            return 0;

        var currentValue = await GetAsync(cancellationToken);
        return (currentValue / (double)maxValue) * 100;
    }
}

/// <summary>
/// Represents the state of a distributed counter.
/// </summary>
public class CounterState
{
    /// <summary>
    /// Gets or sets the counter name.
    /// </summary>
    public string? CounterName { get; set; }

    /// <summary>
    /// Gets or sets the current counter value.
    /// </summary>
    public long CurrentValue { get; set; }

    /// <summary>
    /// Gets or sets the time this state was captured.
    /// </summary>
    public DateTime Timestamp { get; set; }

    /// <summary>
    /// Gets or sets the number of shards (if using sharded counter).
    /// </summary>
    public int ShardCount { get; set; } = 1;
}

/// <summary>
/// Provider contract for distributed counter operations.
/// </summary>
public interface IDistributedCounterProvider
{
    /// <summary>
    /// Initializes the counter provider (creates necessary backend structures).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task EnsureInitializedAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Increments the counter by a specified amount.
    /// </summary>
    /// <param name="counterName">Name of the counter.</param>
    /// <param name="amount">Amount to increment (default: 1).</param>
    /// <param name="options">Counter options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new counter value.</returns>
    Task<long> IncrementAsync(
        string counterName,
        long amount = 1,
        CounterOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Decrements the counter by a specified amount.
    /// </summary>
    /// <param name="counterName">Name of the counter.</param>
    /// <param name="amount">Amount to decrement (default: 1).</param>
    /// <param name="options">Counter options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new counter value.</returns>
    Task<long> DecrementAsync(
        string counterName,
        long amount = 1,
        CounterOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the current counter value.
    /// </summary>
    /// <param name="counterName">Name of the counter.</param>
    /// <param name="options">Counter options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The current counter value.</returns>
    Task<long> GetAsync(
        string counterName,
        CounterOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the counter to a specific value.
    /// </summary>
    /// <param name="counterName">Name of the counter.</param>
    /// <param name="value">The value to set.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The set value.</returns>
    Task<long> SetAsync(
        string counterName,
        long value,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the current counter state.
    /// </summary>
    /// <param name="counterName">Name of the counter.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Counter state snapshot.</returns>
    Task<CounterState> GetStateAsync(
        string counterName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Resets the counter to its initial value.
    /// </summary>
    /// <param name="counterName">Name of the counter.</param>
    /// <param name="options">Counter options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ResetAsync(
        string counterName,
        CounterOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically increments if the counter is below the specified max value.
    /// </summary>
    /// <param name="counterName">Name of the counter.</param>
    /// <param name="maxValue">Maximum allowed value.</param>
    /// <param name="amount">Amount to increment.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if incremented, false if at or above maxValue.</returns>
    Task<bool> IncrementIfBelowAsync(
        string counterName,
        long maxValue,
        long amount = 1,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Options for distributed counter operations.
/// </summary>
public class CounterOptions
{
    /// <summary>
    /// Gets or sets the initial value for the counter.
    /// Default: 0.
    /// </summary>
    public long InitialValue { get; set; } = 0;

    /// <summary>
    /// Gets or sets the maximum allowed value for the counter.
    /// Set to long.MaxValue for no limit.
    /// Default: long.MaxValue.
    /// </summary>
    public long MaxValue { get; set; } = long.MaxValue;

    /// <summary>
    /// Gets or sets the number of shards for sharded counter strategy.
    /// Higher shard count reduces contention but increases memory usage.
    /// Default: 1 (non-sharded).
    /// </summary>
    public int ShardCount { get; set; } = 1;

    /// <summary>
    /// Gets or sets whether to clamp values at MaxValue instead of throwing.
    /// Default: true.
    /// </summary>
    public bool ClampAtMax { get; set; } = true;

    /// <summary>
    /// Gets or sets the time-to-live for the counter.
    /// Set to TimeSpan.Zero for no expiration.
    /// Default: zero (no expiration).
    /// </summary>
    public TimeSpan Ttl { get; set; } = TimeSpan.Zero;
}