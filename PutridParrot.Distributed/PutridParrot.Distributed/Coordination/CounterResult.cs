namespace PutridParrot.Distributed.Coordination;

/// <summary>
/// Result of a counter operation.
/// </summary>
public class CounterResult
{
    /// <summary>
    /// Gets a value indicating whether the operation was successful.
    /// </summary>
    public bool IsSuccessful { get; set; }

    /// <summary>
    /// Gets the resulting counter value after the operation.
    /// </summary>
    public long Value { get; set; }

    /// <summary>
    /// Gets the time when this result was generated.
    /// </summary>
    public DateTime Timestamp { get; set; }

    /// <summary>
    /// Gets an optional message describing the result.
    /// </summary>
    public string? Message { get; set; }

    /// <summary>
    /// Gets the amount that was actually applied (may differ if clamped).
    /// </summary>
    public long AmountApplied { get; set; }

    /// <summary>
    /// Returns a string representation of this result.
    /// </summary>
    public override string ToString()
    {
        return IsSuccessful ? 
            $"CounterResult {{ IsSuccessful=true, Value={Value} }}" : 
            $"CounterResult {{ IsSuccessful=false, Message={Message} }}";
    }
}