using Microsoft.Data.SqlClient;
using PutridParrot.Distributed.Coordination;

namespace PutridParrot.Distributed.SqlServer.Coordination;

/// <summary>
/// SQL Server-based distributed idempotency key provider.
/// 
/// Uses a persistent IdempotencyKeys table to store results with automatic cleanup.
/// Provides durability and audit trail for processed operations.
/// </summary>
public class SqlServerIdempotencyKeyProvider : IDistributedIdempotencyKeyProvider
{
    private readonly string _connectionString;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private volatile bool _initialized;

    public SqlServerIdempotencyKeyProvider(string connectionString)
    {
        ArgumentNullException.ThrowIfNullOrEmpty(connectionString);
        _connectionString = connectionString;
    }

    /// <summary>
    /// Ensures the IdempotencyKeys table exists.
    /// </summary>
    private async Task EnsureInitializedAsync(CancellationToken cancellationToken = default)
    {
        if (_initialized)
            return;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_initialized)
                return;

            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string createTableSql = @"
                IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'IdempotencyKeys')
                BEGIN
                    CREATE TABLE IdempotencyKeys (
                        IdempotencyKey NVARCHAR(256) PRIMARY KEY,
                        Result NVARCHAR(MAX) NOT NULL,
                        ProcessCount INT NOT NULL DEFAULT 1,
                        ClaimedAt DATETIME2 NOT NULL,
                        ProcessedAt DATETIME2 NOT NULL,
                        ExpiresAt DATETIME2 NOT NULL,
                        INDEX idx_expires ON IdempotencyKeys(ExpiresAt)
                    )
                END";

            await using var command = new SqlCommand(createTableSql, connection);
            await command.ExecuteNonQueryAsync(cancellationToken);

            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    /// <summary>
    /// Gets a cached result and prunes expired entries.
    /// </summary>
    public async Task<string?> GetCachedResultAsync(string idempotencyKey, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // Prune expired entries while we're here
        const string pruneSql = "DELETE FROM IdempotencyKeys WHERE ExpiresAt < GETUTCDATE()";
        using var pruneCommand = new SqlCommand(pruneSql, connection);
        await pruneCommand.ExecuteNonQueryAsync(cancellationToken);

        // Get result
        const string selectSql = @"
            SELECT Result FROM IdempotencyKeys 
            WHERE IdempotencyKey = @key AND ExpiresAt > GETUTCDATE()";

        using var command = new SqlCommand(selectSql, connection);
        command.Parameters.AddWithValue("@key", idempotencyKey);
        var result = await command.ExecuteScalarAsync(cancellationToken);

        return result is not DBNull && result is not null ? result.ToString() : null;
    }

    /// <summary>
    /// Stores a result in the database with TTL.
    /// </summary>
    public async Task<bool> StoreCachedResultAsync(
        string idempotencyKey,
        string result,
        TimeSpan? ttl = null,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        var expiresAt = DateTime.UtcNow.Add(ttl ?? TimeSpan.FromHours(1));

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        const string upsertSql = @"
            IF EXISTS (SELECT 1 FROM IdempotencyKeys WHERE IdempotencyKey = @key)
            BEGIN
                UPDATE IdempotencyKeys 
                SET Result = @result,
                    ProcessCount = ProcessCount + 1,
                    ProcessedAt = GETUTCDATE(),
                    ExpiresAt = @expiresAt
                WHERE IdempotencyKey = @key
                RETURN 0
            END
            ELSE
            BEGIN
                INSERT INTO IdempotencyKeys 
                    (IdempotencyKey, Result, ClaimedAt, ProcessedAt, ExpiresAt)
                VALUES 
                    (@key, @result, GETUTCDATE(), GETUTCDATE(), @expiresAt)
                RETURN 1
            END";

        using var command = new SqlCommand(upsertSql, connection);
        command.Parameters.AddWithValue("@key", idempotencyKey);
        command.Parameters.AddWithValue("@result", result);
        command.Parameters.AddWithValue("@expiresAt", expiresAt);

        var result_val = await command.ExecuteScalarAsync(cancellationToken);
        return result_val is not null && Convert.ToInt32(result_val) == 1;
    }

    /// <summary>
    /// Tries to claim an idempotency key for exclusive processing.
    /// </summary>
    public async Task<bool> TryClaimAsync(string idempotencyKey, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        const string tryClaimSql = @"
            BEGIN TRY
                BEGIN TRANSACTION
                IF EXISTS (SELECT 1 FROM IdempotencyKeys WHERE IdempotencyKey = @key)
                BEGIN
                    ROLLBACK TRANSACTION
                    RETURN 0
                END
                ELSE
                BEGIN
                    -- Reserve slot (will be updated when result is stored)
                    INSERT INTO IdempotencyKeys 
                        (IdempotencyKey, Result, ClaimedAt, ProcessedAt, ExpiresAt)
                    VALUES 
                        (@key, '', GETUTCDATE(), GETUTCDATE(), DATEADD(HOUR, 1, GETUTCDATE()))
                    COMMIT TRANSACTION
                    RETURN 1
                END
            END TRY
            BEGIN CATCH
                ROLLBACK TRANSACTION
                RETURN 0
            END CATCH";

        using var command = new SqlCommand(tryClaimSql, connection);
        command.Parameters.AddWithValue("@key", idempotencyKey);

        try
        {
            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result is not null && Convert.ToInt32(result) == 1;
        }
        catch (SqlException)
        {
            // Key already exists or constraint violated
            return false;
        }
    }

    /// <summary>
    /// Gets the number of times this key has been processed.
    /// </summary>
    public async Task<int> GetProcessCountAsync(string idempotencyKey, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = @"
            SELECT ProcessCount FROM IdempotencyKeys WHERE IdempotencyKey = @key";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@key", idempotencyKey);
        var result = await command.ExecuteScalarAsync(cancellationToken);

        return result is not DBNull && result is not null ? Convert.ToInt32(result) : 0;
    }

    /// <summary>
    /// Deletes an idempotency key entry.
    /// </summary>
    public async Task<bool> DeleteAsync(string idempotencyKey, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        const string deleteSql = @"
            DELETE FROM IdempotencyKeys WHERE IdempotencyKey = @key";

        using var command = new SqlCommand(deleteSql, connection);
        command.Parameters.AddWithValue("@key", idempotencyKey);
        var rowsAffected = await command.ExecuteNonQueryAsync(cancellationToken);

        return rowsAffected > 0;
    }

    /// <summary>
    /// Clears all idempotency keys.
    /// </summary>
    public async Task ResetAsync(CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        const string truncateSql = "TRUNCATE TABLE IdempotencyKeys";
        using var command = new SqlCommand(truncateSql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
