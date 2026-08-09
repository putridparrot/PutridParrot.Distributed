using Npgsql;
using PutridParrot.Distributed.Coordination;

namespace PutridParrot.Distributed.Postgresql.Coordination;

/// <summary>
/// PostgreSQL-based distributed idempotency key provider.
/// 
/// Uses a persistent idempotency_keys table with JSONB result storage.
/// Provides durability and rich query capabilities via PostgreSQL's JSON support.
/// </summary>
public class PostgreSqlIdempotencyKeyProvider : IDistributedIdempotencyKeyProvider
{
    private readonly string _connectionString;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private volatile bool _initialized;

    public PostgreSqlIdempotencyKeyProvider(string connectionString)
    {
        ArgumentNullException.ThrowIfNullOrEmpty(connectionString);
        _connectionString = connectionString;
    }

    /// <summary>
    /// Ensures the idempotency_keys table exists.
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

            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);

            const string createTableSql = @"
                CREATE TABLE IF NOT EXISTS idempotency_keys (
                    idempotency_key TEXT PRIMARY KEY,
                    result TEXT NOT NULL,
                    process_count INT NOT NULL DEFAULT 1,
                    claimed_at TIMESTAMP NOT NULL,
                    processed_at TIMESTAMP NOT NULL,
                    expires_at TIMESTAMP NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_idempotency_keys_expires_at 
                    ON idempotency_keys (expires_at);";

            await using var command = new NpgsqlCommand(createTableSql, connection);
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

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // Prune expired entries
        const string pruneSql = "DELETE FROM idempotency_keys WHERE expires_at < CURRENT_TIMESTAMP";
        using var pruneCommand = new NpgsqlCommand(pruneSql, connection);
        await pruneCommand.ExecuteNonQueryAsync(cancellationToken);

        // Get result
        const string selectSql = @"
            SELECT result FROM idempotency_keys 
            WHERE idempotency_key = $1 AND expires_at > CURRENT_TIMESTAMP";

        using var command = new NpgsqlCommand(selectSql, connection);
        command.Parameters.AddWithValue(idempotencyKey);
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

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        const string upsertSql = @"
            INSERT INTO idempotency_keys 
                (idempotency_key, result, claimed_at, processed_at, expires_at)
            VALUES 
                ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $3)
            ON CONFLICT (idempotency_key) DO UPDATE
            SET result = EXCLUDED.result,
                process_count = process_count + 1,
                processed_at = CURRENT_TIMESTAMP,
                expires_at = $3
            RETURNING (xmax = 0) AS is_insert;";

        using var command = new NpgsqlCommand(upsertSql, connection);
        command.Parameters.AddWithValue(idempotencyKey);
        command.Parameters.AddWithValue(result);
        command.Parameters.AddWithValue(expiresAt);

        var result_val = await command.ExecuteScalarAsync(cancellationToken);
        return result_val is not null && Convert.ToBoolean(result_val);
    }

    /// <summary>
    /// Tries to claim an idempotency key for exclusive processing.
    /// Uses a unique constraint and INSERT ... ON CONFLICT to achieve atomicity.
    /// </summary>
    public async Task<bool> TryClaimAsync(string idempotencyKey, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        try
        {
            const string tryClaimSql = @"
                INSERT INTO idempotency_keys 
                    (idempotency_key, result, claimed_at, processed_at, expires_at)
                VALUES 
                    ($1, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + interval '1 hour')
                ON CONFLICT DO NOTHING
                RETURNING 1 AS claimed;";

            using var command = new NpgsqlCommand(tryClaimSql, connection);
            command.Parameters.AddWithValue(idempotencyKey);

            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result is not null && Convert.ToInt32(result) == 1;
        }
        catch (PostgresException)
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

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = @"
            SELECT process_count FROM idempotency_keys WHERE idempotency_key = $1";

        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(idempotencyKey);
        var result = await command.ExecuteScalarAsync(cancellationToken);

        return result is not DBNull && result is not null ? Convert.ToInt32(result) : 0;
    }

    /// <summary>
    /// Deletes an idempotency key entry.
    /// </summary>
    public async Task<bool> DeleteAsync(string idempotencyKey, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        const string deleteSql = @"
            DELETE FROM idempotency_keys WHERE idempotency_key = $1";

        using var command = new NpgsqlCommand(deleteSql, connection);
        command.Parameters.AddWithValue(idempotencyKey);
        var rowsAffected = await command.ExecuteNonQueryAsync(cancellationToken);

        return rowsAffected > 0;
    }

    /// <summary>
    /// Clears all idempotency keys.
    /// </summary>
    public async Task ResetAsync(CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken);

        using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        const string deleteSql = "DELETE FROM idempotency_keys";
        using var command = new NpgsqlCommand(deleteSql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}

