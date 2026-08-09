using Npgsql;
using PutridParrot.Distributed.Coordination;

namespace PutridParrot.Distributed.Postgresql.Coordination;

/// <summary>
/// PostgreSQL-backed implementation of distributed leader election.
/// Uses a table-based state with FOR UPDATE locking and atomic operations for coordination.
/// </summary>
public class PostgreSqlLeaderElectionProvider : IDistributedLeaderElectionProvider
{
    private readonly string _connectionString;
    private const string TableName = "leader_election_state";

    /// <summary>
    /// Initializes a new instance of the PostgreSqlLeaderElectionProvider.
    /// </summary>
    /// <param name="connectionString">PostgreSQL connection string.</param>
    public PostgreSqlLeaderElectionProvider(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    /// Ensures the leader election state table exists.
    /// </summary>
    private async Task EnsureInitializedAsync()
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = $@"
            create table if not exists {TableName} (
                id serial primary key,
                leader_key varchar(255) not null unique,
                leader_id varchar(255),
                elected_at timestamp with time zone,
                renewal_deadline timestamp with time zone,
                renewal_count integer default 0
            );
            create index if not exists idx_leader_key on {TableName}(leader_key);";

        await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Attempts to acquire leadership by updating or inserting a row atomically.
    /// </summary>
    public async Task<bool> CandidateAsync(
        string leaderKey,
        string candidateId,
        LeaderElectionOptions options,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync();

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            await using var command = connection.CreateCommand();
            command.CommandTimeout = (int)options.CandidacyTimeout.TotalSeconds;

            // Check if a non-expired leader exists
            command.CommandText = $@"
                select leader_id
                from {TableName}
                where leader_key = @leaderKey
                and renewal_deadline > now() at time zone 'UTC'
                for update";

            command.Parameters.AddWithValue("@leaderKey", leaderKey);

            var existing = await command.ExecuteScalarAsync(cancellationToken);
            if (existing != null && existing != DBNull.Value)
            {
                await transaction.RollbackAsync(cancellationToken);
                return false; // Another leader is already in place
            }

            // Insert or update with the new candidate
            var ttlSeconds = (int)options.StateTtl.TotalSeconds;
            command.CommandText = $@"
                insert into {TableName} (leader_key, leader_id, elected_at, renewal_deadline, renewal_count)
                values (@leaderKey, @candidateId, now() at time zone 'UTC', (now() at time zone 'UTC' + interval '{ttlSeconds} seconds'), 0)
                on conflict (leader_key)
                do update set
                    leader_id = @candidateId,
                    elected_at = now() at time zone 'UTC',
                    renewal_deadline = (now() at time zone 'UTC' + interval '{ttlSeconds} seconds'),
                    renewal_count = 0";

            command.Parameters.Clear();
            command.Parameters.AddWithValue("@leaderKey", leaderKey);
            command.Parameters.AddWithValue("@candidateId", candidateId);

            await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            return true;
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    /// <summary>
    /// Renews leadership by updating the renewal deadline if the candidate still owns it.
    /// </summary>
    public async Task<bool> RenewAsync(
        string leaderKey,
        string candidateId,
        LeaderElectionOptions options,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            await using var command = connection.CreateCommand();
            command.CommandTimeout = (int)options.CandidacyTimeout.TotalSeconds;

            // Update only if we are the current leader
            var ttlSeconds = (int)options.StateTtl.TotalSeconds;
            command.CommandText = $@"
                update {TableName}
                set renewal_deadline = (now() at time zone 'UTC' + interval '{ttlSeconds} seconds'),
                    renewal_count = renewal_count + 1
                where leader_key = @leaderKey
                and leader_id = @candidateId
                and renewal_deadline > now() at time zone 'UTC'";

            command.Parameters.AddWithValue("@leaderKey", leaderKey);
            command.Parameters.AddWithValue("@candidateId", candidateId);

            var rows = await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            return rows > 0;
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    /// <summary>
    /// Yields leadership by clearing the leader ID.
    /// </summary>
    public async Task YieldAsync(
        string leaderKey,
        string candidateId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = $@"
            update {TableName}
            set leader_id = null,
                renewal_deadline = null,
                renewal_count = 0
            where leader_key = @leaderKey
            and leader_id = @candidateId";

        command.Parameters.AddWithValue("@leaderKey", leaderKey);
        command.Parameters.AddWithValue("@candidateId", candidateId);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Retrieves the current leader state.
    /// </summary>
    public async Task<LeaderElectionState> GetLeaderAsync(
        string leaderKey,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync();

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = $@"
            select leader_id, elected_at, renewal_deadline, renewal_count
            from {TableName}
            where leader_key = @leaderKey
            limit 1";

        command.Parameters.AddWithValue("@leaderKey", leaderKey);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            var leaderId = reader["leader_id"] as string;
            var electedAt = reader["elected_at"] as DateTime?;
            var renewalDeadline = reader["renewal_deadline"] as DateTime?;
            var renewalCount = (int?)reader["renewal_count"] ?? 0;

            return new LeaderElectionState
            {
                LeaderId = leaderId,
                LeaderKey = leaderKey,
                ElectedAt = electedAt,
                RenewalDeadline = renewalDeadline,
                RenewalCount = renewalCount
            };
        }

        return new LeaderElectionState
        {
            LeaderId = null,
            LeaderKey = leaderKey,
            ElectedAt = null,
            RenewalDeadline = null,
            RenewalCount = 0
        };
    }

    /// <summary>
    /// Resets the leader election state by deleting the row.
    /// </summary>
    public async Task ResetAsync(
        string leaderKey,
        CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = $"delete from {TableName} where leader_key = @leaderKey";
        command.Parameters.AddWithValue("@leaderKey", leaderKey);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
