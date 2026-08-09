using Microsoft.Data.SqlClient;
using System.Data;
using PutridParrot.Distributed.Coordination;

namespace PutridParrot.Distributed.SqlServer.Coordination;

/// <summary>
/// SQL Server-backed implementation of distributed leader election.
/// Uses a table-based state with transaction-based coordination for strong consistency.
/// </summary>
public class SqlServerLeaderElectionProvider : IDistributedLeaderElectionProvider
{
    private readonly string _connectionString;
    private const string TableName = "LeaderElectionState";

    /// <summary>
    /// Initializes a new instance of the SqlServerLeaderElectionProvider.
    /// </summary>
    /// <param name="connectionString">SQL Server connection string.</param>
    public SqlServerLeaderElectionProvider(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    /// Ensures the leader election state table exists.
    /// </summary>
    private async Task EnsureInitializedAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var command = connection.CreateCommand();
        command.CommandText = $@"
            if not exists(select * from information_schema.tables where table_name = '{TableName}')
            begin
                create table {TableName} (
                    [Id] int primary key identity(1,1),
                    [LeaderKey] nvarchar(255) not null unique,
                    [LeaderId] nvarchar(255) null,
                    [ElectedAt] datetime2 null,
                    [RenewalDeadline] datetime2 null,
                    [RenewalCount] int default 0
                );
            end";

        await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Attempts to acquire leadership by inserting or updating a row.
    /// </summary>
    public async Task<bool> CandidateAsync(
        string leaderKey,
        string candidateId,
        LeaderElectionOptions options,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync();

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        try
        {
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandTimeout = (int)options.CandidacyTimeout.TotalSeconds;

            // Check if a leader already exists and is not expired
            command.CommandText = $@"
                select LeaderId, RenewalDeadline from {TableName}
                where LeaderKey = @leaderKey
                and RenewalDeadline > getutcdate()";

            command.Parameters.AddWithValue("@leaderKey", leaderKey);

            var existing = await command.ExecuteScalarAsync(cancellationToken);
            if (existing != null)
            {
                await transaction.RollbackAsync(cancellationToken);
                return false; // Another leader is already in place
            }

            // Try to insert or update with the new candidate
            command.CommandText = $@"
                merge {TableName} as target
                using (select @leaderKey as LeaderKey) as source
                on target.LeaderKey = source.LeaderKey
                when matched then
                    update set LeaderId = @candidateId,
                               ElectedAt = getutcdate(),
                               RenewalDeadline = dateadd(second, @ttlSeconds, getutcdate()),
                               RenewalCount = 0
                when not matched then
                    insert (LeaderKey, LeaderId, ElectedAt, RenewalDeadline, RenewalCount)
                    values (@leaderKey, @candidateId, getutcdate(), dateadd(second, @ttlSeconds, getutcdate()), 0)";

            command.Parameters.Clear();
            command.Parameters.AddWithValue("@leaderKey", leaderKey);
            command.Parameters.AddWithValue("@candidateId", candidateId);
            command.Parameters.AddWithValue("@ttlSeconds", options.StateTtl.TotalSeconds);

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
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        try
        {
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandTimeout = (int)options.CandidacyTimeout.TotalSeconds;

            // Update only if we are the current leader
            command.CommandText = $@"
                update {TableName}
                set RenewalDeadline = dateadd(second, @ttlSeconds, getutcdate()),
                    RenewalCount = RenewalCount + 1
                where LeaderKey = @leaderKey
                and LeaderId = @candidateId
                and RenewalDeadline > getutcdate()";

            command.Parameters.AddWithValue("@leaderKey", leaderKey);
            command.Parameters.AddWithValue("@candidateId", candidateId);
            command.Parameters.AddWithValue("@ttlSeconds", options.StateTtl.TotalSeconds);

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
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        using var command = connection.CreateCommand();
        command.CommandText = $@"
            update {TableName}
            set LeaderId = null,
                RenewalDeadline = null,
                RenewalCount = 0
            where LeaderKey = @leaderKey
            and LeaderId = @candidateId";

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

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        using var command = connection.CreateCommand();
        command.CommandText = $@"
            select top 1 LeaderId, ElectedAt, RenewalDeadline, RenewalCount
            from {TableName}
            where LeaderKey = @leaderKey";

        command.Parameters.AddWithValue("@leaderKey", leaderKey);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            var leaderId = reader["LeaderId"] as string;
            var electedAt = reader["ElectedAt"] as DateTime?;
            var renewalDeadline = reader["RenewalDeadline"] as DateTime?;
            var renewalCount = reader["RenewalCount"] as int? ?? 0;

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
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        using var command = connection.CreateCommand();
        command.CommandText = $"delete from {TableName} where LeaderKey = @leaderKey";
        command.Parameters.AddWithValue("@leaderKey", leaderKey);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
