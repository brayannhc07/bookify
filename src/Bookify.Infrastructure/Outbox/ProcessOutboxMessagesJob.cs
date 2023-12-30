using System.Data;
using Bookify.Application.Abstractions.Clock;
using Bookify.Application.Abstractions.Data;
using Bookify.Domain.Abstractions;
using Dapper;
using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Quartz;

namespace Bookify.Infrastructure.Outbox;

[DisallowConcurrentExecution]
internal sealed class ProcessOutboxMessagesJob : IJob {
    private readonly JsonSerializerSettings _jsonSerializerSettings = new() {
        TypeNameHandling = TypeNameHandling.All
    };

    private readonly ISqlConnectionFactory _sqlConnectionFactory;
    private readonly IPublisher _publisher;
    private readonly IDateTimeProvider _dateTimeProvider;
    private readonly OutboxOptions _outboxOptions;
    private readonly ILogger<ProcessOutboxMessagesJob> _logger;

    public ProcessOutboxMessagesJob(ISqlConnectionFactory sqlConnectionFactory, IPublisher publisher,
        IDateTimeProvider dateTimeProvider, IOptions<OutboxOptions> outboxOptions,
        ILogger<ProcessOutboxMessagesJob> logger) {
        _sqlConnectionFactory = sqlConnectionFactory;
        _publisher = publisher;
        _dateTimeProvider = dateTimeProvider;
        _outboxOptions = outboxOptions.Value;
        _logger = logger;
    }

    public async Task Execute(IJobExecutionContext context) {
        _logger.LogInformation("Beginning to process outbox messages");

        using var connection = _sqlConnectionFactory.CreateConnection();
        using var transaction = connection.BeginTransaction();

        var outboxMessages = await GetOutboxMessagesAsync(connection, transaction: transaction);

        foreach (var outboxMessage in outboxMessages) {
            Exception? exception = null;

            try {
                var domainEvent = JsonConvert.DeserializeObject<IDomainEvent>(
                    outboxMessage.Content,
                    _jsonSerializerSettings);
                await _publisher.Publish(domainEvent, context.CancellationToken);
            }
            catch (Exception e) {
                _logger.LogError(
                    e, "Exception while processing outbox message {MessageId}", outboxMessage.Id);
                exception = e;
            }

            await UpdateOutboxMessageAsync(connection, transaction, outboxMessage, exception);
        }
    }

    private async Task<IReadOnlyList<OutboxMessageResponse>> GetOutboxMessagesAsync(IDbConnection connection,
        IDbTransaction transaction) {
        var sql = $"""
                   SELECT id, content
                   FROM outbox_messages
                   WHERE processed_on_utc IS NULL
                   ORDER BY occurred_on_utc
                   LIMIT {_outboxOptions.BatchSize}
                   FOR UPDATE
                   """;
        var outboxMessages = await connection.QueryAsync<OutboxMessageResponse>(sql, transaction: transaction);

        return outboxMessages.ToList();
    }

    private async Task UpdateOutboxMessageAsync(IDbConnection connection, IDbTransaction transaction,
        OutboxMessageResponse outboxMessage, Exception? exception) {
        const string sql = """
                           UPDATE outbox_messages
                           SET processed_on_utc = @ProcessedOnUtc,
                               error = @Error
                           WHERE id = @Id
                           """;
        await connection.ExecuteAsync(sql, new {
            outboxMessage.Id,
            ProcessedOnUtc = _dateTimeProvider.UtcNow,
            Error = exception?.ToString()
        }, transaction: transaction);
    }

    internal sealed record OutboxMessageResponse(Guid Id, string Content);
}