using System.Reflection.Metadata;
using System.Text;
using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;
using Testcontainers.Redpanda;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace GlobalRestore.Test;

public class GlobalRestoreStreamTests : IAsyncLifetime
{
    private readonly StreamBuilder _builder = new();
    private readonly StreamBuilder _globalBuilder = new();
    private KafkaStream? _client;
    private KafkaStream? _globalClient;
    private IProducer<string, string> _producer = default!;

    private RedpandaContainer? _redpanda;

    private readonly string APPLICATION_ID = "global-restore-test";
    private readonly string STATE_STORE_NAME = "state";
    private readonly string INPUT_TOPIC = "events";
    private string OUTPUT_TOPIC => $"{APPLICATION_ID}-{STATE_STORE_NAME}-changelog";

    [Fact]
    public async Task TestAtLeastOnceProcessing()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>()
        {
            ApplicationId = APPLICATION_ID,
            BootstrapServers = _redpanda?.GetBootstrapAddress(),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = true,
            Guarantee = ProcessingGuarantee.AT_LEAST_ONCE,
        };

        _builder.Stream<string, string>(INPUT_TOPIC)
            .Peek((k, v, c) => Console.WriteLine($"Event => Key: {k}, Value: {v}"))
            .GroupByKey()
            .Aggregate(() => "", (k, v, r) => v, InMemory.As<string, string>(STATE_STORE_NAME))
            .ToStream()
            .Peek((k, v, c) => Console.WriteLine($"State => Key: {k}, Value: {v}"));

        _client = new KafkaStream(_builder.Build(), config);
        await _client.StartAsync();

        ProduceMessage(INPUT_TOPIC, "key1", "value1");

        await Task.Delay(3000);

        _globalBuilder.GlobalTable(OUTPUT_TOPIC, InMemory.As<string, string>(STATE_STORE_NAME));
        _globalClient = new KafkaStream(_globalBuilder.Build(), config);
        await _globalClient.StartAsync();

        await Task.Delay(3000);

        var store = _globalClient.Store(
            StoreQueryParameters.FromNameAndType(
                STATE_STORE_NAME,
                QueryableStoreTypes.KeyValueStore<string, string>()
            )
        );

        var items = store.All();

        Assert.Single(items);
    }

    // 15s seems to be enough to demonstrate. Adjust as necessary.
    [Fact(Timeout = 15000)]
    public async Task TestExactlyOnceProcessing()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>()
        {
            ApplicationId = APPLICATION_ID,
            BootstrapServers = _redpanda?.GetBootstrapAddress(),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = true,
            Guarantee = ProcessingGuarantee.EXACTLY_ONCE,
        };

        _builder.Stream<string, string>(INPUT_TOPIC)
            .Peek((k, v, c) => Console.WriteLine($"Event => Key: {k}, Value: {v}"))
            .GroupByKey()
            .Aggregate(() => "", (k, v, r) => v, InMemory.As<string, string>(STATE_STORE_NAME))
            .ToStream()
            .Peek((k, v, c) => Console.WriteLine($"State => Key: {k}, Value: {v}"));

        _client = new KafkaStream(_builder.Build(), config);

        await _client.StartAsync();

        ProduceMessage(INPUT_TOPIC, "key1", "value1");

        await Task.Delay(3000);

        _globalBuilder.GlobalTable(OUTPUT_TOPIC, InMemory.As<string, string>(STATE_STORE_NAME));
        _globalClient = new KafkaStream(_globalBuilder.Build(), config);
        await _globalClient.StartAsync();

        await Task.Delay(3000);

        var store = _globalClient.Store(
            StoreQueryParameters.FromNameAndType(
                STATE_STORE_NAME,
                QueryableStoreTypes.KeyValueStore<string, string>()
            )
        );

        var items = store.All();

        Assert.Single(items);
    }

    private void ProduceMessage(string topic, string key, string value)
    {
        _producer.Produce(topic, new Message<string, string> { Key = key, Value = value }, result =>
        {
            Console.WriteLine($"Produce Result: {result.Status}");
        });
        _producer.Flush(TimeSpan.FromSeconds(10));
    }

    public async Task InitializeAsync()
    {
        _redpanda = new RedpandaBuilder()
            .WithImage("docker.redpanda.com/redpandadata/redpanda:latest")
            .Build();
        await _redpanda.StartAsync();

        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = _redpanda.GetBootstrapAddress(),
        }).Build();
    }

    public async Task DisposeAsync()
    {
        _producer.Dispose();
        _client?.Dispose();
        _globalClient?.Dispose();

        if (_redpanda != null)
        {
            await _redpanda.StopAsync();
        }
    }
}