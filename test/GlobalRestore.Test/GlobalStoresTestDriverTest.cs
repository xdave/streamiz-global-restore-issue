using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace GlobalRestore.Test;

public sealed class GlobalStoresTestDriverTest : IDisposable
{
    private readonly StreamBuilder _builder = new();
    private TopologyTestDriver? _client;
    private TestInputTopic<string, string>? _inputTopic;
    private TestOutputTopic<string, string>? _outputTopic;

    private const string APPLICATION_ID = "global-restore-test";
    private const string STATE_STORE_NAME = "state";
    private const string GLOBAL_STATE_STORE_NAME = "state-global";
    private const string INPUT_TOPIC = "events";
    private static string OUTPUT_TOPIC => $"{APPLICATION_ID}-{STATE_STORE_NAME}-changelog";

    private StreamConfig config => new StreamConfig<StringSerDes, StringSerDes>()
    {
        ApplicationId = APPLICATION_ID,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        AllowAutoCreateTopics = true,
        Guarantee = ProcessingGuarantee.EXACTLY_ONCE,
    };

    [Theory]
    [InlineData(TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, STATE_STORE_NAME)]
    [InlineData(TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, GLOBAL_STATE_STORE_NAME)]
    [InlineData(TopologyTestDriver.Mode.SYNC_TASK, STATE_STORE_NAME)]
    [InlineData(TopologyTestDriver.Mode.SYNC_TASK, GLOBAL_STATE_STORE_NAME)]
    public void TestGlobalStore(TopologyTestDriver.Mode mode, string assertStoreName)
    {
        _builder.Stream<string, string>(INPUT_TOPIC)
            .Peek((k, v, c) => Console.WriteLine($"Event => Key: {k}, Value: {v}"))
            .GroupByKey()
            .Aggregate(() => "", (k, v, r) => v, InMemory.As<string, string>(STATE_STORE_NAME))
            .ToStream()
            .Peek((k, v, c) => Console.WriteLine($"State => Key: {k}, Value: {v}"));

        _builder.GlobalTable(OUTPUT_TOPIC, InMemory.As<string, string>(GLOBAL_STATE_STORE_NAME));

        _client = new TopologyTestDriver(_builder.Build(), config, mode);
        _inputTopic = _client.CreateInputTopic<string, string>(INPUT_TOPIC);
        _outputTopic = _client.CreateOuputTopic<string, string>(OUTPUT_TOPIC);

        ProduceMessage(INPUT_TOPIC, "key1", "value1");
        ProduceMessage(INPUT_TOPIC, "key2", "value2");
        ProduceMessage(INPUT_TOPIC, "key3", "value3");

        var store = _client.GetKeyValueStore<string, string>(assertStoreName);
        var items = store.All();

        Assert.Equal(3, items.Count());
        Assert.Contains(items, item => item.Key == "key1" && item.Value == "value1");
        Assert.Contains(items, item => item.Key == "key2" && item.Value == "value2");
        Assert.Contains(items, item => item.Key == "key3" && item.Value == "value3");
    }

    private void ProduceMessage(string topic, string key, string value)
    {
        _inputTopic?.PipeInput(key, value);
    }

    public void Dispose()
    {
        _client?.Dispose();
    }
}