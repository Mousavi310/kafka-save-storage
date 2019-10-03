using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using KafkaReduceMessageSize.Producer;

namespace KafkaReduceMessageSize.Consumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var broker = "localhost:9092";
            var schemaRegistryUrl = "http://localhost:8081/";

            //Consume_Json(broker, "json-simple");
            //Consume_Json(broker, "json-lingering");
            
            //Consume_Json(broker, "json-gzip");
            //Consume_Json(broker, "json-snappy");
            
            Consume_Avro(broker, schemaRegistryUrl, "avro-lingering");
            //Consume_Avro(broker, schemaRegistryUrl, "avro-gzip");
            //Consume_Avro(broker, schemaRegistryUrl, "avro-snappy");
                        
            Console.WriteLine("Hello World!");
        }

        private static void Consume_Avro(string broker, string schemaRegistryUrl, string topic)
        {
            var conf = new ConsumerConfig
            { 
                //Just for test use unique consumer groupd id.
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = broker,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using(var schemaRegistry = new CachedSchemaRegistryClient(
                new SchemaRegistryConfig {SchemaRegistryUrl = schemaRegistryUrl}
            ))
            {
                using (var c = new ConsumerBuilder<Ignore, OrderAvroModel>(conf)
                .SetValueDeserializer(new SyncOverAsyncDeserializer<OrderAvroModel>(new AvroDeserializer<OrderAvroModel>(schemaRegistry)))
                .Build())
                {
                    c.Subscribe(topic);

                    CancellationTokenSource cts = new CancellationTokenSource();
                    Console.CancelKeyPress += (_, e) => {
                        e.Cancel = true; // prevent the process from terminating.
                        cts.Cancel();
                    };

                    try
                    {
                        while (true)
                        {
                            try
                            {
                                var cr = c.Consume(cts.Token);
                                Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                            }
                            catch (ConsumeException e)
                            {
                                Console.WriteLine($"Error occured: {e.Error.Reason}");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Ensure the consumer leaves the group cleanly and final offsets are committed.
                        c.Close();
                    }
                }
            }
        }

        private static void Consume_Json(string broker, string topic)
        {
            var conf = new ConsumerConfig
            { 
                //Just for test use unique consumer groupd id.
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = broker,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe(topic);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
    }
}
