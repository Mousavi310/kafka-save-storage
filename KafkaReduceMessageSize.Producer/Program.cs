using System;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Newtonsoft.Json;

namespace KafkaReduceMessageSize.Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
        }

        public static async Task Produce_Json(string broker, double? linger, int delayForEachProduce)
        {
            using(var producer = new ProducerBuilder<Null, string>(
                new ProducerConfig
                {
                    BootstrapServers = "localhost:9092",
                    LingerMs = 0,
                }
            ).Build())
            {
                var records = OrderRepository
                    .CreateJsonOrders()
                    .Select(d => JsonConvert.SerializeObject(d));
                foreach(var item in records)
                {
                    await Task.Delay(100);
                    producer
                        .Produce("sample-json-4", new Message<Null, string>{Value = item});
                }
            }
        }

        public static async Task Produce_Single_Json_NoCompression()
        {
            using(var producer = new ProducerBuilder<Null, string>(
                new ProducerConfig
                {
                    BootstrapServers = "localhost:9092",
                    LingerMs = 0,
                }
            ).Build())
            {
                var records = OrderRepository
                    .CreateJsonOrders()
                    .Select(d => JsonConvert.SerializeObject(d));
                foreach(var item in records)
                {
                    await Task.Delay(100);
                    producer
                        .Produce("sample-json-4", new Message<Null, string>{Value = item});
                }
            }
        }

        public static void Produce_Batch_Json()
        {
            using(var producer = new ProducerBuilder<Null, string>(
                new ProducerConfig{
                    BootstrapServers = "localhost:9092",
                }
            ).Build())
            {
                var records = OrderRepository
                    .CreateJsonOrders()
                    .Select(d => JsonConvert.SerializeObject(d));
                foreach(var item in records)
                {
                    producer
                        .Produce("sample-json-batch-1", new Message<Null, string>{Value = item});
                }

                producer.Flush(TimeSpan.FromSeconds(30));
            }
        }

        public static void Produce_Avro_Batch_Compression_Async()
        {
            using(var schemaRegistry = new CachedSchemaRegistryClient(
                new SchemaRegistryConfig {SchemaRegistryUrl = "http://localhost:8081/"}
            ))
            {
                using(var producer = new ProducerBuilder<Null, OrderAvroModel>(
                    new ProducerConfig{
                        BootstrapServers = "localhost:9092",
                        //CompressionType = CompressionType.Gzip,
                        //CompressionLevel = 9
                    }
                ).SetValueSerializer(new SyncOverAsyncSerializer<OrderAvroModel>(new AvroSerializer<OrderAvroModel>(schemaRegistry)))
                 .Build())
                {
                     var records = OrderRepository
                    .CreateJsonOrders()
                    .Select(d => new OrderAvroModel{
                        CreationTime = d.CreationTime,
                        CustomerId  = d.CustomerId,
                        Id = d.Id,
                        ProductId = d.ProductId,
                        Status = d.Status
                    });
                    foreach(var item in records)
                    {
                        producer
                            .Produce("sample-avro-batch-compression-2", new Message<Null, OrderAvroModel>{Value = item});
                    }

                    producer.Flush(TimeSpan.FromSeconds(30));
                }
            }
        }
    }
}
