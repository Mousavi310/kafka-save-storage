﻿using System;
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
        static async Task Main(string[] args)
        {
            var broker = "localhost:9092";
            var schemaRegistryUrl = "http://localhost:8081/";


            await Produce_Json(broker, "maintopic40", linger:100);
            //await Produce_Json(broker, "maintopic30", linger:1, delayForEachProduce:100);
            //await Produce_Json(broker, "maintopic20", linger:100, compressionType: CompressionType.Gzip);
            //await Produce_Json(broker, "maintopic15", linger:100, compressionType: CompressionType.Snappy);
            
            await Produce_Avro(broker, schemaRegistryUrl, "maintopic41", linger:100);
            //await Produce_Avro(broker, schemaRegistryUrl, "maintopic31", linger:1, delayForEachProduce:100);
            //await Produce_Avro(broker, schemaRegistryUrl, "maintopic21", linger:100, compressionType: CompressionType.Gzip);
            //await Produce_Avro(broker, schemaRegistryUrl, "maintopic16", linger:100, compressionType: CompressionType.Snappy);
            
            
            Console.WriteLine("Hello World!");
        }

        public static async Task Produce_Json(string broker, 
            string topic,
            double? linger = null, 
            int? delayForEachProduce = null, 
            CompressionType? compressionType = null)
        {
            using(var producer = new ProducerBuilder<Null, string>(
                new ProducerConfig
                {
                    BootstrapServers = broker,
                    LingerMs = linger,
                    CompressionType = compressionType,
                    CompressionLevel = GetCompressionLevel(compressionType)
                }
            ).Build())
            {
                var records = OrderRepository
                    .CreateJsonOrders()
                    .Select(d => JsonConvert.SerializeObject(d));
                foreach(var item in records)
                {
                    if(delayForEachProduce.HasValue)
                        await Task.Delay(delayForEachProduce.Value);
                    producer
                        .Produce(topic, new Message<Null, string>{Value = item});
                }

                producer.Flush();
            }
        }

        public static async Task Produce_Avro(string broker, 
            string schemaRegistryUrl,
            string topic,
            double? linger = null, 
            int? delayForEachProduce = null, 
            CompressionType? compressionType = null,
            int compressionLevel = 1)
        {
            using(var schemaRegistry = new CachedSchemaRegistryClient(
                new SchemaRegistryConfig {SchemaRegistryUrl = schemaRegistryUrl}
            ))
            {
                var config = new ProducerConfig{
                        BootstrapServers = broker,
                        CompressionType = compressionType,
                        LingerMs = linger,
                        CompressionLevel = GetCompressionLevel(compressionType)
                    };
                using(var producer = new ProducerBuilder<Null, OrderAvroModel>(config)
                .SetValueSerializer(new SyncOverAsyncSerializer<OrderAvroModel>(new AvroSerializer<OrderAvroModel>(schemaRegistry)))
                 .Build())
                {
                     var records = OrderRepository
                    .CreateAvroOrders();

                    foreach(var item in records)
                    {
                        if(delayForEachProduce.HasValue)
                            await Task.Delay(delayForEachProduce.Value);
                        producer
                            .Produce(topic, new Message<Null, OrderAvroModel>{Value = item});
                    }

                    producer.Flush();
                }
            }

        }

        private static int? GetCompressionLevel(CompressionType? compressionType)
        {
            if(compressionType == null)
                return null;
            
            if(compressionType == CompressionType.Gzip)
                return 9;
            
            if(compressionType == CompressionType.Snappy)
                return 0;

            return null;
        }
    }
}
