namespace KafkaConfig

open Confluent.Kafka

// Concrete Kafka configuration class implementing IKafkaConfig
type KafkaConfig() =
    interface IKafkaConfig with
        member _.BootstrapServers = "localhost:29092,localhost:29093,localhost:29094"
        member _.SecurityProtocol = SecurityProtocol.Plaintext
