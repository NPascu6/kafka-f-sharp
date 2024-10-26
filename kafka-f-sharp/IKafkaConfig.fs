namespace KafkaConfig

open Confluent.Kafka

type IKafkaConfig =
    abstract member BootstrapServers: string
    abstract member SecurityProtocol: SecurityProtocol
