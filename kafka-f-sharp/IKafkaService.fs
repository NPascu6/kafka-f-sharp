namespace KafkaService

type IKafkaService =
    abstract member ProduceMessage:
        topic: string -> key: string -> value: string -> partition: int option -> Async<unit>

    abstract member CreateTopic: topic: string -> numPartitions: int -> replicationFactor: int -> Async<unit>

    abstract member ListTopics: unit -> Async<unit>

    abstract member DeleteTopic: topic: string -> Async<unit>

    abstract member GetTopicDetails: topic: string -> Async<unit>

    abstract member ConsumeMessages: topic: string -> groupId: string -> timeoutMs: int -> Async<unit>
