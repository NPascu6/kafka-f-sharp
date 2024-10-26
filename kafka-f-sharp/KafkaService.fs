namespace KafkaService

open Confluent.Kafka
open Confluent.Kafka.Admin
open System
open LoggingWrapper
open KafkaConfig

// Kafka service implementation using IKafkaConfig and ILogger for dependency injection
type KafkaService(kafkaConfig: IKafkaConfig, logger: ILoggingWrapper) =

    // Configuration for the Kafka producer using provided settings
    let producerConfig =
        let config = ProducerConfig(BootstrapServers = kafkaConfig.BootstrapServers)
        config.MessageTimeoutMs <- 5000
        config.RequestTimeoutMs <- 3000
        config.EnableIdempotence <- true
        config.MessageSendMaxRetries <- 3
        config.Acks <- Acks.All
        config.LingerMs <- 1
        config.EnableDeliveryReports <- true
        config.EnableBackgroundPoll <- true
        config.MaxInFlight <- 5
        config.RetryBackoffMs <- 100
        config.SecurityProtocol <- kafkaConfig.SecurityProtocol
        config

    member this.ProduceMessage (topic: string) (key: string) (value: string) (partition: int option) =
        async {
            logger.LogInfo(sprintf "Producing message to topic '%s'." topic)
            use producer = ProducerBuilder<string, string>(producerConfig).Build()

            let message = Message<string, string>(Key = key, Value = value)

            try
                let! result =
                    match partition with
                    | Some p ->
                        logger.LogInfo(sprintf "Sending message to topic '%s', partition %d." topic p)

                        producer.ProduceAsync(TopicPartition(topic, Partition p), message)
                        |> Async.AwaitTask
                    | None ->
                        logger.LogInfo(
                            sprintf "Sending message to topic '%s' (partition will be chosen by Kafka)." topic
                        )

                        producer.ProduceAsync(topic, message) |> Async.AwaitTask

                logger.LogInfo(
                    sprintf
                        "Message delivered to %s [%d] at offset %d"
                        result.Topic
                        result.Partition.Value
                        result.Offset.Value
                )
            with ex ->
                let errorMessage = sprintf "Failed to produce message to topic '%s'" topic
                logger.LogError(errorMessage, ex)
        }

    member this.CreateTopic (topic: string) (numPartitions: int) (replicationFactor: int) =
        async {
            logger.LogInfo(
                sprintf
                    "Creating topic '%s' with %d partitions and replication factor %d."
                    topic
                    numPartitions
                    replicationFactor
            )

            let adminConfig = AdminClientConfig(BootstrapServers = kafkaConfig.BootstrapServers)
            adminConfig.SecurityProtocol <- kafkaConfig.SecurityProtocol

            use adminClient = AdminClientBuilder(adminConfig).Build()

            let topicConfig =
                TopicSpecification(
                    Name = topic,
                    NumPartitions = numPartitions,
                    ReplicationFactor = int16 replicationFactor
                )

            let checkIfTopicExists () =
                try
                    let metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(5))

                    metadata.Topics
                    |> Seq.exists (fun t -> t.Topic = topic && t.Error.Code = ErrorCode.NoError)
                with :? KafkaException as ex when ex.Error.Code = ErrorCode.UnknownTopicOrPart ->
                    false

            try
                logger.LogInfo(sprintf "Checking if topic '%s' already exists." topic)
                let topicExists = checkIfTopicExists ()

                if topicExists then
                    logger.LogInfo(sprintf "Topic '%s' already exists, skipping creation." topic)
                else
                    adminClient.CreateTopicsAsync([ topicConfig ]) |> Async.AwaitTask |> ignore
                    logger.LogInfo(sprintf "Topic '%s' created successfully." topic)
            with
            | :? CreateTopicsException as ex when ex.Results.[0].Error.Code = ErrorCode.TopicAlreadyExists ->
                logger.LogInfo(sprintf "Topic '%s' already exists, skipping creation." topic)
            | ex -> logger.LogError(sprintf "Failed to create topic '%s'" topic, ex)
        }

    member this.ListTopics() =
        async {
            logger.LogInfo("Listing topics...")
            let adminConfig = AdminClientConfig(BootstrapServers = kafkaConfig.BootstrapServers)
            adminConfig.SecurityProtocol <- kafkaConfig.SecurityProtocol

            use adminClient = AdminClientBuilder(adminConfig).Build()

            try
                let metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5))
                let topics = metadata.Topics |> Seq.map (fun t -> t.Topic)
                logger.LogInfo("Topics found:")

                if Seq.isEmpty topics then
                    logger.LogInfo("No topics found.")
                else
                    logger.LogInfo("Topics found:")
                    topics |> Seq.iter (fun topicName -> logger.LogInfo(sprintf "- %s" topicName))
            with ex ->
                logger.LogError("Failed to list topics.", ex)
        }

    member this.DeleteTopic(topic: string) =
        async {
            logger.LogInfo(sprintf "Deleting topic '%s'." topic)
            let adminConfig = AdminClientConfig(BootstrapServers = kafkaConfig.BootstrapServers)
            adminConfig.SecurityProtocol <- kafkaConfig.SecurityProtocol

            use adminClient = AdminClientBuilder(adminConfig).Build()

            let options =
                new DeleteTopicsOptions(
                    RequestTimeout = TimeSpan.FromSeconds(5),
                    OperationTimeout = TimeSpan.FromSeconds(5)
                )

            try
                // Attempt to delete the specified topic
                adminClient.DeleteTopicsAsync([ topic ], options) |> Async.AwaitTask |> ignore

                logger.LogInfo(sprintf "Topic '%s' deleted successfully." topic)
            with
            | :? DeleteTopicsException as ex when
                ex.Results |> Seq.exists (fun r -> r.Error.Code = ErrorCode.UnknownTopicOrPart)
                ->
                logger.LogInfo(sprintf "Topic '%s' does not exist, nothing to delete." topic)
            | ex -> logger.LogError(sprintf "Failed to delete topic '%s'." topic, ex)
        }

    member this.ConsumeMessages (topic: string) (groupId: string) (timeoutMs: int) =
        let consumerConfig =
            let config = ConsumerConfig()
            config.BootstrapServers <- kafkaConfig.BootstrapServers
            config.GroupId <- groupId
            config.AutoOffsetReset <- AutoOffsetReset.Earliest
            config.EnableAutoCommit <- true
            config.SecurityProtocol <- kafkaConfig.SecurityProtocol
            config

        async {
            use consumer = ConsumerBuilder<string, string>(consumerConfig).Build()
            consumer.Subscribe(topic)
            logger.LogInfo(sprintf "Consuming messages from topic '%s' with group ID '%s'." topic groupId)

            try
                let rec consumeLoop () =
                    async {
                        let! result =
                            async {
                                try
                                    return Some(consumer.Consume(TimeSpan.FromMilliseconds(float timeoutMs)))
                                with :? ConsumeException as ex ->
                                    logger.LogError("Error consuming message", ex)
                                    return None
                            }

                        match result with
                        | Some record when record <> null ->
                            logger.LogInfo(
                                sprintf
                                    "Consumed message: Key=%s, Value=%s, Partition=%d, Offset=%d"
                                    record.Message.Key
                                    record.Message.Value
                                    record.Partition.Value
                                    record.Offset.Value
                            )

                            return! consumeLoop ()
                        | _ -> return ()

                    }

                do! consumeLoop ()
            finally
                consumer.Close()
        }

    member this.GetTopicDetails(topic: string) =
        async {
            logger.LogInfo(sprintf "Fetching details for topic '%s'." topic)
            let adminConfig = AdminClientConfig(BootstrapServers = kafkaConfig.BootstrapServers)
            adminConfig.SecurityProtocol <- kafkaConfig.SecurityProtocol

            use adminClient = AdminClientBuilder(adminConfig).Build()

            try
                let metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(5))
                let topicMetadata = metadata.Topics |> Seq.tryFind (fun t -> t.Topic = topic)

                match topicMetadata with
                | Some t ->
                    logger.LogInfo(sprintf "Topic: %s" t.Topic)
                    logger.LogInfo(sprintf "Partitions: %d" t.Partitions.Count)

                    t.Partitions
                    |> Seq.iter (fun p ->
                        logger.LogInfo(
                            sprintf "Partition: %d, Leader: %d, Replicas: %A" p.PartitionId p.Leader p.Replicas
                        ))
                | None -> logger.LogInfo(sprintf "Topic '%s' does not exist." topic)
            with ex ->
                logger.LogError(sprintf "Failed to fetch details for topic '%s'." topic, ex)
        }

    interface IKafkaService with
        member this.ProduceMessage (topic: string) (key: string) (value: string) (partition: int option) =
            this.ProduceMessage topic key value partition

        member this.CreateTopic (topic: string) (numPartitions: int) (replicationFactor: int) =
            this.CreateTopic topic numPartitions replicationFactor

        member this.ListTopics() = this.ListTopics()

        member this.DeleteTopic(topic: string) = this.DeleteTopic topic

        member this.GetTopicDetails(topic: string) = this.GetTopicDetails topic

        member this.ConsumeMessages (topic: string) (groupId: string) (timeoutMs: int) =
            this.ConsumeMessages topic groupId timeoutMs
