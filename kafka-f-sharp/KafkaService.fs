namespace KafkaService

open Confluent.Kafka
open Confluent.Kafka.Admin
open System
open LoggingWrapper
open KafkaConfig

type KafkaService(kafkaConfig: IKafkaConfig, logger: ILoggingWrapper) =

    // Configuration setup for the producer with specified parameters
    let producerConfig =
        ProducerConfig(
            BootstrapServers = kafkaConfig.BootstrapServers,
            MessageTimeoutMs = 5000,
            RequestTimeoutMs = 3000,
            EnableIdempotence = true,
            MessageSendMaxRetries = 3,
            Acks = Acks.All,
            LingerMs = 1,
            EnableDeliveryReports = true,
            EnableBackgroundPoll = true,
            MaxInFlight = 5,
            RetryBackoffMs = 100,
            SecurityProtocol = kafkaConfig.SecurityProtocol
        )

    // Generic method for handling logging of async tasks with exception handling
    let asyncLogger (action: Async<unit>) errorMessage =
        async {
            try
                do! action
            with ex ->
                logger.LogError(errorMessage, ex)
        }

    // Checks for topic existence with async functionality
    member this.CheckTopicExists(topic: string) =
        async {
            if String.IsNullOrEmpty topic then
                return false
            else
                let adminConfig =
                    AdminClientConfig(
                        BootstrapServers = kafkaConfig.BootstrapServers,
                        SecurityProtocol = kafkaConfig.SecurityProtocol
                    )

                use adminClient = AdminClientBuilder(adminConfig).Build()

                try
                    let metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(5))

                    return
                        metadata.Topics
                        |> Seq.exists (fun t -> t.Topic = topic && t.Error.Code = ErrorCode.NoError)
                with ex ->
                    logger.LogError(sprintf "Error checking existence of topic '%s'." topic, ex)
                    return false
        }
        |> Async.RunSynchronously

    // Method for producing a single message with options for specifying a partition
    member this.ProduceMessage (topic: string) (key: string) (value: string) (partition: int option) =
        async {
            if String.IsNullOrWhiteSpace(topic) || String.IsNullOrWhiteSpace(value) then
                logger.LogError("Topic and message value must be provided.", null)
            else
                use producer = ProducerBuilder<string, string>(producerConfig).Build()
                let message = Message<string, string>(Key = key, Value = value)
                logger.LogInfo(sprintf "Producing message to topic '%s'." topic)

                let! result =
                    match partition with
                    | Some p ->
                        producer.ProduceAsync(TopicPartition(topic, Partition p), message)
                        |> Async.AwaitTask
                    | None -> producer.ProduceAsync(topic, message) |> Async.AwaitTask

                logger.LogInfo(
                    sprintf
                        "Message delivered to %s [%d] at offset %d"
                        result.Topic
                        result.Partition.Value
                        result.Offset.Value
                )
        }

    // Produces a batch of messages to the specified topic
    member this.ProduceBatchMessages (topic: string) (messages: (string * string) seq) =
        if String.IsNullOrWhiteSpace(topic) then
            logger.LogError("Topic must be provided.", null)
        else
            use producer = ProducerBuilder<string, string>(producerConfig).Build()

            let produceTask =
                async {
                    for (key, value) in messages do
                        let message = Message<string, string>(Key = key, Value = value)
                        producer.ProduceAsync(topic, message) |> Async.AwaitTask |> ignore
                        logger.LogInfo(sprintf "Message with key '%s' sent to topic '%s'." key topic)
                }

            try
                produceTask |> Async.RunSynchronously
                logger.LogInfo("All messages sent successfully.")
            with ex ->
                logger.LogError("Error producing messages.", ex)

    // Creates or updates a topic configuration
    member this.CreateOrUpdateTopic (topic: string) (numPartitions: int) (replicationFactor: int) =
        async {
            if String.IsNullOrWhiteSpace(topic) || numPartitions <= 0 || replicationFactor <= 0 then
                logger.LogError("Invalid topic name, partition count, or replication factor.", null)
            else
                let adminConfig =
                    AdminClientConfig(
                        BootstrapServers = kafkaConfig.BootstrapServers,
                        SecurityProtocol = kafkaConfig.SecurityProtocol
                    )

                use adminClient = AdminClientBuilder(adminConfig).Build()

                let topicConfig =
                    TopicSpecification(
                        Name = topic,
                        NumPartitions = numPartitions,
                        ReplicationFactor = int16 replicationFactor
                    )

                let exists = this.CheckTopicExists topic

                if exists then
                    logger.LogInfo(sprintf "Topic '%s' exists. Updating configuration if necessary." topic)
                else
                    do!
                        adminClient.CreateTopicsAsync([ topicConfig ])
                        |> Async.AwaitTask
                        |> Async.Ignore

                    logger.LogInfo(sprintf "Topic '%s' created successfully." topic)
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
            let topicExists = this.CheckTopicExists topic
            use adminClient = AdminClientBuilder(adminConfig).Build()

            if (not topicExists) then
                logger.LogInfo(sprintf "Topic '%s' does not exist." topic)
            else
                try
                    let metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(5))
                    let topicMetadata = metadata.Topics |> Seq.tryFind (fun t -> t.Topic = topic)
                    let partitions = topicMetadata |> Option.map (fun t -> t.Partitions.Count)

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

    member this.GetPartitionsForTopic topic =
        async {
            logger.LogInfo(sprintf "Fetching partitions for topic '%s'." topic)
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

                    t.Partitions |> Seq.map (fun p -> p.PartitionId) |> ignore
                | None ->
                    logger.LogInfo(sprintf "Topic '%s' does not exist." topic)
                    Seq.empty |> ignore
            with ex ->
                logger.LogError(sprintf "Failed to fetch partitions for topic '%s'." topic, ex)
                Seq.empty |> ignore
        }

    member this.GetAllTopics() =
        async {
            logger.LogInfo("Fetching details for all topics.")
            let adminConfig = AdminClientConfig(BootstrapServers = kafkaConfig.BootstrapServers)
            adminConfig.SecurityProtocol <- kafkaConfig.SecurityProtocol

            use adminClient = AdminClientBuilder(adminConfig).Build()

            try
                let metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5))
                let topics = metadata.Topics

                if Seq.isEmpty topics then
                    logger.LogInfo("No topics found.")
                else
                    topics
                    |> Seq.iter (fun t ->
                        logger.LogInfo(sprintf "Topic: %s" t.Topic)
                        logger.LogInfo(sprintf "Partitions: %d" t.Partitions.Count)

                        t.Partitions
                        |> Seq.iter (fun p ->
                            logger.LogInfo(
                                sprintf "Partition: %d, Leader: %d, Replicas: %A" p.PartitionId p.Leader p.Replicas
                            )))
            with ex ->
                logger.LogError("Failed to fetch details for all topics.", ex)
        }

    member this.GetTopicByName name =
        logger.LogInfo(sprintf "Fetching details for topic '%s'." name)
        let adminConfig = AdminClientConfig(BootstrapServers = kafkaConfig.BootstrapServers)
        adminConfig.SecurityProtocol <- kafkaConfig.SecurityProtocol

        use adminClient = AdminClientBuilder(adminConfig).Build()

        try
            let metadata = adminClient.GetMetadata(name, TimeSpan.FromSeconds(5))
            let topicMetadata = metadata.Topics |> Seq.tryFind (fun t -> t.Topic = name)

            match topicMetadata with
            | Some t ->
                logger.LogInfo(sprintf "Topic: %s" t.Topic)
                logger.LogInfo(sprintf "Partitions: %d" t.Partitions.Count)

                t.Partitions
                |> Seq.iter (fun p ->
                    logger.LogInfo(sprintf "Partition: %d, Leader: %d, Replicas: %A" p.PartitionId p.Leader p.Replicas))

                t.Topic
            | None ->
                logger.LogInfo(sprintf "Topic '%s' does not exist." name)
                "0"

        with ex ->
            logger.LogError(sprintf "Failed to fetch details for topic '%s'." name, ex)
            "0"

    member this.DeleteAllTopics() =
        async {
            logger.LogInfo("Deleting all topics.")
            let adminConfig = AdminClientConfig(BootstrapServers = kafkaConfig.BootstrapServers)
            adminConfig.SecurityProtocol <- kafkaConfig.SecurityProtocol

            use adminClient = AdminClientBuilder(adminConfig).Build()

            try
                let metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5))
                let topics = metadata.Topics |> Seq.map (fun t -> t.Topic)

                if Seq.isEmpty topics then
                    logger.LogInfo("No topics found.")
                else
                    topics
                    |> Seq.iter (fun t ->
                        logger.LogInfo(sprintf "Deleting topic '%s'." t)
                        adminClient.DeleteTopicsAsync([ t ]) |> Async.AwaitTask |> ignore
                        logger.LogInfo(sprintf "Topic '%s' deleted successfully." t))
            with ex ->
                logger.LogError("Failed to delete all topics.", ex)
        }

    member this.GetTopicMessages(topic: string) =
        async {
            let consumerConfig =
                let config = ConsumerConfig()
                config.BootstrapServers <- kafkaConfig.BootstrapServers
                config.GroupId <- "KafkaService"
                config.AutoOffsetReset <- AutoOffsetReset.Earliest
                config.EnableAutoCommit <- true
                config.SecurityProtocol <- kafkaConfig.SecurityProtocol
                config

            let adminClient =
                AdminClientBuilder(AdminClientConfig(BootstrapServers = kafkaConfig.BootstrapServers))
                    .Build()

            let metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5))
            let topicMetadata = metadata.Topics |> Seq.tryFind (fun t -> t.Topic = topic)

            let partitions =
                match topicMetadata with
                | Some t -> t.Partitions |> Seq.map (fun p -> p.PartitionId)
                | None -> Seq.empty

            let listOfPartitions = partitions |> Seq.toList

            for partition in listOfPartitions do
                async {
                    use consumer = ConsumerBuilder<string, string>(consumerConfig).Build()
                    consumer.Assign([ TopicPartitionOffset(topic, partition, Offset.Beginning) ])

                    let rec consumeLoop () =
                        async {
                            let! result =
                                async {
                                    try
                                        return Some(consumer.Consume(TimeSpan.FromSeconds(1)))
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
                }
                |> Async.Start
        }

    interface IKafkaService with
        member this.ProduceMessage (topic: string) (key: string) (value: string) (partition: int option) =
            this.ProduceMessage topic key value partition

        member this.CreateTopic (topic: string) (numPartitions: int) (replicationFactor: int) =
            this.CreateOrUpdateTopic topic numPartitions replicationFactor

        member this.ListTopics() = this.ListTopics()

        member this.DeleteTopic(topic: string) = this.DeleteTopic topic

        member this.GetTopicDetails(topic: string) = this.GetTopicDetails topic

        member this.ConsumeMessages (topic: string) (groupId: string) (timeoutMs: int) =
            this.ConsumeMessages topic groupId timeoutMs

        member this.GetAllTopics() = this.GetAllTopics()

        member this.GetTopicByName name = this.GetTopicByName name

        member this.ProduceBatchMessages (topic: string) (messages: (string * string) seq) =
            this.ProduceBatchMessages topic messages

        member this.GetTopicMessages topic = this.GetTopicMessages topic

        member this.GetPartitionsForTopic topic = this.GetPartitionsForTopic topic

        member this.DeleteAllTopics() = this.DeleteAllTopics()
