module KafkaStreamsService

open System
open System.Threading.Tasks
open Streamiz.Kafka.Net
open Streamiz.Kafka.Net.SerDes
open KafkaConfig
open LoggingWrapper
open KafkaService
open Confluent.Kafka
open Streamiz.Kafka.Net.Table
open Streamiz.Kafka.Net.State
open Streamiz.Kafka.Net.Crosscutting

type KafkaStreamsService(kafkaConfig: IKafkaConfig, logger: ILoggingWrapper, kafkaService: IKafkaService) =
    let mutable kafkaStream: KafkaStream option = None
    let mutable streamShouldRun = true

    // Stream configuration
    let streamConfig =
        StreamConfig<StringSerDes, StringSerDes>()
        |> fun config ->
            config.BootstrapServers <- kafkaConfig.BootstrapServers
            config.ApplicationId <- "fsharp-streams-app"
            config.SecurityProtocol <- kafkaConfig.SecurityProtocol
            config.AutoOffsetReset <- AutoOffsetReset.Earliest
            config.NumStreamThreads <- 2 // Increase stream threads for resilience
            config

    // Helper to start the stream with retry
    let rec startStreamWithRetry (builder: StreamBuilder) =
        async {
            let stream = new KafkaStream(builder.Build(), streamConfig)

            try
                stream.StartAsync().Wait()
                logger.LogInfo("Kafka Stream started.")
                kafkaStream <- Some stream
            with ex ->
                logger.LogError("Error starting Kafka Stream. Retrying in 5 seconds...", ex)
                Task.Delay(5000) |> Async.AwaitTask |> ignore
                return! startStreamWithRetry builder // Retry start on failure
        }

    // Start a stream with input and output topics
    member this.StartStream(inputTopic: string, outputTopic: string) =
        let builder = StreamBuilder()

        let inputTopic = kafkaService.GetTopicByName inputTopic

        let outputTopic = kafkaService.GetTopicByName outputTopic

        if
            inputTopic <> "0"
            && outputTopic <> "0"
            && not (String.IsNullOrEmpty inputTopic || String.IsNullOrEmpty outputTopic)
        then
            // Define the stream topology with error handling in transformations
            builder
                .Stream<string, string>(inputTopic)
                .MapValues(fun value ->
                    try
                        logger.LogInfo(sprintf "Processing message value: %s" value)
                        value.ToUpper() // Example transformation
                    with ex ->
                        logger.LogError("Error processing message value.", ex)
                        value) // Return original value if an error occurs
                .To(outputTopic)

            // Start the stream with retry logic
            streamShouldRun <- true
            Async.Start(startStreamWithRetry builder)

    // Stop the stream if it is active
    member this.StopStream() =
        streamShouldRun <- false

        match kafkaStream with
        | Some stream ->
            async {
                try
                    stream.Dispose()
                    logger.LogInfo("Kafka Stream stopped.")
                finally
                    kafkaStream <- None // Clear the reference after stopping
            }
            |> Async.RunSynchronously
        | None -> logger.LogInfo("No active Kafka stream to stop.")

    member this.RestartStream(inputTopic: string, outputTopic: string) =
        this.StopStream()
        this.StartStream(inputTopic, outputTopic)

    member this.CombineStreams(inputTopic1: string, inputTopic2: string, outputTopic: string) =
        let builder = StreamBuilder()

        let topic1 = kafkaService.GetTopicByName inputTopic1
        let topic2 = kafkaService.GetTopicByName inputTopic2

        if topic1 <> "0" && topic2 <> "0" then
            // Define SerDes explicitly
            let keySerdes = StringSerDes() :> ISerDes<string>
            let valueSerdes = StringSerDes() :> ISerDes<string>

            // Create the first stream with explicit SerDes
            let stream1 = builder.Stream<string, string>(topic1, keySerdes, valueSerdes)

            // Create the second stream with explicit SerDes
            let stream2 = builder.Stream<string, string>(topic2, keySerdes, valueSerdes)

            // Merge the two streams
            let combinedStream = stream1.Merge(stream2)

            // Send the merged stream to the output topic with explicit SerDes
            combinedStream.To(outputTopic, keySerdes, valueSerdes)

            streamShouldRun <- true
            Async.Start(startStreamWithRetry builder)
            logger.LogInfo("Combined streams from topics into one output stream.")

    member this.JoinTwoKTables(topic1: string, topic2: string, outputTopic: string) =
        let builder = StreamBuilder()
        let topic1 = kafkaService.GetTopicByName topic1
        let topic2 = kafkaService.GetTopicByName topic2

        if topic1 <> "0" && topic2 <> "0" then
            let table1 =
                builder.Table<string, string>(
                    topic1,
                    Materialized<string, string, IKeyValueStore<Bytes, byte[]>>
                        .Create($"{topic1}-table1-store")
                )

            logger.LogInfo("Table 1 created.")
            logger.LogInfo(table1.MapValues(fun v -> v).ToString())

            let table2 =
                builder.Table<string, string>(
                    topic2,
                    Materialized<string, string, IKeyValueStore<Bytes, byte[]>>
                        .Create($"{topic2}-table2-store")
                )

            logger.LogInfo("Table 2 created.")
            logger.LogInfo(table2.MapValues(fun v -> v).ToString())

            // Join the two tables and output the result to a new topic
            table1.Join(table2, (fun v1 v2 -> v1 + v2)).ToStream().To(outputTopic)

            logger.LogInfo("Tables joined.")
            logger.LogInfo(builder.ToString())

            streamShouldRun <- true
            Async.Start(startStreamWithRetry builder)

    // Implement IDisposable to ensure resource cleanup
    interface IDisposable with
        member this.Dispose() =
            this.StopStream() // Ensure stream is stopped when disposed
            kafkaStream <- None
            logger.LogInfo("KafkaStreamsService disposed.")
