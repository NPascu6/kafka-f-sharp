open System
open KafkaService
open KafkaConfig
open LoggingWrapper
open KafkaStreamsService

let createTopic (kafkaService: IKafkaService) =
    printf "Enter the topic name: "
    let topicName = Console.ReadLine()
    printf "Enter the number of partitions (default 1): "

    let partitions =
        Console.ReadLine()
        |> Int32.TryParse
        |> function
            | true, v -> v
            | _ -> 1

    printf "Enter the replication factor (default 1): "

    let replicationFactor =
        Console.ReadLine()
        |> Int32.TryParse
        |> function
            | true, v -> v
            | _ -> 1

    kafkaService.CreateTopic topicName partitions replicationFactor
    |> Async.RunSynchronously

let sendMessage (kafkaService: IKafkaService) =
    printf "Enter the topic name: "
    let topicName = Console.ReadLine()
    printf "Enter the message key (optional, press Enter to skip): "
    let key = Console.ReadLine()
    printf "Enter the message value: "
    let value = Console.ReadLine()
    printf "Enter the partition (optional, press Enter to skip): "

    let partition =
        Console.ReadLine()
        |> Int32.TryParse
        |> function
            | true, p -> Some p
            | _ -> None

    kafkaService.ProduceMessage topicName key value partition
    |> Async.RunSynchronously

    printfn "Message sent to topic '%s'." topicName

let listTopics (kafkaService: IKafkaService) =
    kafkaService.ListTopics() |> Async.RunSynchronously

let deleteTopic (kafkaService: IKafkaService) =
    printf "Enter the topic name: "
    let topicName = Console.ReadLine()
    kafkaService.DeleteTopic(topicName) |> Async.RunSynchronously

let getTopicDetails (kafkaService: IKafkaService) =
    printf "Enter the topic name: "
    kafkaService.GetTopicDetails(Console.ReadLine()) |> Async.RunSynchronously

let consumeMessages (kafkaService: IKafkaService) =
    printf "Enter the topic name to consume messages from: "
    let topicName = Console.ReadLine()
    printf "Enter the consumer group ID: "
    let groupId = Console.ReadLine()
    printf "Enter timeout in milliseconds (default 5000): "

    let timeoutMs =
        Console.ReadLine()
        |> Int32.TryParse
        |> function
            | true, v -> v
            | _ -> 5000

    kafkaService.ConsumeMessages topicName groupId timeoutMs
    |> Async.RunSynchronously

let deleteAllTopics (kafkaService: IKafkaService) =
    kafkaService.DeleteAllTopics() |> Async.RunSynchronously

// Stream functions with validations
let startStream (kafkaStreamsService: KafkaStreamsService) =
    printf "Enter the input topic (must be a valid, existing topic): "
    let inputTopic = Console.ReadLine()
    printf "Enter the output topic (must be a valid, existing topic different from input): "
    let outputTopic = Console.ReadLine()
    kafkaStreamsService.StartStream(inputTopic, outputTopic)
    printfn "Kafka Stream started between '%s' and '%s'." inputTopic outputTopic

let stopStream (kafkaStreamsService: KafkaStreamsService) =
    kafkaStreamsService.StopStream()
    printfn "Kafka Stream stopped."

let restartStream (kafkaStreamsService: KafkaStreamsService) =
    printf "Enter the input topic for restart: "
    let inputTopic = Console.ReadLine()
    printf "Enter the output topic for restart: "
    let outputTopic = Console.ReadLine()
    kafkaStreamsService.RestartStream(inputTopic, outputTopic)
    printfn "Kafka Stream restarted between '%s' and '%s'." inputTopic outputTopic

let getTopicMessages (kafkaService: IKafkaService) =
    printf ("Enter topic to get messages from: ")
    let topic = Console.ReadLine()
    kafkaService.GetTopicMessages topic |> Async.RunSynchronously

let getPartitionsForTopic (kafkaService: IKafkaService) =
    printf ("Enter topic to get partitions for: ")
    let topic = Console.ReadLine()
    kafkaService.GetPartitionsForTopic topic |> Async.RunSynchronously

let combineStreams (kafkaStreamsService: KafkaStreamsService) =
    printf "Enter the input1 topic (must be a valid, existing topic): "
    let inputTopic1 = Console.ReadLine()
    printf "Enter the input2 topic (must be a valid, existing topic): "
    let inputTopic2 = Console.ReadLine()
    printf "Enter the output topic (must be a valid, existing topic different from input): "
    let outputTopic = Console.ReadLine()
    kafkaStreamsService.CombineStreams(inputTopic1, inputTopic2, outputTopic)
    printfn "Kafka Stream started between '%s' and '%s' to %s." inputTopic1 inputTopic2 outputTopic

let showMenu () =
    printfn "\nKafka Client Options:"
    printfn "1. Create a new topic"
    printfn "2. Send a message to a topic"
    printfn "3. List topics"
    printfn "4. Delete a topic"
    printfn "5. Get topic details"
    printfn "6. Consume messages from a topic real time"
    printfn "7. Start Kafka stream"
    printfn "10. Get partitions for a topic"
    printfn "11. Delete all topics"
    printfn "12. Get messages from a topic"
    printfn "13. Combine two streams"
    printfn "q. Quit"
    printf "Select an option: "

[<EntryPoint>]
let main argv =
    let kafkaConfig = KafkaConfig() :> IKafkaConfig
    let logger = LoggingWrapper() :> ILoggingWrapper
    let kafkaService = KafkaService(kafkaConfig, logger) :> IKafkaService
    let kafkaStreamsService = new KafkaStreamsService(kafkaConfig, logger, kafkaService)

    let rec interactiveLoop () =
        showMenu ()

        match Console.ReadLine() with
        | "1" ->
            createTopic kafkaService
            interactiveLoop ()
        | "2" ->
            sendMessage kafkaService
            interactiveLoop ()
        | "3" ->
            listTopics kafkaService
            interactiveLoop ()
        | "4" ->
            deleteTopic kafkaService
            interactiveLoop ()
        | "5" ->
            getTopicDetails kafkaService
            interactiveLoop ()
        | "6" ->
            consumeMessages kafkaService
            interactiveLoop ()
        | "7" ->
            startStream kafkaStreamsService
            interactiveLoop ()
        | "8" ->
            stopStream kafkaStreamsService
            interactiveLoop ()
        | "9" ->
            restartStream kafkaStreamsService
            interactiveLoop ()
        | "10" ->
            getPartitionsForTopic kafkaService
            interactiveLoop ()
        | "11" ->
            deleteAllTopics kafkaService
            interactiveLoop ()
        | "12" ->
            getTopicMessages kafkaService |> ignore
            interactiveLoop ()
        | "13" ->
            combineStreams kafkaStreamsService
            interactiveLoop ()
        | "q"
        | "Q" -> printfn "Exiting Kafka Client..."
        | _ ->
            printfn "Invalid choice. Please select a valid option."
            interactiveLoop ()

    interactiveLoop ()
    0
