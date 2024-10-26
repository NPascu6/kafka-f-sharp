open System
open KafkaService
open KafkaConfig
open LoggingWrapper

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

    kafkaService.DeleteTopic topicName |> Async.RunSynchronously

    printfn "Topic '%s' deletion requested." topicName

let getTopicDetails (kafkaService: IKafkaService) =
    printf "Enter the topic name to retrieve details: "
    let topicName = Console.ReadLine()

    kafkaService.GetTopicDetails topicName |> Async.RunSynchronously

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
            | _ -> 40000

    kafkaService.ConsumeMessages topicName groupId timeoutMs

let showMenu () =
    printfn "\nKafka Client Options:"
    printfn "1. Create a new topic"
    printfn "2. Send a message to a topic"
    printfn "3. List topics"
    printfn "4. Delete a topic"
    printfn "5. Get topic details"
    printfn "6. Consume messages from a topic"
    printfn "q. Quit"
    printf "Select an option: "

[<EntryPoint>]
let main argv =
    let kafkaConfig = KafkaConfig() :> IKafkaConfig
    let logger = LoggingWrapper() :> ILoggingWrapper
    let kafkaService = KafkaService(kafkaConfig, logger) :> IKafkaService

    let rec interactiveLoop () =
        showMenu ()
        let choice = Console.ReadLine()

        match choice with
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
            consumeMessages kafkaService |> Async.RunSynchronously
            interactiveLoop ()
        | "q"
        | "Q" -> printfn "Exiting Kafka Client..."
        | _ ->
            printfn "Invalid choice. Please select a valid option."
            interactiveLoop ()

    interactiveLoop ()
    0 // Return an integer exit code
