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

    printfn "Topic '%s' created with %d partitions and replication factor %d." topicName partitions replicationFactor

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
    printfn "Topics listed successfully."

let deleteTopic (kafkaService: IKafkaService) =
    printf "Enter the topic name: "
    let topicName = Console.ReadLine()
    kafkaService.DeleteTopic(topicName) |> Async.RunSynchronously
    printfn "Topic '%s' deletion requested." topicName

let getTopicDetails (kafkaService: IKafkaService) =
    printf "Enter the topic name: "
    kafkaService.GetTopicDetails(Console.ReadLine()) |> Async.RunSynchronously
    printfn "Topic details fetched successfully."

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

    printfn "Messages consumed from topic '%s'." topicName

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

let addBroker (kafkaService: IKafkaService) =
    printf "Enter the broker address: "
    let brokerAddress = Console.ReadLine()
    kafkaService.AddBroker(brokerAddress) |> Async.RunSynchronously
    printfn "Broker '%s' added successfully." brokerAddress


let userActionJoinFunction (action: string) (profile: string) =
    sprintf "Action: %s, Profile: %s" action profile

let showMenu () =
    printfn "\nKafka Client Options:"
    printfn "1. Create a new topic"
    printfn "2. Send a message to a topic"
    printfn "3. List topics"
    printfn "4. Delete a topic"
    printfn "5. Get topic details"
    printfn "6. Consume messages from a topic"
    printfn "7. Start Kafka stream"
    printfn "8. Stop Kafka stream"
    printfn "9. Restart Kafka stream"
    printfn "10. Add a new broker"
    printfn "q. Quit"
    printf "Select an option: "

[<EntryPoint>]
let main argv =
    if (argv.Length > 0) then
        match Int32.TryParse(argv.[0]) with
        | true, num when num > 0 ->
            for i in 1..num do
                printfn $"Starting console app instance {i}..."
        | _ -> printfn "Invalid number of console apps specified."

        printfn $"Starting  {argv} console apps..."
        1
    else
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
                addBroker kafkaService
                interactiveLoop ()
            | "11" ->
                printf "Enter stream topic (e.g., 'user-actions'): "
                let streamTopic = Console.ReadLine()
                printf "Enter table topic (e.g., 'user-profiles'): "
                let tableTopic = Console.ReadLine()
                printf "Enter output topic (e.g., 'user-interactions-enriched'): "
                let outputTopic = Console.ReadLine()

                kafkaStreamsService.JoinStreamWithTable<string, string, string, string>(
                    streamTopic,
                    tableTopic,
                    outputTopic,
                    userActionJoinFunction
                )

                printfn
                    "Stream and table joined on topics '%s' and '%s' with output to '%s'."
                    streamTopic
                    tableTopic
                    outputTopic

                interactiveLoop ()
            | "q"
            | "Q" -> printfn "Exiting Kafka Client..."
            | _ ->
                printfn "Invalid choice. Please select a valid option."
                interactiveLoop ()

        interactiveLoop ()
        0
