namespace LoggingWrapper

open Serilog

type LoggingWrapper() =

    // Create the Serilog logger configuration
    let logger = LoggerConfiguration().WriteTo.Console().CreateLogger()

    interface ILoggingWrapper with
        member _.LogInfo(message: string) = logger.Information(message)

        member _.LogWarning(message: string) = logger.Warning(message)

        member _.LogError(message: string, ex: exn) = logger.Error(ex, message)
