namespace LoggingWrapper

type ILoggingWrapper =
    abstract member LogInfo: message: string -> unit
    abstract member LogWarning: message: string -> unit
    abstract member LogError: message: string * ex: exn -> unit