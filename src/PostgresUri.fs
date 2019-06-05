[<AutoOpen>]
module PostgresUri

open System

let private extractHost (uri: Uri) =
    if String.IsNullOrWhiteSpace uri.Host
    then Some "Host=localhost"
    else Some (sprintf "Host=%s" uri.Host)

let private extractUser (uri: Uri) =
    if uri.UserInfo.Contains ":" then
      match uri.UserInfo.Split ':' with
      | [| username; password|] ->  Some (sprintf "Username=%s;Password=%s" username password)
      | otherwise -> None
    elif not (String.IsNullOrWhiteSpace uri.UserInfo) then
      Some (sprintf "Username=%s" uri.UserInfo)
    else
      None

let private extractDatabase (uri: Uri) =
    match uri.LocalPath.Split '/' with
    | [| ""; databaseName |] -> Some (sprintf "Database=%s" databaseName)
    | otherwise -> None

let private extractPort (uri: Uri) =
    match uri.Port with
    | -1 -> Some (sprintf "Port=%d" 5432)
    | n -> Some (sprintf "Port=%d" n)

type Uri with
    member uri.ToPostgresConnectionString() : string =
        let parts =  [
            extractHost uri
            extractUser uri
            extractDatabase uri
            extractPort uri
        ]

        String.concat ";" (List.choose id parts)