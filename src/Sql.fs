namespace Npgsql.FSharp

open System
open Npgsql
open System.Threading
open System.Threading.Tasks
open System.Data
open System.Collections.Generic
open FSharp.Control.Tasks

open System.Reflection
open Microsoft.FSharp.Reflection
open System.Security.Cryptography.X509Certificates

module internal Utils =
    let isOption (p:PropertyInfo) =
        p.PropertyType.IsGenericType &&
        p.PropertyType.GetGenericTypeDefinition() = typedefof<Option<_>>

module Async =
    let map f comp =
        async {
            let! result = comp
            return f result
        }

type Sql() =
    static member Value(value: int) = SqlValue.Int value
    static member Value(value: string) = SqlValue.String value
    static member Value(value: int16) = SqlValue.Short value
    static member Value(value: double) = SqlValue.Number value
    static member Value(value: decimal) = SqlValue.Decimal value
    static member Value(value: int64) = SqlValue.Long value
    static member Value(value: DateTime) = SqlValue.Date value
    static member Value(value: bool) = SqlValue.Bool value
    static member Value(value: DateTimeOffset) = SqlValue.TimeWithTimeZone value
    static member Value(value: Guid) = SqlValue.Uuid value
    static member Value(bytea: byte[]) = SqlValue.Bytea bytea
    static member Value(map: Map<string, string>) = SqlValue.HStore map
    static member Value(value: TimeSpan) = SqlValue.Time value
    static member Value(value : string array) =  SqlValue.StringArray value
    static member Value(value : int array) =  SqlValue.IntArray value
    static member Value(value: int option) = match value with | Some value -> SqlValue.Int value | None -> SqlValue.Null
    static member Value(value: string option) = match value with | Some value -> SqlValue.String value | None -> SqlValue.Null
    static member Value(value: int16 option) = match value with | Some value -> SqlValue.Short value | None -> SqlValue.Null
    static member Value(value: double option) = match value with | Some value -> SqlValue.Number value | None -> SqlValue.Null
    static member Value(value: decimal option) = match value with | Some value -> SqlValue.Decimal value | None -> SqlValue.Null
    static member Value(value: int64 option) = match value with | Some value -> SqlValue.Long value | None -> SqlValue.Null
    static member Value(value: DateTime option) = match value with | Some value -> SqlValue.Date value | None -> SqlValue.Null
    static member Value(value: bool option) = match value with | Some value -> SqlValue.Bool value | None -> SqlValue.Null
    static member Value(value: DateTimeOffset option) = match value with | Some value -> SqlValue.TimeWithTimeZone value | None -> SqlValue.Null
    static member Value(value: Guid option) = match value with | Some value -> SqlValue.Uuid value | None -> SqlValue.Null
    static member Value(value: byte[] option) = match value with | Some value -> SqlValue.Bytea value | None -> SqlValue.Null
    static member Value(map: Map<string, string> option) = match map with | Some map -> SqlValue.HStore map | None -> SqlValue.Null
    static member Value(value: TimeSpan option) = match value with | Some value -> SqlValue.Time value | None -> SqlValue.Null
    static member Value(value: string[] option) = match value with | Some value -> SqlValue.StringArray value | None -> SqlValue.Null
    static member Value(value: int[] option) = match value with | Some value -> SqlValue.IntArray value | None -> SqlValue.Null

/// Specifies how to manage SSL.
[<RequireQualifiedAccess>]
type SslMode =
    /// SSL is disabled. If the server requires SSL, the connection will fail.
    | Disable
    /// Prefer SSL connections if the server allows them, but allow connections without SSL.
    | Prefer
    /// Fail the connection if the server doesn't support SSL.
    | Require
    with
      member this.Serialize() =
        match this with
        | Disable -> "Disable"
        | Prefer -> "Prefer"
        | Require -> "Require"

[<RequireQualifiedAccess>]
module Sql =
    type ConnectionStringBuilder = private {
        Host: string
        Database: string
        Username: string option
        Password: string option
        Port: int option
        Config : string option
        SslMode : SslMode option
        TrustServerCertificate : bool option
        ConvertInfinityDateTime : bool option
    }

    type SqlProps = private {
        ConnectionString : string
        SqlQuery : string list
        Parameters : SqlRow
        IsFunction : bool
        NeedPrepare : bool
        ClientCertificate: X509Certificate option
    }

    let private defaultConString() : ConnectionStringBuilder = {
        Host = ""
        Database = ""
        Username = None
        Password = None
        Port = Some 5432
        Config = None
        SslMode = None
        TrustServerCertificate = None
        ConvertInfinityDateTime = None
    }

    let private defaultProps() = {
        ConnectionString = "";
        SqlQuery = [];
        Parameters = [];
        IsFunction = false
        NeedPrepare = false
        ClientCertificate = None
    }

    let connect constr  = { defaultProps() with ConnectionString = constr }
    let withCert cert props = { props with ClientCertificate = Some cert }
    let host x = { defaultConString() with Host = x }
    let username username config = { config with Username = Some username }
    /// Specifies the password of the user that is logging in into the database server
    let password password config = { config with Password = Some password }
    /// Specifies the database name
    let database x con = { con with Database = x }
    /// Specifies how to manage SSL Mode.
    let sslMode mode config = { config with SslMode = Some mode }
    /// Specifies the port of the database server. If you don't specify the port, the default port of `5432` is used.
    let port port config = { config with Port = Some port }
    let trustServerCertificate value config = { config with TrustServerCertificate = Some value }
    let convertInfinityDateTime value config = { config with ConvertInfinityDateTime = Some value }
    let config extraConfig config = { config with Config = Some extraConfig }
    let str (config:ConnectionStringBuilder) =
        [
            Some (sprintf "Host=%s" config.Host)
            config.Port |> Option.map (sprintf "Port=%d")
            Some (sprintf "Database=%s" config.Database)
            config.Username |> Option.map (sprintf "Username=%s")
            config.Password |> Option.map (sprintf "Password=%s")
            config.SslMode |> Option.map (fun mode -> sprintf "SslMode=%s" (mode.Serialize()))
            config.TrustServerCertificate |> Option.map (sprintf "Trust Server Certificate=%b")
            config.ConvertInfinityDateTime |> Option.map (sprintf "Convert Infinity DateTime=%b")
            config.Config
        ]
        |> List.choose id
        |> String.concat ";"

    let connectFromConfig (connectionConfig: ConnectionStringBuilder) =
        connect (str connectionConfig)
    /// Turns the given postgres Uri into a proper connection string
    let fromUri (uri: Uri) = uri.ToPostgresConnectionString()
    /// Creates initial database connection configration from a the Uri components.
    /// It try to find `Host`, `Username`, `Password`, `Database` and `Port` from the input `Uri`.
    let fromUriToConfig (uri: Uri) =
        let extractHost (uri: Uri) =
            if String.IsNullOrWhiteSpace uri.Host
            then Some ("Host", "localhost")
            else Some ("Host", uri.Host)

        let extractUser (uri: Uri) =
            if uri.UserInfo.Contains ":" then
                match uri.UserInfo.Split ':' with
                | [| username; password|] ->
                  [ ("Username", username); ("Password", password) ]
                | otherwise -> [ ]
            elif not (String.IsNullOrWhiteSpace uri.UserInfo) then
                ["Username", uri.UserInfo ]
            else
                [ ]

        let extractDatabase (uri: Uri) =
            match uri.LocalPath.Split '/' with
            | [| ""; databaseName |] -> Some ("Database", databaseName)
            | otherwise -> None

        let extractPort (uri: Uri) =
            match uri.Port with
            | -1 -> Some ("Port", "5432")
            | n -> Some ("Port", string n)

        let uriParts =
            [ extractHost uri; extractDatabase uri; extractPort uri ]
            |> List.choose id
            |> List.append (extractUser uri)

        let updateConfig config (partName, value) =
            match partName with
            | "Host" -> { config with Host = value }
            | "Username" -> { config with Username = Some value }
            | "Password" -> { config with Password = Some value }
            | "Database" -> { config with Database = value }
            | "Port" -> { config with Port = Some (int value) }
            | otherwise -> config

        (defaultConString(), uriParts)
        ||> List.fold updateConfig

    let query (sql: string) props = { props with SqlQuery = [sql] }
    let func (sql: string) props = { props with SqlQuery = [sql]; IsFunction = true }
    let prepare  props = { props with NeedPrepare = true}
    let queryMany queries props = { props with SqlQuery = queries }
    let parameters ls props = { props with Parameters = ls }

    let newConnection (props: SqlProps): NpgsqlConnection =
        let connection = new NpgsqlConnection(props.ConnectionString)
        match props.ClientCertificate with
        | Some cert ->
            connection.ProvideClientCertificatesCallback <- new ProvideClientCertificatesCallback(fun certs ->
                certs.Add(cert) |> ignore)
        | None -> ()
        connection

    /// Tries to read the column value as an `int` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of integer and when it is not null.
    /// Returns `None` otherwise.
    let readInt (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.Int value) -> Some value
            | _ -> None

    /// Tries to read the column value as an `int64` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `int64` and when it is not null.
    /// Returns `None` otherwise.
    let readLong (columnName: string) (row: SqlRow)  =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.Long value) -> Some value
            | _ -> None

    /// Tries to read the column value as an `string` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `string` and when it is not null.
    /// Returns `None` otherwise.
    let readString (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.String value) -> Some value
            | _ -> None

    /// Tries to read the column value as a `DateTime` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `DateTime` and when it is not null.
    /// Returns `None` otherwise.
    let readDate (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.Date value) -> Some value
            | _ -> None

    /// Tries to read the column value as a `TimeSpan` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `TimeSpan` and when it is not null.
    /// Returns `None` otherwise.
    let readTime (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.Time value) -> Some value
            | _ -> None

    /// Tries to read the column value as a `bool` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `bool` and when it is not null.
    /// Returns `None` otherwise.
    let readBool (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.Bool value) -> Some value
            | _ -> None

    /// Tries to read the column value as a `decimal` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `decimal` and when it is not null.
    /// Returns `None` otherwise. Similar to `Sql.readMoney`.
    let readDecimal (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.Decimal value) -> Some value
            | _ -> None

    /// Tries to read the column value as a `decimal` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `decimal` or `money` and when it is not null.
    /// Returns `None` otherwise.
    let readMoney (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.Decimal value) -> Some value
            | _ -> None

    /// Tries to read the column value as a `double` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `double` and when it is not null.
    /// Returns `None` otherwise.
    let readNumber (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.Number value) -> Some value
            | _ -> None

    /// Tries to read the column value as a `Guid` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `Guid` (type `Uuid` in Postgres) and when it is not null.
    /// Returns `None` otherwise.
    let readUuid (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.Uuid value) -> Some value
            | _ -> None

    /// Tries to read the column value as a `byte array` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `byte array` or `bytea` in Postgres and when it is not null.
    /// Returns `None` otherwise.
    let readBytea (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.Bytea value) -> Some value
            | _ -> None

    /// Tries to read the column value as a `DateTime` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `timestamp` or `timestamp without time zone` and when it is not null.
    /// Returns `None` otherwise.
    /// Alias for `Sql.readTimeWithTimeZone`
    let readTimestamp (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.Timestamp value) -> Some value
            | _ -> None

    /// Tries to read the column value as a `DateTime` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `timestamptz` or `timestamp with time zone` and when it is not null.
    /// Returns `None` otherwise.
    let readTimestampTz (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.TimestampWithTimeZone value) -> Some value
            | _ -> None

    /// Tries to read the column value as a `DateTimeOffset` where the *date* component is dropped off from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `DateTimeOffset` or `time with timezone` in Postgres and when it is not null.
    /// Returns `None` otherwise.
    let readTimeWithTimeZone (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.TimeWithTimeZone value) -> Some value
            | _ -> None

    /// Tries to read the column value as a `Map<string, string>` from a row based on the provided name of the column.
    /// Returns `Some value` when the column exists, when it has the type of `HStore` which is a key-value dictionary in Postgres.
    /// Returns `None` otherwise.
    let readHStore (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.HStore value) -> Some value
            | _ -> None

    let readStringArray (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.StringArray value) -> Some value
            | _ -> None

    let readIntArray (columnName: string) (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = columnName)
        |> Option.map snd
        |> function
            | Some (SqlValue.IntArray value) -> Some value
            | _ -> None

    let toBool = function
        | SqlValue.Bool x -> x
        | value -> failwithf "Could not convert %A into a boolean value" value

    let toInt = function
        | SqlValue.Int x -> x
        | value -> failwithf "Could not convert %A into an integer" value

    let toLong = function
        | SqlValue.Long x -> x
        | SqlValue.Int x -> int64 x
        | value -> failwithf "Could not convert %A into a long" value

    let toString = function
        | SqlValue.String x -> x
        | SqlValue.Int x -> string x
        | SqlValue.Long x -> string x
        | value -> failwithf "Could not convert %A into a string" value

    let toDateTime = function
        | SqlValue.Date x -> x
        | SqlValue.Timestamp x -> x
        | SqlValue.TimestampWithTimeZone x -> x
        | value -> failwithf "Could not convert %A into a DateTime" value

    let toTime = function
        | SqlValue.Time x -> x
        | value -> failwithf "Could not convert %A into a TimeSpan" value

    let toFloat = function
        | SqlValue.Number x -> x
        | value -> failwithf "Could not convert %A into a floating number" value

    let toStringArray = function
        | SqlValue.StringArray x -> x
        | value -> failwithf "Could not convert %A into a string array" value

    let (|NullInt|_|) = function
        | SqlValue.Null -> None
        | SqlValue.Int value -> Some value
        | _ -> None

    let (|NullShort|_|) = function
        | SqlValue.Null -> None
        | SqlValue.Short value -> Some value
        | _ -> None

    let (|NullLong|_|) = function
        | SqlValue.Null -> None
        | SqlValue.Long value -> Some value
        | _ -> None

    let (|NullDate|_|) = function
        | SqlValue.Null -> None
        | SqlValue.Date value -> Some value
        | _ -> None

    let (|NullBool|_|) = function
        | SqlValue.Null -> None
        | SqlValue.Bool value -> Some value
        | _ -> None

    let (|NullTimeWithTimeZone|_|) = function
        | SqlValue.Null -> None
        | SqlValue.TimeWithTimeZone value -> Some value
        | _ -> None

    let (|NullDecimal|_|) = function
        | SqlValue.Null -> None
        | SqlValue.Decimal value -> Some value
        | _ -> None

    let (|NullBytea|_|) = function
        | SqlValue.Null -> None
        | SqlValue.Bytea value -> Some value
        | _ -> None

    let (|NullHStore|_|) = function
        | SqlValue.Null -> None
        | SqlValue.HStore value -> Some value
        | _ -> None

    let (|NullUuid|_|) = function
        | SqlValue.Null -> None
        | SqlValue.Uuid value -> Some value
        | _ -> None

    let (|NullNumber|_|) = function
        | SqlValue.Null -> None
        | SqlValue.Number value -> Some value
        | _ -> None

    let (|NullStringArray|_|) = function
        | SqlValue.Null -> None
        | SqlValue.StringArray value -> Some value
        | _ -> None

    let (|NullIntArray|_|) = function
        | SqlValue.Null -> None
        | SqlValue.IntArray value -> Some value
        | _ -> None

    let readValue (columnName: Option<string>) value =
        match box value with
        | :? int16 as x -> SqlValue.Short x
        | :? int32 as x -> SqlValue.Int x
        | :? string as x -> SqlValue.String x
        | :? DateTime as x -> SqlValue.Timestamp x
        | :? bool as x ->  SqlValue.Bool x
        | :? int64 as x ->  SqlValue.Long x
        | :? decimal as x -> SqlValue.Decimal x
        | :? double as x ->  SqlValue.Number x
        | :? Guid as x -> SqlValue.Uuid x
        | :? array<byte> as xs -> SqlValue.Bytea xs
        | :? IDictionary<string, string> as dict ->
            dict
            |> Seq.map (|KeyValue|)
            |> Map.ofSeq
            |> SqlValue.HStore
        | null -> SqlValue.Null
        | :? System.DBNull -> SqlValue.Null
        | :? TimeSpan as x -> SqlValue.Time x
        | :? array<string> as x -> SqlValue.StringArray x
        | :? array<int> as x -> SqlValue.IntArray x
        | other ->
            let typeName = (other.GetType()).FullName
            match columnName with
            | Some name -> failwithf "Unable to read column '%s' of type '%s'" name typeName
            | None -> failwithf "Unable to read column of type '%s'" typeName

    /// Reads a single row from the data reader synchronously
    let readRow (reader : Npgsql.NpgsqlDataReader) : SqlRow =
        let readFieldSync fieldIndex =
            let fieldName = reader.GetName(fieldIndex)
            let typeName = reader.GetDataTypeName(fieldIndex)
            if reader.IsDBNull(fieldIndex)
            then fieldName, SqlValue.Null
            elif (typeName = "timestamptz" || typeName = "timestamp with time zone")
            then fieldName, SqlValue.TimestampWithTimeZone(reader.GetFieldValue<DateTime>(fieldIndex))
            elif (typeName = "timestamp" || typeName = "timestamp without time zone")
            then fieldName, SqlValue.Timestamp(reader.GetFieldValue<DateTime>(fieldIndex))
            elif (typeName = "time" || typeName = "time without time zone")
            then fieldName, SqlValue.Time(reader.GetFieldValue<TimeSpan>(fieldIndex))
            elif (typeName = "timetz" || typeName = "time with time zone")
            then fieldName, SqlValue.TimeWithTimeZone(reader.GetFieldValue<DateTimeOffset>(fieldIndex))
            elif typeName = "date"
            then fieldName, SqlValue.Date(reader.GetFieldValue<DateTime>(fieldIndex))
            else fieldName, readValue (Some fieldName) (reader.GetFieldValue(fieldIndex))
        [0 .. reader.FieldCount - 1]
        |> List.map readFieldSync

    /// Reads a single row from the data reader asynchronously
    let readRowTaskCt (cancellationToken : CancellationToken) (reader: NpgsqlDataReader) =
        let readValueTask fieldIndex =
          task {
              let fieldName = reader.GetName fieldIndex
              let! isNull = reader.IsDBNullAsync(fieldIndex,cancellationToken)
              if isNull then
                return fieldName, SqlValue.Null
              else
                let! value = reader.GetFieldValueAsync(fieldIndex,cancellationToken)
                return fieldName, readValue (Some fieldName) value
          }

        [0 .. reader.FieldCount - 1]
        |> List.map readValueTask
        |> Task.WhenAll

    /// Reads a single row from the data reader asynchronously
    let readRowTask (reader: NpgsqlDataReader) =
        readRowTaskCt CancellationToken.None reader

    /// Reads a single row from the data reader asynchronously
    let readRowAsync (reader: NpgsqlDataReader) =
        async {
            let! ct = Async.CancellationToken
            return!
                readRowTaskCt ct reader
                |> Async.AwaitTask
        }

    let readTable (reader: NpgsqlDataReader) : SqlTable =
        [ while reader.Read() do yield readRow reader ]

    let readTableTaskCt (cancellationToken : CancellationToken) (reader: NpgsqlDataReader) =
        task {
            let rows = ResizeArray<_>()
            let canRead = ref true
            while !canRead do
                let! readerAvailable = reader.ReadAsync(cancellationToken)
                canRead := readerAvailable

                if readerAvailable then
                    let! row = readRowTaskCt cancellationToken reader
                    rows.Add (List.ofArray row)
                else
                    ()

            return List.ofArray (rows.ToArray())
        }

    let readTableTask (reader: NpgsqlDataReader) : Task<SqlTable> =
        readTableTaskCt CancellationToken.None reader

    let readTableAsync (reader: NpgsqlDataReader) : Async<SqlTable> =
        async {
            let! ct = Async.CancellationToken
            return! Async.AwaitTask (readTableTaskCt ct reader)
        }

    let private populateRow (cmd: NpgsqlCommand) (row: SqlRow) =
        for (paramName, value) in row do
          let paramValue, paramType : (obj * NpgsqlTypes.NpgsqlDbType option) =
            match value with
            | SqlValue.String text -> upcast text, None
            | SqlValue.Int i -> upcast i, None
            | SqlValue.Uuid x -> upcast x, None
            | SqlValue.Short x -> upcast x, None
            | SqlValue.Date date -> upcast date, None
            | SqlValue.Timestamp x -> upcast x, None
            | SqlValue.TimestampWithTimeZone x -> upcast x, None
            | SqlValue.Number n -> upcast n, None
            | SqlValue.Bool b -> upcast b, None
            | SqlValue.Decimal x -> upcast x, None
            | SqlValue.Long x -> upcast x, None
            | SqlValue.Bytea x -> upcast x, None
            | SqlValue.TimeWithTimeZone x -> upcast x, None
            | SqlValue.Null -> upcast System.DBNull.Value, None
            | SqlValue.HStore dictionary ->
                let value =
                  dictionary
                  |> Map.toList
                  |> dict
                  |> Dictionary
                upcast value, None
            | SqlValue.Jsonb x -> upcast x, Some NpgsqlTypes.NpgsqlDbType.Jsonb
            | SqlValue.Time x -> upcast x, None
            | SqlValue.StringArray x -> upcast x, Some (NpgsqlTypes.NpgsqlDbType.Array ||| NpgsqlTypes.NpgsqlDbType.Text )
            | SqlValue.IntArray x -> upcast x, Some (NpgsqlTypes.NpgsqlDbType.Array ||| NpgsqlTypes.NpgsqlDbType.Integer )

          let paramName =
            if not (paramName.StartsWith "@")
            then sprintf "@%s" paramName
            else paramName

          match paramType with
          | Some x -> cmd.Parameters.AddWithValue(paramName, x, paramValue) |> ignore
          | None -> cmd.Parameters.AddWithValue(paramName, paramValue) |> ignore

    let private populateCmd (cmd: NpgsqlCommand) (props: SqlProps) =
        if props.IsFunction then cmd.CommandType <- CommandType.StoredProcedure
        populateRow cmd props.Parameters

    [<Obsolete "Sql.executeTable will be removed in the next major release because it creates an intermediate list of parsed values. Use Sql.executeReader instead to lower the memory footprint.">]
    let executeTable (props: SqlProps) : SqlTable =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
        use connection = newConnection props
        connection.Open()
        use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
        populateCmd command props
        if props.NeedPrepare then command.Prepare()
        use reader = command.ExecuteReader()
        readTable reader

    [<Obsolete "Sql.executeTableSafe will be removed in the next major release because it creates an intermediate list of parsed values. Use Sql.executeReader instead to lower the memory footprint.">]
    let executeTableSafe (props: SqlProps) : Result<SqlTable, exn> =
        try Ok (executeTable props)
        with | ex -> Error ex

    [<Obsolete "Sql.executeTableTaskCt will be removed in the next major release because it creates an intermediate list of parsed values. Use Sql.executeReader instead to lower the memory footprint.">]
    let executeTableTaskCt (cancellationToken : CancellationToken) (props: SqlProps) =
        task {
            if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
            use connection = newConnection props
            do! connection.OpenAsync(cancellationToken)
            use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
            do populateCmd command props
            if props.NeedPrepare then command.Prepare()
            use! reader = command.ExecuteReaderAsync(cancellationToken)
            return! readTableTaskCt cancellationToken (reader |> unbox<NpgsqlDataReader>)
        }

    [<Obsolete "Sql.executeTableTask will be removed in the next major release because it creates an intermediate list of parsed values. Use Sql.executeReader instead to lower the memory footprint.">]
    let executeTableTask (props: SqlProps) =
        executeTableTaskCt CancellationToken.None

    [<Obsolete "Sql.executeTableAsync will be removed in the next major release because it creates an intermediate list of parsed values. Use Sql.executeReader instead to lower the memory footprint.">]
    let executeTableAsync (props: SqlProps) : Async<SqlTable> =
        async {
            let! ct = Async.CancellationToken
            return!
                executeTableTaskCt ct props
                |> Async.AwaitTask
        }

    let executeTransaction queries (props: SqlProps)  =
        if List.isEmpty queries
        then [ ]
        else
        use connection = newConnection props
        connection.Open()
        use transaction = connection.BeginTransaction()
        let affectedRowsByQuery = ResizeArray<int>()
        for (query, parameterSets) in queries do
            if List.isEmpty parameterSets
            then
               use command = new NpgsqlCommand(query, connection, transaction)
               let affectedRows = command.ExecuteNonQuery()
               affectedRowsByQuery.Add affectedRows
            else
              for parameterSet in parameterSets do
                use command = new NpgsqlCommand(query, connection, transaction)
                populateRow command parameterSet
                let affectedRows = command.ExecuteNonQuery()
                affectedRowsByQuery.Add affectedRows

        transaction.Commit()
        List.ofSeq affectedRowsByQuery

    let executeTransactionAsync queries (props: SqlProps)  =
        async {
            let! token = Async.CancellationToken
            if List.isEmpty queries
            then return [ ]
            else
            use connection = newConnection props
            do! Async.AwaitTask (connection.OpenAsync token)
            use transaction = connection.BeginTransaction()
            let affectedRowsByQuery = ResizeArray<int>()
            for (query, parameterSets) in queries do
                if List.isEmpty parameterSets
                then
                  use command = new NpgsqlCommand(query, connection, transaction)
                  let! affectedRows = Async.AwaitTask (command.ExecuteNonQueryAsync token)
                  affectedRowsByQuery.Add affectedRows
                else
                  for parameterSet in parameterSets do
                    use command = new NpgsqlCommand(query, connection, transaction)
                    populateRow command parameterSet
                    let! affectedRows = Async.AwaitTask (command.ExecuteNonQueryAsync token)
                    affectedRowsByQuery.Add affectedRows
            do! Async.AwaitTask(transaction.CommitAsync token)
            return List.ofSeq affectedRowsByQuery
        }

    let executeTransactionSafeAsync queries (props: SqlProps)  =
        async {
            let! result = Async.Catch (executeTransactionAsync queries props)
            match result with
            | Choice1Of2 affectedRows -> return Ok affectedRows
            | Choice2Of2 ex -> return Error ex
        }

    let executeTransactionSafe queries (props: SqlProps) =
        try
            if List.isEmpty queries
            then Ok [ ]
            else
            use connection = newConnection props
            connection.Open()
            use transaction = connection.BeginTransaction()
            let affectedRowsByQuery = ResizeArray<int>()
            for (query, parameterSets) in queries do
                if List.isEmpty parameterSets
                then
                   use command = new NpgsqlCommand(query, connection, transaction)
                   let affectedRows = command.ExecuteNonQuery()
                   affectedRowsByQuery.Add affectedRows
                else
                  for parameterSet in parameterSets do
                      use command = new NpgsqlCommand(query, connection, transaction)
                      populateRow command parameterSet
                      let affectedRows = command.ExecuteNonQuery()
                      affectedRowsByQuery.Add affectedRows
            transaction.Commit()
            Ok (List.ofSeq affectedRowsByQuery)
        with
        | ex -> Error ex

    let executeReader (read: NpgsqlDataReader -> Option<'t>) (props: SqlProps) : 't list =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
        use connection = newConnection props
        connection.Open()
        use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
        do populateCmd command props
        if props.NeedPrepare then command.Prepare()
        use reader = command.ExecuteReader()
        let postgresReader = unbox<NpgsqlDataReader> reader
        let result = ResizeArray<'t option>()
        while reader.Read() do result.Add (read postgresReader)
        List.choose id (List.ofSeq result)

    let executeReaderSafe (read: NpgsqlDataReader -> Option<'t>) (props: SqlProps)  =
        try
          if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
          use connection = newConnection props
          connection.Open()
          use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
          do populateCmd command props
          if props.NeedPrepare then command.Prepare()
          use reader = command.ExecuteReader()
          let postgresReader = unbox<NpgsqlDataReader> reader
          let result = ResizeArray<'t option>()
          while reader.Read() do result.Add (read postgresReader)
          Ok (List.choose id (List.ofSeq result))
        with
        | ex -> Error ex

    let executeReaderTaskCt (cancellationToken : CancellationToken) (props: SqlProps) (read: NpgsqlDataReader -> Option<'t>) : Task<'t list> =
        task {
            if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
            use connection = newConnection props
            do! connection.OpenAsync(cancellationToken)
            use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
            do populateCmd command props
            if props.NeedPrepare then command.Prepare()
            use! reader = command.ExecuteReaderAsync(cancellationToken)
            let postgresReader = unbox<NpgsqlDataReader> reader
            let result = ResizeArray<'t option>()
            let canRead = ref true
            while !canRead do
                let! readMore = reader.ReadAsync cancellationToken
                canRead := readMore
                if !canRead then result.Add (read postgresReader)

            return List.choose id (List.ofSeq result)
        }

    let executeReaderTask (read: NpgsqlDataReader -> Option<'t>) (props: SqlProps) =
        executeReaderTaskCt CancellationToken.None props read

    let executeReaderAsync (read: NpgsqlDataReader -> Option<'t>) (props: SqlProps) =
        async {
            let! token = Async.CancellationToken
            return!
                executeReaderTaskCt token props read
                |> Async.AwaitTask
        }

    let executeReaderSafeTaskCt (cancellationToken : CancellationToken) (read: NpgsqlDataReader -> Option<'t>) (props: SqlProps) : Task<Result<'t list, exn>> =
        task {
            try
                if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
                use connection = newConnection props
                do! connection.OpenAsync(cancellationToken)
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                use! reader = command.ExecuteReaderAsync(cancellationToken)
                let postgresReader = unbox<NpgsqlDataReader> reader
                let result = ResizeArray<'t option>()
                let canRead = ref true
                while !canRead do
                    let! readMore = reader.ReadAsync cancellationToken
                    canRead := readMore
                    if !canRead then result.Add (read postgresReader)

                return Ok (List.choose id (List.ofSeq result))
            with
            | ex -> return Error ex
        }

    let executeReaderSafeTask read (props: SqlProps)  =
        executeReaderSafeTaskCt CancellationToken.None read props

    let executeReaderSafeAsync read (props: SqlProps)   =
        async {
            let! token = Async.CancellationToken
            let! readerResult = Async.AwaitTask (executeReaderSafeTaskCt token read props)
            return readerResult
        }

    let executeTableSafeTaskCt (cancellationToken : CancellationToken) (props: SqlProps) : Task<Result<SqlTable, exn>> =
        task {
            try
                if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
                use connection = newConnection props
                do! connection.OpenAsync(cancellationToken)
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                use! reader = command.ExecuteReaderAsync(cancellationToken)
                let! result = readTableTaskCt cancellationToken (reader |> unbox<NpgsqlDataReader>)
                return Ok (result)
            with
            | ex -> return Error ex
        }

    let executeTableSafeTask (props: SqlProps) : Task<Result<SqlTable, exn>> =
        executeTableSafeTaskCt CancellationToken.None props

    let executeTableSafeAsync (props: SqlProps) : Async<Result<SqlTable, exn>> =
        async {
            let! ct = Async.CancellationToken
            return!
                executeTableSafeTaskCt ct props
                |> Async.AwaitTask
        }

    let private valueAsObject = function
    | SqlValue.Short s -> box s
    | SqlValue.Int i -> box i
    | SqlValue.Long l -> box l
    | SqlValue.String s -> box s
    | SqlValue.Date dt -> box dt
    | SqlValue.Bool b -> box b
    | SqlValue.Number d -> box d
    | SqlValue.Decimal d -> box d
    | SqlValue.Bytea b -> box b
    | SqlValue.HStore hs -> box hs
    | SqlValue.Uuid g -> box g
    | SqlValue.TimeWithTimeZone g -> box g
    | SqlValue.Null -> null
    | SqlValue.Jsonb s -> box s
    | SqlValue.Time t -> box t
    | SqlValue.Timestamp value -> box value
    | SqlValue.TimestampWithTimeZone value -> box value
    | SqlValue.StringArray value -> box value
    | SqlValue.IntArray value -> box value

    let private valueAsOptionalObject = function
    | SqlValue.Short value -> box (Some value)
    | SqlValue.Int value -> box (Some value)
    | SqlValue.Long value -> box (Some value)
    | SqlValue.String value -> box (Some value)
    | SqlValue.Date value -> box (Some value)
    | SqlValue.Bool value -> box (Some value)
    | SqlValue.Number value -> box (Some value)
    | SqlValue.Decimal value -> box (Some value)
    | SqlValue.Bytea value -> box (Some value)
    | SqlValue.HStore value -> box (Some value)
    | SqlValue.Uuid value -> box (Some value)
    | SqlValue.TimeWithTimeZone value -> box (Some value)
    | SqlValue.Null -> box (None)
    | SqlValue.Jsonb value -> box (Some value)
    | SqlValue.Time value -> box (Some value)
    | SqlValue.Timestamp value -> box (Some value)
    | SqlValue.TimestampWithTimeZone value -> box (Some value)
    | SqlValue.StringArray value -> box (Some value)
    | SqlValue.IntArray value -> box (Some value)

    let multiline xs = String.concat Environment.NewLine xs

    /// Executes multiple queries and returns each result set as a distinct table
    let executeMany (props: SqlProps)  =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
        let queryCount = List.length props.SqlQuery
        let singleQuery = String.concat ";" props.SqlQuery
        use connection = newConnection props
        connection.Open()
        use command = new NpgsqlCommand(singleQuery, connection)
        populateCmd command props
        if props.NeedPrepare then command.Prepare()
        use reader = command.ExecuteReader()
        [ for _ in 1 .. queryCount do
            yield readTable reader
            reader.NextResult() |> ignore ]

    let executeScalar (props: SqlProps) : SqlValue =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
        use connection = newConnection props
        connection.Open()
        use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
        populateCmd command props
        if props.NeedPrepare then command.Prepare()
        command.ExecuteScalar()
        |> readValue None

    /// Executes the query and returns the number of rows affected
    let executeNonQuery (props: SqlProps) : int =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
        use connection = newConnection props
        connection.Open()
        use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
        populateCmd command props
        if props.NeedPrepare then command.Prepare()
        command.ExecuteNonQuery()

    /// Executes the query safely (does not throw) and returns the number of rows affected
    let executeNonQuerySafe (props: SqlProps) : Result<int, exn> =
        try Ok (executeNonQuery props)
        with | ex -> Error ex

    /// Executes the query as a task and returns the number of rows affected
    let executeNonQueryTaskCt (cancellationToken : CancellationToken) (props: SqlProps) =
        task {
            use connection = newConnection props
            do! connection.OpenAsync(cancellationToken)
            use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
            do populateCmd command props
            if props.NeedPrepare then command.Prepare()
            return! command.ExecuteNonQueryAsync(cancellationToken)
        }

    /// Executes the query as a task and returns the number of rows affected
    let executeNonQueryTask (props: SqlProps) =
        executeNonQueryTaskCt CancellationToken.None props

    /// Executes the query as asynchronously and returns the number of rows affected
    let executeNonQueryAsync  (props: SqlProps) =
        async {
            let! ct = Async.CancellationToken
            return!
                executeNonQueryTaskCt ct props
                |> Async.AwaitTask
        }

    /// Executes the query safely as task (does not throw) and returns the number of rows affected
    let executeNonQuerySafeTaskCt (cancellationToken : CancellationToken) (props: SqlProps) =
        task {
            try
                use connection = newConnection props
                do! connection.OpenAsync(cancellationToken)
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                let! result = command.ExecuteNonQueryAsync(cancellationToken)
                return Ok (result)
            with
            | ex -> return Error ex
        }

    /// Executes the query safely as task (does not throw) and returns the number of rows affected
    let executeNonQuerySafeTask (props: SqlProps) =
        executeNonQuerySafeTaskCt CancellationToken.None props

    /// Executes the query safely asynchronously (does not throw) and returns the number of rows affected
    let executeNonQuerySafeAsync (props: SqlProps) =
        async {
            let! ct = Async.CancellationToken
            return!
                executeNonQuerySafeTaskCt ct props
                |> Async.AwaitTask
        }

    /// Executes the query and returns a scalar value safely (does not throw)
    let executeScalarSafe (props: SqlProps) =
        try  Ok (executeScalar props)
        with | ex -> Error ex


    let executeScalarTaskCt (cancellationToken : CancellationToken)  (props: SqlProps) =
        task {
            if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
            use connection = newConnection props
            do! connection.OpenAsync(cancellationToken)
            use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
            do populateCmd command props
            if props.NeedPrepare then command.Prepare()
            let! value = command.ExecuteScalarAsync(cancellationToken)
            return readValue None value
        }
    let executeScalarTask (props: SqlProps) =
        executeScalarTaskCt CancellationToken.None props

    let executeScalarAsync (props: SqlProps) =
        async {
            let! ct = Async.CancellationToken
            return!
                executeScalarTaskCt ct props
                |> Async.AwaitTask
        }

    let executeScalarSafeTaskCt (cancellationToken : CancellationToken) (props: SqlProps) =
        task {
            try
                if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
                use connection = newConnection props
                do! connection.OpenAsync(cancellationToken)
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                let! value = command.ExecuteScalarAsync(cancellationToken)
                return Ok (readValue None value)
            with
            | ex -> return Error ex
        }
    let executeScalarSafeTask (props: SqlProps) =
        executeScalarSafeTaskCt CancellationToken.None props

    let executeScalarSafeAsync (props: SqlProps) =
        async {
            let! ct = Async.CancellationToken
            return!
                executeScalarSafeTaskCt ct props
                |> Async.AwaitTask
        }

    /// Executes multiple queries and returns each result set as a distinct table
    let executeManyTaskCt (cancellationToken : CancellationToken) (props: SqlProps)  =
        task {
            if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
            let singleQuery = String.concat ";" props.SqlQuery
            use connection = newConnection props
            do! connection.OpenAsync()
            use command = new NpgsqlCommand(singleQuery, connection)
            populateCmd command props
            if props.NeedPrepare then command.Prepare()
            use! reader = command.ExecuteReaderAsync()
            let pgreader = reader :?> NpgsqlDataReader
            let rec loop acc = task {
                let acc = readTable pgreader::acc
                let! rest = pgreader.NextResultAsync()
                if rest then
                    return! loop acc
                else
                    return List.rev acc

            }
            return! loop []
        }

    let executeManyTask (props: SqlProps) =
        executeManyTaskCt CancellationToken.None props

    let executeManyAsync (props: SqlProps) =
        async {
            let! ct = Async.CancellationToken
            return!
                executeManyTaskCt ct props
                |> Async.AwaitTask
        }

    let mapEachRow (f: SqlRow -> Option<'a>) (table: SqlTable) =
        List.choose f table

    let parseRow<'a> (row : SqlRow) =
        let findRowValue isOptional name row =
            match isOptional, List.tryFind (fun (n, _) -> n = name) row with
            | _, None -> failwithf "Missing parameter: %s" name
            | false, Some (_, x) -> valueAsObject x
            | true, Some (_, x) -> valueAsOptionalObject x

        if FSharpType.IsRecord typeof<'a>
            then
                let args =
                    FSharpType.GetRecordFields typeof<'a>
                    |> Array.map (fun propInfo ->  findRowValue (Utils.isOption propInfo) propInfo.Name row)
                Some <| (FSharpValue.MakeRecord(typeof<'a>, args) :?> 'a)
            else None

    let parseEachRow<'a> =
        mapEachRow parseRow<'a>