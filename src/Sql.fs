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

[<RequireQualifiedAccess>]
module Sql =

    type ConnectionStringBuilder = private {
        Host: string
        Database: string
        Username: string
        Password: string
        Port: int
        Config : string
    }


    type SqlProps = private {
        ConnectionString : string
        SqlQuery : string list
        Parameters : SqlRow
        IsFunction : bool
        NeedPrepare : bool
    }

    let private defaultConString() : ConnectionStringBuilder = {
            Host = ""
            Database = ""
            Username = ""
            Password = ""
            Port = 5432
            Config = ""
    }
    let private defaultProps() = {
        ConnectionString = "";
        SqlQuery = [];
        Parameters = [];
        IsFunction = false
        NeedPrepare = false
    }

    let connect constr  = { defaultProps() with ConnectionString = constr }
    let host x = { defaultConString() with Host = x }
    let username x con = { con with Username = x }
    let password x con = { con with Password = x }
    let database x con = { con with Database = x }
    let port n con = { con with Port = n }
    let config x con = { con with Config = x }
    let str (con:ConnectionStringBuilder) =
        sprintf "Host=%s;Username=%s;Password=%s;Database=%s;Port=%d;%s"
            con.Host
            con.Username
            con.Password
            con.Database
            con.Port
            con.Config

    let query (sql: string) props = { props with SqlQuery = [sql] }
    let func (sql: string) props = { props with SqlQuery = [sql]; IsFunction = true }
    let prepare  props = { props with NeedPrepare = true}
    let queryMany queries props = { props with SqlQuery = queries }
    let parameters ls props = { props with Parameters = ls }

    let readInt name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Int value) -> Some value
            | _ -> None

    let readLong name (row: SqlRow)  =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Long value) -> Some value
            | _ -> None

    let readString name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.String value) -> Some value
            | _ -> None

    let readDate name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Date value) -> Some value
            | _ -> None

    let readBool name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Bool value) -> Some value
            | _ -> None

    let readDecimal name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Decimal value) -> Some value
            | _ -> None

    let readNumber name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Number value) -> Some value
            | _ -> None

    let readUuid name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Uuid value) -> Some value
            | _ -> None

    let readBytea name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Bytea value) -> Some value
            | _ -> None

    let readHStore name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.HStore value) -> Some value
            | _ -> None

    let toBool = function
        | SqlValue.Bool x -> x
        | value -> failwithf "Could not convert %A into a boolean value" value

    let toInt = function
        | SqlValue.Int x -> x
        | value -> failwithf "Could not convert %A into an integer" value

    let toLong = function
        | SqlValue.Long x -> x
        | value -> failwithf "Could not convert %A into a long" value

    let toString = function
        | SqlValue.String x -> x
        | value -> failwithf "Could not convert %A into a string" value

    let toDateTime = function
        | SqlValue.Date x -> x
        | value -> failwithf "Could not convert %A into a DateTime" value

    let toFloat = function
        | SqlValue.Number x -> x
        | value -> failwithf "Could not convert %A into a floating number" value

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

    let readValue (columnName: Option<string>) value =
        match box value with
        | :? int16 as x -> SqlValue.Short x
        | :? int32 as x -> SqlValue.Int x
        | :? string as x -> SqlValue.String x
        | :? System.DateTimeOffset as x -> SqlValue.TimeWithTimeZone x
        | :? System.DateTime as x -> SqlValue.Date x
        | :? bool as x ->  SqlValue.Bool x
        | :? int64 as x ->  SqlValue.Long x
        | :? decimal as x -> SqlValue.Decimal x
        | :? double as x ->  SqlValue.Number x
        | :? System.Guid as x -> SqlValue.Uuid x
        | :? array<byte> as xs -> SqlValue.Bytea xs
        | :? IDictionary<string, string> as dict ->
            dict
            |> Seq.map (|KeyValue|)
            |> Map.ofSeq
            |> SqlValue.HStore
        | null -> SqlValue.Null
        | :? System.DBNull -> SqlValue.Null
        | other ->
            let typeName = (other.GetType()).FullName
            match columnName with
            | Some name -> failwithf "Unable to read column '%s' of type '%s'" name typeName
            | None -> failwithf "Unable to read column of type '%s'" typeName

    /// Reads a single row from the data reader synchronously
    let readRow (reader : NpgsqlDataReader) : SqlRow =
        let readFieldSync fieldIndex =
            let fieldName = reader.GetName(fieldIndex)
            if reader.IsDBNull(fieldIndex)
            then fieldName, SqlValue.Null
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
        let rec readRows rows = task {
            let! canRead = reader.ReadAsync(cancellationToken)
            if canRead then
              let! row = readRowTaskCt cancellationToken reader
              return! readRows (List.ofArray row :: rows)
            else
              return rows
        }
        readRows []

    let readTableTask (reader: NpgsqlDataReader) =
        readTableTaskCt CancellationToken.None reader

    let readTableAsync (reader: NpgsqlDataReader) =
        async {
            let! ct = Async.CancellationToken
            return!
                readTableTaskCt ct reader
                |> Async.AwaitTask
        }

    let private populateCmd (cmd: NpgsqlCommand) (props: SqlProps) =
        if props.IsFunction then cmd.CommandType <- CommandType.StoredProcedure

        for (paramName, value) in props.Parameters do
          let paramValue, paramType : (obj * NpgsqlTypes.NpgsqlDbType option) =
            match value with
            | SqlValue.String text -> upcast text, None
            | SqlValue.Int i -> upcast i, None
            | SqlValue.Uuid x -> upcast x, None
            | SqlValue.Short x -> upcast x, None
            | SqlValue.Date date -> upcast date, None
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

          let paramName =
            if not (paramName.StartsWith "@")
            then sprintf "@%s" paramName
            else paramName

          match paramType with
          | Some x -> cmd.Parameters.AddWithValue(paramName, x, paramValue) |> ignore
          | None -> cmd.Parameters.AddWithValue(paramName, paramValue) |> ignore

    let executeTable (props: SqlProps) : SqlTable =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
        use connection = new NpgsqlConnection(props.ConnectionString)
        connection.Open()
        use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
        if props.NeedPrepare then command.Prepare()
        populateCmd command props
        use reader = command.ExecuteReader()
        readTable reader

    let executeTableSafe (props: SqlProps) : Result<SqlTable, exn> =
        try Ok (executeTable props)
        with | ex -> Error ex

    let executeTableTaskCt (cancellationToken : CancellationToken) (props: SqlProps) =
        task {
            if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
            use connection = new NpgsqlConnection(props.ConnectionString)
            do! connection.OpenAsync(cancellationToken)
            use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
            if props.NeedPrepare then command.Prepare()
            do populateCmd command props
            use! reader = command.ExecuteReaderAsync(cancellationToken)
            return! readTableTaskCt cancellationToken (reader |> unbox<NpgsqlDataReader>)
        }
    let executeTableTask (props: SqlProps) =
        executeTableTaskCt CancellationToken.None


    let executeTableAsync (props: SqlProps) : Async<SqlTable> =
        async {
            let! ct = Async.CancellationToken
            return!
                executeTableTaskCt ct props
                |> Async.AwaitTask
        }

    let executeTableSafeTaskCt (cancellationToken : CancellationToken) (props: SqlProps) : Task<Result<SqlTable, exn>> =
        task {
            try
                if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
                use connection = new NpgsqlConnection(props.ConnectionString)
                do! connection.OpenAsync(cancellationToken)
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                if props.NeedPrepare then command.Prepare()
                do populateCmd command props
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

    let multiline xs = String.concat Environment.NewLine xs

    /// Executes multiple queries and returns each result set as a distinct table
    let executeMany (props: SqlProps)  =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
        let queryCount = List.length props.SqlQuery
        let singleQuery = String.concat ";" props.SqlQuery
        use connection = new NpgsqlConnection(props.ConnectionString)
        connection.Open()
        use command = new NpgsqlCommand(singleQuery, connection)
        if props.NeedPrepare then command.Prepare()
        populateCmd command props
        use reader = command.ExecuteReader()
        [ for _ in 1 .. queryCount do
            yield readTable reader
            reader.NextResult() |> ignore ]

    let executeScalar (props: SqlProps) : SqlValue =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute"
        use connection = new NpgsqlConnection(props.ConnectionString)
        connection.Open()
        use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
        if props.NeedPrepare then command.Prepare()
        populateCmd command props
        command.ExecuteScalar()
        |> readValue None

    /// Executes the query and returns the number of rows affected
    let executeNonQuery (props: SqlProps) : int =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
        use connection = new NpgsqlConnection(props.ConnectionString)
        connection.Open()
        use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
        if props.NeedPrepare then command.Prepare()
        populateCmd command props
        command.ExecuteNonQuery()

    /// Executes the query safely (does not throw) and returns the number of rows affected
    let executeNonQuerySafe (props: SqlProps) : Result<int, exn> =
        try Ok (executeNonQuery props)
        with | ex -> Error ex

    /// Executes the query as a task and returns the number of rows affected
    let executeNonQueryTaskCt (cancellationToken : CancellationToken) (props: SqlProps) =
        task {
            use connection = new NpgsqlConnection(props.ConnectionString)
            do! connection.OpenAsync(cancellationToken)
            use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
            if props.NeedPrepare then command.Prepare()
            do populateCmd command props
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
                use connection = new NpgsqlConnection(props.ConnectionString)
                do! connection.OpenAsync(cancellationToken)
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                if props.NeedPrepare then command.Prepare()
                do populateCmd command props
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
            use connection = new NpgsqlConnection(props.ConnectionString)
            do! connection.OpenAsync(cancellationToken)
            use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
            if props.NeedPrepare then command.Prepare()
            do populateCmd command props
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
                use connection = new NpgsqlConnection(props.ConnectionString)
                do! connection.OpenAsync(cancellationToken)
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                if props.NeedPrepare then command.Prepare()
                do populateCmd command props
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