namespace Npgsql.FSharp

open System
open Npgsql
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

type Sql =
    | Short of int16
    | Int of int
    | Long of int64
    | String of string
    | Date of DateTime
    | Bool of bool
    | Number of double
    | Decimal of decimal
    | Bytea of byte[]
    | HStore of Map<string, string>
    | Uuid of Guid
    | Null
    | Other of obj
    with
        static member toObj = function
            | Short s -> box s
            | Int i -> box i
            | Long l -> box l
            | String s -> box s
            | Date dt -> box dt
            | Bool b -> box b
            | Number d -> box d
            | Decimal d -> box d
            | Bytea b -> box b
            | HStore hs -> box hs
            | Uuid g -> box g
            | Null -> null
            | Other o -> o

        static member toOptionObj = function
            | Short s -> box <| Some s
            | Int i -> box <| Some i
            | Long l -> box <| Some l
            | String s -> box <| Some s
            | Date dt -> box <| Some dt
            | Bool b -> box <| Some b
            | Number d -> box <| Some d
            | Decimal d -> box <| Some d
            | Bytea b -> box <| Some b
            | HStore hs -> box <| Some hs
            | Uuid g -> box <| Some g
            | Null -> box None
            | Other o -> box <| Some o

type SqlRow = list<string * Sql>

type SqlTable = list<SqlRow>

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

    let toBool = function
        | Bool x -> x
        | value -> failwithf "Could not convert %A into a boolean value" value

    let toInt = function
        | Int x -> x
        | value -> failwithf "Could not convert %A into an integer" value

    let toString = function
        | String x -> x
        | value -> failwithf "Could not convert %A into a string" value

    let toDateTime = function
        | Date x -> x
        | value -> failwithf "Could not convert %A into a DateTime" value

    let toFloat = function
        | Number x -> x
        | value -> failwithf "Could not convert %A into a floating number" value

    let readValue value =
        match box value with
        | :? int32 as x -> Int x
        | :? string as x -> String x
        | :? System.DateTime as x -> Date x
        | :? bool as x ->  Bool x
        | :? int64 as x ->  Long x
        | :? decimal as x -> Decimal x
        | :? double as x ->  Number x
        | :? System.Guid as x -> Uuid x
        | :? array<byte> as xs -> Bytea xs
        | :? IDictionary<string, string> as dict ->
            dict
            |> Seq.map (|KeyValue|)
            |> Map.ofSeq
            |> HStore
        | null -> Null
        | _ -> Other value

    let readRow (reader : NpgsqlDataReader) : SqlRow =

        let readFieldSync fieldIndex =

            let fieldName = reader.GetName(fieldIndex)
            if reader.IsDBNull(fieldIndex)
            then fieldName, Null
            else fieldName, readValue (reader.GetFieldValue(fieldIndex))

        [0 .. reader.FieldCount - 1]
        |> List.map readFieldSync

    let readRowTask (reader: NpgsqlDataReader) =
        let readValueTask fieldIndex =
          task {
              let fieldName = reader.GetName fieldIndex
              let! isNull = reader.IsDBNullAsync fieldIndex
              if isNull then
                return fieldName, Null
              else
                let! value = reader.GetFieldValueAsync fieldIndex
                return fieldName, readValue value
          }

        [0 .. reader.FieldCount - 1]
        |> List.map readValueTask
        |> Task.WhenAll

    let readRowAsync (reader: NpgsqlDataReader) =
        readRowTask reader
        |> Async.AwaitTask


    let readTable (reader: NpgsqlDataReader) : SqlTable =
        [ while reader.Read() do yield readRow reader ]

    let readTableTask (reader: NpgsqlDataReader) =
        let rec readRows rows = task {
            let! canRead = reader.ReadAsync()
            if canRead then
              let! row = readRowTask reader
              return! readRows (List.ofArray row :: rows)
            else
              return rows
        }
        readRows []

    let readTableAsync (reader: NpgsqlDataReader) =
        readTableTask reader
        |> Async.AwaitTask

    let private populateCmd (cmd: NpgsqlCommand) (props: SqlProps) =
        if props.IsFunction then cmd.CommandType <- CommandType.StoredProcedure

        for param in props.Parameters do
          let paramValue : obj =
            match snd param with
            | String text -> upcast text
            | Int i -> upcast i
            | Uuid x -> upcast x
            | Short x -> upcast x
            | Date date -> upcast date
            | Number n -> upcast n
            | Bool b -> upcast b
            | Decimal x -> upcast x
            | Long x -> upcast x
            | Bytea x -> upcast x
            | HStore dictionary ->
                let value =
                  dictionary
                  |> Map.toList
                  |> dict
                  |> Dictionary
                upcast value
            | Null -> null
            | Other x -> x


          let paramName = sprintf "@%s" (fst param)
          cmd.Parameters.AddWithValue(paramName, paramValue) |> ignore

    let executeTable (props: SqlProps) : SqlTable =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
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

    let executeTableTask (props: SqlProps) =
        task {
            if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
            use connection = new NpgsqlConnection(props.ConnectionString)
            do! connection.OpenAsync()
            use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
            if props.NeedPrepare then command.Prepare()
            do populateCmd command props
            use! reader = command.ExecuteReaderAsync()
            return! readTableTask (reader |> unbox<NpgsqlDataReader>)
        }

    let executeTableAsync (props: SqlProps) =
        executeTableTask props
        |> Async.AwaitTask

    let executeTableSafeTask (props: SqlProps) =
        task {
            try
                if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
                use connection = new NpgsqlConnection(props.ConnectionString)
                do! connection.OpenAsync()
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                if props.NeedPrepare then command.Prepare()
                do populateCmd command props
                use! reader = command.ExecuteReaderAsync()
                let! result = readTableTask (reader |> unbox<NpgsqlDataReader>)
                return Ok (result)
            with
            | ex -> return Error ex
        }

    let executeTableSafeAsync (props: SqlProps) =
        executeTableSafeTask props
        |> Async.AwaitTask

    let multiline xs = String.concat Environment.NewLine xs

    let executeMany (props: SqlProps)  =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
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

    let executeScalar (props: SqlProps) : Sql =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
        use connection = new NpgsqlConnection(props.ConnectionString)
        connection.Open()
        use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
        if props.NeedPrepare then command.Prepare()
        populateCmd command props
        command.ExecuteScalar()
        |> readValue

    let executeNonQuery (props: SqlProps) : int =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
        use connection = new NpgsqlConnection(props.ConnectionString)
        connection.Open()
        use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
        if props.NeedPrepare then command.Prepare()
        populateCmd command props
        command.ExecuteNonQuery()

    let executeNonQuerySafe (props: SqlProps) : Result<int, exn> =
        try Ok (executeNonQuery props)
        with | ex -> Error ex

    let executeNonQueryTask (props: SqlProps) =
        task {
            use connection = new NpgsqlConnection(props.ConnectionString)
            do! connection.OpenAsync()
            use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
            if props.NeedPrepare then command.Prepare()
            do populateCmd command props
            return! command.ExecuteNonQueryAsync()
        }

    let executeNonQueryAsync  (props: SqlProps) =
        executeNonQueryTask props
        |> Async.AwaitTask

    let executeNonQuerySafeTask (props: SqlProps) =
        task {
            try
                use connection = new NpgsqlConnection(props.ConnectionString)
                do! connection.OpenAsync()
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                if props.NeedPrepare then command.Prepare()
                do populateCmd command props
                let! result = command.ExecuteNonQueryAsync()
                return Ok (result)
            with
            | ex -> return Error ex
        }

    let executeNonQuerySafeAsync (props: SqlProps) =
        executeNonQuerySafeTask props
        |> Async.AwaitTask

    let executeScalarSafe (props: SqlProps) : Result<Sql, exn> =
        try  Ok (executeScalar props)
        with | ex -> Error ex

    let executeScalarTask (props: SqlProps) =
        task {
            if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
            use connection = new NpgsqlConnection(props.ConnectionString)
            do! connection.OpenAsync()
            use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
            if props.NeedPrepare then command.Prepare()
            do populateCmd command props
            let! value = command.ExecuteScalarAsync()
            return readValue value
        }

    let executeScalarAsync (props: SqlProps) =
        executeScalarTask props
        |> Async.AwaitTask


    let executeScalarSafeTask (props: SqlProps) =
        task {
            try
                if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
                use connection = new NpgsqlConnection(props.ConnectionString)
                do! connection.OpenAsync()
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                if props.NeedPrepare then command.Prepare()
                do populateCmd command props
                let! value = command.ExecuteScalarAsync()
                return Ok (readValue value)
            with
            | ex -> return Error ex
        }
    let executeScalarSafeAsync (props: SqlProps) =
        executeScalarSafeTask props
        |> Async.AwaitTask

    let mapEachRow (f: SqlRow -> Option<'a>) (table: SqlTable) =
        List.choose f table

    let parseRow<'a> (row : SqlRow) = 
        let findRowValue isOptional name row =
            match isOptional, List.tryFind (fun (n, _) -> n = name) row with
            | false, None -> failwithf "Missing parameter: %s" name
            | false, Some (_, x) -> Sql.toObj x
            | true, None -> box None
            | true, Some (_, x) -> Sql.toOptionObj x

        if FSharpType.IsRecord typeof<'a>
            then
                let args =
                    FSharpType.GetRecordFields typeof<'a>
                    |> Array.map (fun pi -> row |> findRowValue (Utils.isOption pi) pi.Name)
                Some <| (FSharpValue.MakeRecord(typeof<'a>, args) :?> 'a)
            else None

    let parseEachRow<'a> =
        mapEachRow parseRow<'a>

    