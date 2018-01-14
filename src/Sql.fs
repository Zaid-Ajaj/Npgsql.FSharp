namespace Npgsql.FSharp

open System
open Npgsql
open Giraffe.Tasks
open System.Threading.Tasks
open System.Data

type Sql = 
    | Int of int
    | String of string
    | Long of int64
    | Date of DateTime
    | Byte of byte
    | Bool of bool
    | Number of double
    | Decimal of decimal
    | Char of char
    | Null
    | Other of obj
    
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
    }


    type SqlProps = private {
        ConnectionString : string
        SqlQuery : string list
        Parameters : SqlRow
        IsFunction : bool
    }

    let private defaultConString() : ConnectionStringBuilder = {
            Host = ""
            Database = ""
            Username = ""
            Password = ""
            Port = 5432
    }
    let private defaultProps() = { 
        ConnectionString = ""; 
        SqlQuery = []; 
        Parameters = [];
        IsFunction = false 
    }

    let connect constr  = { defaultProps() with ConnectionString = constr }
    let host x = { defaultConString() with Host = x }
    let username x con = { con with Username = x }
    let password x con = { con with Password = x }
    let database x con = { con with Database = x }
    let port n con = { con with Port = n }
    let str (con:ConnectionStringBuilder) = 
        sprintf "Host=%s;Username=%s;Password=%s;Database=%s;Port=%d"
            con.Host
            con.Username
            con.Password
            con.Database 
            con.Port

    let query (sql: string) props = { props with SqlQuery = [sql] }
    let func (sql: string) props = { props with SqlQuery = [sql]; IsFunction = true }
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

    let toByte = function
        | Byte x -> x 
        | value -> failwithf "Could not convert %A into a byte" value

    let readValue value = 
        match box value with
        | :? int32 as x -> Int x
        | :? string as x -> String x
        | :? System.DateTime as x -> Date x
        | :? bool as x ->  Bool x
        | :? int64 as x ->  Long x
        | :? byte as x ->  Byte x
        | :? double as x ->  Number x
        | :? decimal as x -> Decimal x
        | :? char as x -> Char x
        | _ -> Other value
        
    let readRow (reader : NpgsqlDataReader) : SqlRow = 
        
        let readFieldSync fieldIndex = 
            let fieldName = reader.GetName(fieldIndex)
            if reader.IsDBNull(fieldIndex) 
            then fieldName, Null
            else fieldName, readValue (reader.GetFieldValue(fieldIndex))

        [0 .. reader.FieldCount - 1]
        |> List.map readFieldSync

    let readRowAsync (reader: NpgsqlDataReader) = 
        let readValueAsync fieldIndex = 
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
        |> List.map readValueAsync
        |> Task.WhenAll

    let readTable (reader: NpgsqlDataReader) : SqlTable = 
        [ while reader.Read() do yield readRow reader ]

    let readTableAsync (reader: NpgsqlDataReader) = 
        
        let rec readRows rows = task {
            let! canRead = reader.ReadAsync()
            if canRead then
              let! row = readRowAsync reader
              return! readRows (List.ofArray row :: rows)
            else 
              return rows 
        }

        readRows []

    // TDODO
    let private populateCmd (cmd: NpgsqlCommand) (props: SqlProps) = 
        if props.IsFunction then
          cmd.CommandType <- CommandType.StoredProcedure
          for param in props.Parameters do 
            let paramValue : obj =
              match snd param with
              | String text -> upcast text
              | Int i -> upcast i
              | Date date -> upcast date
              | Number n -> upcast n
              | Bool b -> upcast b
              | Char x -> upcast x
              | Decimal x -> upcast x
              | Long x -> upcast x
              | Byte x -> upcast x
              | Null -> null
              | _ -> null // TODO

            let paramName = sprintf "@%s" (fst param)
            cmd.Parameters.AddWithValue(paramName, paramValue) |> ignore
        else ()

    let executeTable (props: SqlProps) : SqlTable = 
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
        use connection = new NpgsqlConnection(props.ConnectionString)
        connection.Open()
        use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
        populateCmd command props
        use reader = command.ExecuteReader()
        readTable reader

    let executeTableSafe (props: SqlProps) : Result<SqlTable, exn> = 
        try Ok (executeTable props)
        with | ex -> Error ex

    let executeTableAsync (props: SqlProps) :  Async<SqlTable> = 
      task {
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
        use connection = new NpgsqlConnection(props.ConnectionString)
        do! connection.OpenAsync()
        use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
        do populateCmd command props
        use! reader = command.ExecuteReaderAsync()
        return! readTableAsync (reader |> unbox<NpgsqlDataReader>)
      }
      |> Async.AwaitTask
        
    let multiline xs = String.concat Environment.NewLine xs

    let executeMany (props: SqlProps)  = 
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
        let queryCount = List.length props.SqlQuery
        let singleQuery = String.concat ";" props.SqlQuery
        use connection = new NpgsqlConnection(props.ConnectionString)
        connection.Open()
        use command = new NpgsqlCommand(singleQuery, connection)
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
        populateCmd command props
        command.ExecuteScalar()
        |> readValue

    let executeNonQuery (props: SqlProps) : int =
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
        use connection = new NpgsqlConnection(props.ConnectionString)
        connection.Open()
        use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
        populateCmd command props
        command.ExecuteNonQuery()

    let executeNonQuerySafe (props: SqlProps) : Result<int, exn> =
        try Ok (executeNonQuery props)
        with | ex -> Error ex

    let executeNonQueryAsync (props: SqlProps) : Async<int> = 
        task {
          use connection = new NpgsqlConnection(props.ConnectionString)
          do! connection.OpenAsync()
          use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
          do populateCmd command props
          return! command.ExecuteNonQueryAsync()
        } 
        |> Async.AwaitTask

    let executeNonQuerySafeAsync (props: SqlProps) : Async<Result<int, exn>> = 
        task {
          try  
            use connection = new NpgsqlConnection(props.ConnectionString)
            do! connection.OpenAsync()
            use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
            do populateCmd command props
            let! result = command.ExecuteNonQueryAsync()
            return Ok (result)
          with 
            | ex -> return Error ex 
        } 
        |> Async.AwaitTask

    let executeScalarSafe (props: SqlProps) : Result<Sql, exn> =
        try  Ok (executeScalar props)
        with | ex -> Error ex
            
    let executeScalarAsync (props: SqlProps) : Async<Sql> = 
      task {
        if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
        use connection = new NpgsqlConnection(props.ConnectionString)
        do! connection.OpenAsync()
        use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
        do populateCmd command props
        let! value = command.ExecuteScalarAsync()
        return readValue value
      } 
      |> Async.AwaitTask


    let executeScalarSafeAsync (props: SqlProps) : Async<Result<Sql, exn>> =
      task {
        try
          if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
          use connection = new NpgsqlConnection(props.ConnectionString)
          do! connection.OpenAsync()
          use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
          do populateCmd command props
          let! value = command.ExecuteScalarAsync()
          return Ok (readValue value)
        with
          | ex -> return Error ex
      } 
      |> Async.AwaitTask

    let mapEachRow (f: SqlRow -> Option<'a>) (table: SqlTable) = 
        List.choose f table