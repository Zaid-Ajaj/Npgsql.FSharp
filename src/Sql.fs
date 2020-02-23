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

    let sqlMap (option: 'a option) (f: 'a -> SqlValue) : SqlValue =
        Option.defaultValue SqlValue.Null (Option.map f option)

module Async =
    let map f comp =
        async {
            let! result = comp
            return f result
        }

type Sql() =
    static member int(value: int) = SqlValue.Int value
    static member intOrNull(value: int option) = Utils.sqlMap value Sql.int
    static member string(value: string) = SqlValue.String (if isNull value then String.Empty else value)
    static member stringOrNull(value: string option) = Utils.sqlMap value Sql.string
    static member text(value: string) = SqlValue.String value
    static member textOrNull(value: string option) = Sql.stringOrNull value
    static member bit(value: bool) = SqlValue.Bit value
    static member bitOrNull(value: bool option) = Utils.sqlMap value Sql.bit
    static member bool(value: bool) = SqlValue.Bool value
    static member boolOrNull(value: bool option) = Utils.sqlMap value Sql.bool
    static member double(value: double) = SqlValue.Number value
    static member doubleOrNull(value: double option) = Utils.sqlMap value Sql.double
    static member decimal(value: decimal) = SqlValue.Decimal value
    static member decimalOrNull(value: decimal option) = Utils.sqlMap value Sql.decimal
    static member money(value: decimal) = SqlValue.Decimal value
    static member moneyOrNull(value: decimal option) = Sql.decimalOrNull value
    static member int8(value: int8) = SqlValue.TinyInt value
    static member int8OrNull(value: int8 option) = Utils.sqlMap value Sql.int8
    static member int16(value: int16) = SqlValue.Short value
    static member int16OrNull(value: int16 option) = Utils.sqlMap value Sql.int16
    static member int64(value: int64) = SqlValue.Long value
    static member int64OrNull(value: int64 option) = Utils.sqlMap value Sql.int64
    static member timestamp(value: DateTime) = SqlValue.Timestamp value
    static member timestampOrNull(value: DateTime option) = Utils.sqlMap value Sql.timestamp
    static member timestamptz(value: DateTime) = SqlValue.TimestampWithTimeZone value
    static member timestamptzOrNull(value: DateTime option) = Utils.sqlMap value Sql.timestamptz
    static member uuid(value: Guid) = SqlValue.Uuid value
    static member uuidOrNull(value: Guid option) = Utils.sqlMap value Sql.uuid
    static member bytea(value: byte[]) = SqlValue.Bytea value
    static member byteaOrNull(value: byte[] option) = Utils.sqlMap value Sql.bytea
    static member stringArray(value: string[]) = SqlValue.StringArray value
    static member stringArrayOrNull(value: string[] option) = Utils.sqlMap value Sql.stringArray
    static member intArray(value: int[]) = SqlValue.IntArray value
    static member intArrayOrNull(value: int[] option) = Utils.sqlMap value Sql.intArray
    static member dbnull = SqlValue.Null

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

type RowReader(reader: NpgsqlDataReader) =
    let columnDict = Dictionary<string, int>()
    let columnTypes = Dictionary<string, string>()
    do
        // Populate the names of the columns into a dictionary
        // such that each read doesn't need to loop through all columns
        for fieldIndex in [0 .. reader.FieldCount - 1] do
            columnDict.Add(reader.GetName(fieldIndex), fieldIndex)
            columnTypes.Add(reader.GetName(fieldIndex), reader.GetDataTypeName(fieldIndex))

    let failToRead (column: string) (columnType: string) =
        let availableColumns =
            columnDict.Keys
            |> Seq.map (fun key -> sprintf "[%s:%s]" key columnTypes.[key])
            |> String.concat ", "

        failwithf "Could not read column '%s' as %s. Available columns are %s"  column columnType availableColumns
    with

    member this.int(column: string) : int =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetInt32(columnIndex)
        | false, _ -> failToRead column "int"

    member this.intOrNull(column: string) : int option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetInt32(columnIndex))
        | false, _ -> failToRead column "int"

    /// Gets the value of the specified column as a 16-bit signed integer.
    ///
    /// Can be used to read columns of type `smallint` or `int16`
    member this.int16(column: string) : int16 =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetInt16(columnIndex)
        | false, _ -> failToRead column "int16"

    member this.int16OrNull(column: string) : int16 option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetInt16(columnIndex))
        | false, _ -> failToRead column "int16"

    member this.intArray(column: string) : int[] =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<int[]>(columnIndex)
        | false, _ -> failToRead column "int[]"

    member this.intArrayOrNull(column: string) : int[] option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFieldValue<int[]>(columnIndex))
        | false, _ -> failToRead column "int[]"

    member this.stringArray(column: string) : string[] =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<string[]>(columnIndex)
        | false, _ -> failToRead column "string[]"

    member this.stringArrayOrNull(column: string) : string[] option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFieldValue<string[]>(columnIndex))
        | false, _ -> failToRead column "string[]"

    member this.int64(column: string) : int64 =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetInt64(columnIndex)
        | false, _ -> failToRead column "int64"

    member this.int64OrNull(column: string) : int64 option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetInt64(columnIndex))
        | false, _ -> failToRead column "int64"

    member this.string(column: string) : string =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetString(columnIndex)
        | false, _ -> failToRead column "string"

    member this.stringOrNull(column: string) : string option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetString(columnIndex))
        | false, _ -> failToRead column "string"

    member this.text(column: string) : string = this.string column
    member this.textOrNull(column: string) : string option = this.stringOrNull column

    member this.bool(column: string) : bool =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<bool>(columnIndex)
        | false, _ -> failToRead column "bool"

    member this.boolOrNull(column: string) : bool option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFieldValue<bool>(columnIndex))
        | false, _ -> failToRead column "bool"

    member this.decimal(column: string) : decimal =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetDecimal(columnIndex)
        | false, _ -> failToRead column "decimal"

    member this.decimalOrNull(column: string) : decimal option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetDecimal(columnIndex))
        | false, _ -> failToRead column "decimal"

    member this.double(column: string) : double =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetDouble(columnIndex)
        | false, _ -> failToRead column "double"

    member this.doubleOrNull(column: string) : double option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetDouble(columnIndex))
        | false, _ -> failToRead column "double"

    member this.timestamp(column: string) =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetTimeStamp(columnIndex)
        | false, _ -> failToRead column "timestamp"

    member this.timestampOrNull(column: string) =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetTimeStamp(columnIndex))
        | false, _ -> failToRead column "timestamp"

    member this.NpgsqlReader = reader

    member this.timestamptz(column: string) =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetTimeStamp(columnIndex)
        | false, _ -> failToRead column "timestamp"

    member this.timestamptzOrNull(column: string) =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetTimeStamp(columnIndex))
        | false, _ -> failToRead column "timestamp"


    /// Gets the value of the specified column as a globally-unique identifier (GUID).
    member this.uuid(column: string) : Guid =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetGuid(columnIndex)
        | false, _ -> failToRead column "guid"

    member this.uuidOrNull(column: string) : Guid option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some(reader.GetGuid(columnIndex))
        | false, _ -> failToRead column "guid"

    /// Gets the value of the specified column as an `NpgsqlTypes.NpgsqlDate`, Npgsql's provider-specific type for dates.
    ///
    /// PostgreSQL's date type represents dates from 4713 BC to 5874897 AD, while .NET's `DateTime` only supports years from 1 to 1999. If you require years outside this range use this accessor.
    ///
    /// See http://www.postgresql.org/docs/current/static/datatype-datetime.html to learn more
    member this.date(column: string) : NpgsqlTypes.NpgsqlDate =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetDate(columnIndex)
        | false, _ -> failToRead column "date"

    /// Gets the value of the specified column as an `NpgsqlTypes.NpgsqlDate`, Npgsql's provider-specific type for dates.
    ///
    /// PostgreSQL's date type represents dates from 4713 BC to 5874897 AD, while .NET's `DateTime` only supports years from 1 to 1999. If you require years outside this range use this accessor.
    ///
    /// See http://www.postgresql.org/docs/current/static/datatype-datetime.html to learn more
    member this.dateOrNull(column: string) : NpgsqlTypes.NpgsqlDate option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull (columnIndex)
            then None
            else Some (reader.GetDate(columnIndex))
        | false, _ -> failToRead column "date"

    /// Gets the value of the specified column as a System.DateTime object.
    member this.dateTime(column: string) : DateTime =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetDateTime(columnIndex)
        | false, _ -> failToRead column "datetime"

    /// Gets the value of the specified column as a System.DateTime object.
    member this.dateTimeOrNull(column: string) : DateTime option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetDateTime(columnIndex))
        | false, _ -> failToRead column "datetime"

    /// Gets the value of the specified column as an `NpgsqlTypes.NpgsqlTimeSpan`, Npgsql's provider-specific type for time spans.
    ///
    /// PostgreSQL's interval type has has a resolution of 1 microsecond and ranges from -178000000 to 178000000 years, while .NET's TimeSpan has a resolution of 100 nanoseconds and ranges from roughly -29247 to 29247 years.
    member this.interval(column: string) : NpgsqlTypes.NpgsqlTimeSpan =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetInterval(columnIndex)
        | false, _ -> failToRead column "interval"

    /// Reads the specified column as `byte[]`
    member this.bytea(column: string) : byte[] =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<byte[]>(columnIndex)
        | false, _ -> failToRead column "byte[]"

    /// Reads the specified column as `byte[]`
    member this.byteaOrNull(column: string) : byte[] option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFieldValue<byte[]>(columnIndex))
        | false, _ -> failToRead column "byte[]"

    /// Gets the value of the specified column as a `System.Single` object.
    member this.float(column: string) : float32 =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFloat(columnIndex)
        | false, _ -> failToRead column "float"

    /// Gets the value of the specified column as a `System.Single` object.
    member this.floatOrNull(column: string) : float32 option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFloat(columnIndex))
        | false, _ -> failToRead column "float"

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
        Parameters : (string * SqlValue) list
        IsFunction : bool
        NeedPrepare : bool
        CancellationToken: CancellationToken
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
        CancellationToken = CancellationToken.None
    }

    let connect constr  = { defaultProps() with ConnectionString = constr }
    let clientCertificate cert props = { props with ClientCertificate = Some cert }
    let host x = { defaultConString() with Host = x }
    let username username config = { config with Username = Some username }
    /// Specifies the password of the user that is logging in into the database server
    let password password config = { config with Password = Some password }
    /// Specifies the database name
    let database x con = { con with Database = x }
    /// Specifies how to manage SSL Mode.
    let sslMode mode config = { config with SslMode = Some mode }
    let cancellationToken token config = { config with CancellationToken = token }
    /// Specifies the port of the database server. If you don't specify the port, the default port of `5432` is used.
    let port port config = { config with Port = Some port }
    let trustServerCertificate value config = { config with TrustServerCertificate = Some value }
    let convertInfinityDateTime value config = { config with ConvertInfinityDateTime = Some value }
    let config extraConfig config = { config with Config = Some extraConfig }
    let formatConnectionString (config:ConnectionStringBuilder) =
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
        connect (formatConnectionString connectionConfig)

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
    let parameters ls props = { props with Parameters = ls }
    let private newConnection (props: SqlProps): NpgsqlConnection =
        let connection = new NpgsqlConnection(props.ConnectionString)
        match props.ClientCertificate with
        | Some cert ->
            connection.ProvideClientCertificatesCallback <- new ProvideClientCertificatesCallback(fun certs ->
                certs.Add(cert) |> ignore)
        | None -> ()
        connection

    let private populateRow (cmd: NpgsqlCommand) (row: (string * SqlValue) list) =
        for (paramName, value) in row do
          let paramValue, paramType : (obj * NpgsqlTypes.NpgsqlDbType option) =
            match value with
            | SqlValue.Bit value -> upcast value, Some NpgsqlTypes.NpgsqlDbType.Bit
            | SqlValue.String text -> upcast text, Some NpgsqlTypes.NpgsqlDbType.Text
            | SqlValue.Int i -> upcast i, Some NpgsqlTypes.NpgsqlDbType.Integer
            | SqlValue.Uuid x -> upcast x, Some NpgsqlTypes.NpgsqlDbType.Uuid
            | SqlValue.Short x -> upcast x, Some NpgsqlTypes.NpgsqlDbType.Smallint
            | SqlValue.Date date -> upcast date, Some NpgsqlTypes.NpgsqlDbType.Date
            | SqlValue.Timestamp x -> upcast x, Some NpgsqlTypes.NpgsqlDbType.Timestamp
            | SqlValue.TimestampWithTimeZone x -> upcast x, Some NpgsqlTypes.NpgsqlDbType.TimestampTz
            | SqlValue.Number n -> upcast n, Some NpgsqlTypes.NpgsqlDbType.Double
            | SqlValue.Bool b -> upcast b,  Some NpgsqlTypes.NpgsqlDbType.Boolean
            | SqlValue.Decimal x -> upcast x, Some NpgsqlTypes.NpgsqlDbType.Money
            | SqlValue.Long x -> upcast x, Some NpgsqlTypes.NpgsqlDbType.Bigint
            | SqlValue.Bytea x -> upcast x, Some NpgsqlTypes.NpgsqlDbType.Bytea
            | SqlValue.TimeWithTimeZone x -> upcast x, Some NpgsqlTypes.NpgsqlDbType.TimeTz
            | SqlValue.Null -> upcast DBNull.Value, None
            | SqlValue.TinyInt x -> upcast x, None
            | SqlValue.Jsonb x -> upcast x, Some NpgsqlTypes.NpgsqlDbType.Jsonb
            | SqlValue.Time x -> upcast x, Some NpgsqlTypes.NpgsqlDbType.Time
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

    let executeTransaction queries (props: SqlProps) =
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
        | error -> Error error

    let executeTransactionAsync queries (props: SqlProps)  =
        async {
            try
                let! token = Async.CancellationToken
                use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
                let mergedToken = mergedTokenSource.Token
                if List.isEmpty queries
                then return Ok [ ]
                else
                use connection = newConnection props
                do! Async.AwaitTask (connection.OpenAsync mergedToken)
                use transaction = connection.BeginTransaction()
                let affectedRowsByQuery = ResizeArray<int>()
                for (query, parameterSets) in queries do
                    if List.isEmpty parameterSets
                    then
                      use command = new NpgsqlCommand(query, connection, transaction)
                      let! affectedRows = Async.AwaitTask (command.ExecuteNonQueryAsync mergedToken)
                      affectedRowsByQuery.Add affectedRows
                    else
                      for parameterSet in parameterSets do
                        use command = new NpgsqlCommand(query, connection, transaction)
                        populateRow command parameterSet
                        let! affectedRows = Async.AwaitTask (command.ExecuteNonQueryAsync mergedToken)
                        affectedRowsByQuery.Add affectedRows
                do! Async.AwaitTask(transaction.CommitAsync mergedToken)
                return Ok (List.ofSeq affectedRowsByQuery)
            with error ->
                return Error error
        }

    let execute (read: RowReader -> 't) (props: SqlProps) : Result<'t list, exn> =
        try
            if List.isEmpty props.SqlQuery then failwith "No query provided to execute. Please use Sql.query"
            use connection = newConnection props
            connection.Open()
            use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
            do populateCmd command props
            if props.NeedPrepare then command.Prepare()
            use reader = command.ExecuteReader()
            let postgresReader = unbox<NpgsqlDataReader> reader
            let rowReader = RowReader(postgresReader)
            let result = ResizeArray<'t>()
            while reader.Read() do result.Add (read rowReader)
            Ok (List.ofSeq result)
        with error ->
            Error error

    let executeAsync (read: RowReader -> 't) (props: SqlProps) : Async<Result<'t list, exn>> =
        async {
            try
                let! token =  Async.CancellationToken
                use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
                let mergedToken = mergedTokenSource.Token
                if List.isEmpty props.SqlQuery then failwith "No query provided to execute. Please use Sql.query"
                use connection = newConnection props
                do! Async.AwaitTask(connection.OpenAsync(mergedToken))
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                use! reader = Async.AwaitTask (command.ExecuteReaderAsync(mergedToken))
                let postgresReader = unbox<NpgsqlDataReader> reader
                let rowReader = RowReader(postgresReader)
                let result = ResizeArray<'t>()
                while reader.Read() do result.Add (read rowReader)
                return Ok (List.ofSeq result)
            with error ->
                return Error error
        }

    /// Executes the query and returns the number of rows affected
    let executeNonQuery (props: SqlProps) : Result<int, exn> =
        try
            if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
            use connection = newConnection props
            connection.Open()
            use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
            populateCmd command props
            if props.NeedPrepare then command.Prepare()
            Ok (command.ExecuteNonQuery())
        with
            | error -> Error error

    /// Executes the query as asynchronously and returns the number of rows affected
    let executeNonQueryAsync  (props: SqlProps) =
        async {
            try
                let! token = Async.CancellationToken
                use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
                let mergedToken = mergedTokenSource.Token
                if List.isEmpty props.SqlQuery then failwith "No query provided to execute. Please use Sql.query"
                use connection = newConnection props
                do! Async.AwaitTask (connection.OpenAsync(mergedToken))
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                populateCmd command props
                if props.NeedPrepare then command.Prepare()
                let! affectedRows = Async.AwaitTask(command.ExecuteNonQueryAsync(mergedToken))
                return Ok affectedRows
            with
            | error -> return Error error
        }