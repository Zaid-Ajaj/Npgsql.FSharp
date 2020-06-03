namespace Npgsql.FSharp

open System
open Npgsql
open NpgsqlTypes
open System.Threading
open System.Data
open System.Collections.Generic
open System.Security.Cryptography.X509Certificates

module internal Utils =
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
    static member intOrNone(value: int option) = Utils.sqlMap value Sql.int
    static member string(value: string) = SqlValue.String (if isNull value then String.Empty else value)
    static member stringOrNone(value: string option) = Utils.sqlMap value Sql.string
    static member text(value: string) = SqlValue.String value
    static member textOrNone(value: string option) = Sql.stringOrNone value
    static member jsonb(value: string) = SqlValue.Jsonb value 
    static member jsonbOrNone(value: string option) = Utils.sqlMap value Sql.jsonb
    static member bit(value: bool) = SqlValue.Bit value
    static member bitOrNone(value: bool option) = Utils.sqlMap value Sql.bit
    static member bool(value: bool) = SqlValue.Bool value
    static member boolOrNone(value: bool option) = Utils.sqlMap value Sql.bool
    static member double(value: double) = SqlValue.Number value
    static member doubleOrNone(value: double option) = Utils.sqlMap value Sql.double
    static member decimal(value: decimal) = SqlValue.Decimal value
    static member decimalOrNone(value: decimal option) = Utils.sqlMap value Sql.decimal
    static member money(value: decimal) = SqlValue.Decimal value
    static member moneyOrNone(value: decimal option) = Sql.decimalOrNone value
    static member int8(value: int8) = SqlValue.TinyInt value
    static member int8OrNone(value: int8 option) = Utils.sqlMap value Sql.int8
    static member int16(value: int16) = SqlValue.Short value
    static member int16OrNone(value: int16 option) = Utils.sqlMap value Sql.int16
    static member int64(value: int64) = SqlValue.Long value
    static member int64OrNone(value: int64 option) = Utils.sqlMap value Sql.int64
    static member timestamp(value: DateTime) = SqlValue.Timestamp value
    static member timestampOrNone(value: DateTime option) = Utils.sqlMap value Sql.timestamp
    static member timestamptz(value: DateTime) = SqlValue.TimestampWithTimeZone value
    static member timestamptzOrNone(value: DateTime option) = Utils.sqlMap value Sql.timestamptz
    static member uuid(value: Guid) = SqlValue.Uuid value
    static member uuidOrNone(value: Guid option) = Utils.sqlMap value Sql.uuid
    static member uuidArray(value: Guid []) = SqlValue.UuidArray value
    static member uuidArrayOrNone(value: Guid [] option) = Utils.sqlMap value Sql.uuidArray
    static member bytea(value: byte[]) = SqlValue.Bytea value
    static member byteaOrNone(value: byte[] option) = Utils.sqlMap value Sql.bytea
    static member stringArray(value: string[]) = SqlValue.StringArray value
    static member stringArrayOrNone(value: string[] option) = Utils.sqlMap value Sql.stringArray
    static member intArray(value: int[]) = SqlValue.IntArray value
    static member intArrayOrNone(value: int[] option) = Utils.sqlMap value Sql.intArray
    static member dbnull = SqlValue.Null
    static member parameter(genericParameter: NpgsqlParameter) = SqlValue.Parameter genericParameter
     
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

    member this.intOrNone(column: string) : int option =
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

    member this.int16OrNone(column: string) : int16 option =
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

    member this.intArrayOrNone(column: string) : int[] option =
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

    member this.stringArrayOrNone(column: string) : string[] option =
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

    member this.int64OrNone(column: string) : int64 option =
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

    member this.stringOrNone(column: string) : string option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetString(columnIndex))
        | false, _ -> failToRead column "string"

    member this.text(column: string) : string = this.string column
    member this.textOrNone(column: string) : string option = this.stringOrNone column

    member this.bool(column: string) : bool =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<bool>(columnIndex)
        | false, _ -> failToRead column "bool"

    member this.boolOrNone(column: string) : bool option =
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

    member this.decimalOrNone(column: string) : decimal option =
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

    member this.doubleOrNone(column: string) : double option =
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

    member this.timestampOrNone(column: string) =
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

    member this.timestamptzOrNone(column: string) =
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

    member this.uuidOrNone(column: string) : Guid option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some(reader.GetGuid(columnIndex))
        | false, _ -> failToRead column "guid"

    /// Gets the value of the specified column as a globally-unique identifier (GUID).
    member this.uuidArray(column: string) : Guid [] =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<Guid []>(columnIndex)
        | false, _ -> failToRead column "guid[]"

    member this.uuidArrayOrNone(column: string) : Guid [] option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some(reader.GetFieldValue<Guid []>(columnIndex))
        | false, _ -> failToRead column "guid[]"

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
    member this.dateOrNone(column: string) : NpgsqlTypes.NpgsqlDate option =
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
    member this.dateTimeOrNone(column: string) : DateTime option =
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
    member this.byteaOrNone(column: string) : byte[] option =
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
    member this.floatOrNone(column: string) : float32 option =
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
        ExistingConnection : NpgsqlConnection option
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
        ExistingConnection = None
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

    let existingConnection (connection: NpgsqlConnection) = { defaultProps() with ExistingConnection = connection |> Option.ofObj }
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

    let private getConnection (props: SqlProps): NpgsqlConnection =
        match props.ExistingConnection with
        | Some connection -> connection
        | None -> newConnection props

    let private populateRow (cmd: NpgsqlCommand) (row: (string * SqlValue) list) =
        for (paramName, value) in row do
            
            let normalizedParameterName =
                let paramName = paramName.TrimEnd()
                if not (paramName.StartsWith "@")
                then sprintf "@%s" paramName
                else paramName

            let add value valueType = 
                cmd.Parameters.AddWithValue(normalizedParameterName, valueType, value)
                |> ignore

            match value with
            | SqlValue.Bit bit -> add bit NpgsqlDbType.Bit
            | SqlValue.String text -> add text NpgsqlDbType.Text
            | SqlValue.Int number -> add number NpgsqlDbType.Integer
            | SqlValue.Uuid uuid -> add uuid NpgsqlDbType.Uuid
            | SqlValue.UuidArray uuidArray -> add uuidArray (NpgsqlDbType.Array ||| NpgsqlDbType.Uuid)
            | SqlValue.Short number -> add number NpgsqlDbType.Smallint
            | SqlValue.Date date -> add date NpgsqlDbType.Date
            | SqlValue.Timestamp timestamp -> add timestamp NpgsqlDbType.Timestamp
            | SqlValue.TimestampWithTimeZone timestampTz -> add timestampTz NpgsqlDbType.TimestampTz
            | SqlValue.Number number -> add number NpgsqlDbType.Double
            | SqlValue.Bool boolean -> add boolean NpgsqlDbType.Boolean
            | SqlValue.Decimal number -> add number NpgsqlDbType.Money
            | SqlValue.Long number -> add number NpgsqlDbType.Bigint
            | SqlValue.Bytea binary -> add binary NpgsqlDbType.Bytea
            | SqlValue.TimeWithTimeZone x -> add x NpgsqlDbType.TimeTz
            | SqlValue.Null -> cmd.Parameters.AddWithValue(normalizedParameterName, DBNull.Value) |> ignore
            | SqlValue.TinyInt x -> cmd.Parameters.AddWithValue(normalizedParameterName, x) |> ignore
            | SqlValue.Jsonb x -> add x NpgsqlDbType.Jsonb
            | SqlValue.Time x -> add x NpgsqlDbType.Time
            | SqlValue.StringArray x -> add x (NpgsqlDbType.Array ||| NpgsqlDbType.Text)
            | SqlValue.IntArray x -> add x (NpgsqlDbType.Array ||| NpgsqlDbType.Integer)
            | SqlValue.Parameter x -> cmd.Parameters.AddWithValue(normalizedParameterName, x) |> ignore

    let private populateCmd (cmd: NpgsqlCommand) (props: SqlProps) =
        if props.IsFunction then cmd.CommandType <- CommandType.StoredProcedure
        populateRow cmd props.Parameters

    let executeTransaction queries (props: SqlProps) =
        try
            if List.isEmpty queries
            then Ok [ ]
            else
            let connection = getConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then connection.Open()
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
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()

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
                let connection = getConnection props
                try
                    if not (connection.State.HasFlag ConnectionState.Open)
                    then do! Async.AwaitTask (connection.OpenAsync mergedToken)
                    use transaction = connection.BeginTransaction ()
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
                finally
                    if props.ExistingConnection.IsNone
                    then connection.Dispose()
            with error ->
                return Error error
        }

    let execute (read: RowReader -> 't) (props: SqlProps) : Result<'t list, exn> =
        try
            if List.isEmpty props.SqlQuery then failwith "No query provided to execute. Please use Sql.query"
            let connection = getConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then connection.Open()
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                use reader = command.ExecuteReader()
                let postgresReader = unbox<NpgsqlDataReader> reader
                let rowReader = RowReader(postgresReader)
                let result = ResizeArray<'t>()
                while reader.Read() do result.Add (read rowReader)
                Ok (List.ofSeq result)
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
        with error ->
            Error error

    let iter (perform: RowReader -> unit) (props: SqlProps) : Result<unit, exn> =
        try
            if List.isEmpty props.SqlQuery then failwith "No query provided to execute. Please use Sql.query"
            let connection = getConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then connection.Open()
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                use reader = command.ExecuteReader()
                let postgresReader = unbox<NpgsqlDataReader> reader
                let rowReader = RowReader(postgresReader)
                while reader.Read() do perform rowReader
                Ok ()
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
        with error ->
            Error error

    let executeRow (read: RowReader -> 't) (props: SqlProps) : Result<'t, exn> =
        try
            if List.isEmpty props.SqlQuery then failwith "No query provided to execute. Please use Sql.query"
            let connection = getConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then connection.Open()
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                use reader = command.ExecuteReader()
                let postgresReader = unbox<NpgsqlDataReader> reader
                let rowReader = RowReader(postgresReader)
                if reader.Read() 
                then Ok (read rowReader)
                else failwith "Expected at least one row to be returned from the result set. Instead it was empty"
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
        with error ->
            Error error

    let executeAsync (read: RowReader -> 't) (props: SqlProps) : Async<Result<'t list, exn>> =
        async {
            try
                let! token =  Async.CancellationToken
                use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
                let mergedToken = mergedTokenSource.Token
                if List.isEmpty props.SqlQuery then failwith "No query provided to execute. Please use Sql.query"
                let connection = getConnection props
                try
                    if not (connection.State.HasFlag ConnectionState.Open)
                    then do! Async.AwaitTask(connection.OpenAsync(mergedToken))
                    use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                    do populateCmd command props
                    if props.NeedPrepare then command.Prepare()
                    use! reader = Async.AwaitTask (command.ExecuteReaderAsync(mergedToken))
                    let postgresReader = unbox<NpgsqlDataReader> reader
                    let rowReader = RowReader(postgresReader)
                    let result = ResizeArray<'t>()
                    while reader.Read() do result.Add (read rowReader)
                    return Ok (List.ofSeq result)
                finally
                    if props.ExistingConnection.IsNone
                    then connection.Dispose()
            with error ->
                return Error error
        }

    let iterAsync (perform: RowReader -> unit) (props: SqlProps) : Async<Result<unit, exn>> =
        async {
            try
                let! token =  Async.CancellationToken
                use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
                let mergedToken = mergedTokenSource.Token
                if List.isEmpty props.SqlQuery then failwith "No query provided to execute. Please use Sql.query"
                let connection = getConnection props
                try
                    if not (connection.State.HasFlag ConnectionState.Open)
                    then do! Async.AwaitTask(connection.OpenAsync(mergedToken))
                    use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                    do populateCmd command props
                    if props.NeedPrepare then command.Prepare()
                    use! reader = Async.AwaitTask (command.ExecuteReaderAsync(mergedToken))
                    let postgresReader = unbox<NpgsqlDataReader> reader
                    let rowReader = RowReader(postgresReader)
                    while reader.Read() do perform rowReader
                    return Ok ()
                finally
                    if props.ExistingConnection.IsNone
                    then connection.Dispose()
            with error ->
                return Error error
        }

    let executeRowAsync (read: RowReader -> 't) (props: SqlProps) : Async<Result<'t, exn>> =
        async {
            try
                let! token =  Async.CancellationToken
                use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
                let mergedToken = mergedTokenSource.Token
                if List.isEmpty props.SqlQuery then failwith "No query provided to execute. Please use Sql.query"
                let connection = getConnection props
                try
                    if not (connection.State.HasFlag ConnectionState.Open)
                    then do! Async.AwaitTask(connection.OpenAsync(mergedToken))
                    use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                    do populateCmd command props
                    if props.NeedPrepare then command.Prepare()
                    use! reader = Async.AwaitTask (command.ExecuteReaderAsync(mergedToken))
                    let postgresReader = unbox<NpgsqlDataReader> reader
                    let rowReader = RowReader(postgresReader)
                    if reader.Read() 
                    then return Ok (read rowReader)
                    else return! failwith "Expected at least one row to be returned from the result set. Instead it was empty"
                finally
                    if props.ExistingConnection.IsNone
                    then connection.Dispose()
            with error ->
                return Error error
        }

    /// Executes the query and returns the number of rows affected
    let executeNonQuery (props: SqlProps) : Result<int, exn> =
        try
            if List.isEmpty props.SqlQuery then failwith "No query provided to execute..."
            let connection = getConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then connection.Open()
                use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                populateCmd command props
                if props.NeedPrepare then command.Prepare()
                Ok (command.ExecuteNonQuery())
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
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
                let connection = getConnection props
                try
                    if not (connection.State.HasFlag ConnectionState.Open)
                    then do! Async.AwaitTask (connection.OpenAsync(mergedToken))
                    use command = new NpgsqlCommand(List.head props.SqlQuery, connection)
                    populateCmd command props
                    if props.NeedPrepare then command.Prepare()
                    let! affectedRows = Async.AwaitTask(command.ExecuteNonQueryAsync(mergedToken))
                    return Ok affectedRows
                finally
                    if props.ExistingConnection.IsNone
                    then connection.Dispose()
            with
            | error -> return Error error
        }