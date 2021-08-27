namespace Npgsql.FSharp

open System
open Npgsql
open NpgsqlTypes
open System.Threading
open System.Data
open System.Security.Cryptography.X509Certificates
open FSharp.Control.Tasks
open System.Threading.Tasks

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
    type ExecutionTarget =
        | ConnectionString of string
        | Connection of NpgsqlConnection
        | Empty

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
        ExecutionTarget : ExecutionTarget
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
        ExecutionTarget = Empty
        SqlQuery = [];
        Parameters = [];
        IsFunction = false
        NeedPrepare = false
        ClientCertificate = None
        CancellationToken = CancellationToken.None
    }

    let connect (constr: string) =
        if Uri.IsWellFormedUriString(constr, UriKind.Absolute) && constr.StartsWith "postgres://"
        then { defaultProps() with ExecutionTarget = ConnectionString (Uri(constr).ToPostgresConnectionString()) }
        else { defaultProps() with ExecutionTarget = ConnectionString (constr) }

    let clientCertificate cert props = { props with ClientCertificate = Some cert }
    let host x = { defaultConString() with Host = x }
    let username username config = { config with Username = Some username }
    /// Specifies the password of the user that is logging in into the database server
    let password password config = { config with Password = Some password }
    /// Specifies the database name
    let database x con = { con with Database = x }
    /// Specifies how to manage SSL Mode.
    let sslMode mode config = { config with SslMode = Some mode }
    let requireSslMode config = { config with SslMode = Some SslMode.Require }

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

    /// Uses an existing connection to execute SQL commands against
    let existingConnection (connection: NpgsqlConnection) = { defaultProps() with ExecutionTarget = Connection connection }
    /// Configures the SQL query to execute
    let query (sql: string) props = { props with SqlQuery = [sql] }
    let func (sql: string) props = { props with SqlQuery = [sql]; IsFunction = true }

    let queryI (sql: FormattableString) props =
        let mutable parameterizedString = sql.Format
        for i = 0 to sql.ArgumentCount-1 do
            parameterizedString <- parameterizedString.Replace($"{{{i}}}", $"@p{i}")
        let parameters =
            List.init (sql.ArgumentCount) (fun i ->
                let name = $"p{i}"
                let p = NpgsqlParameter(name, sql.GetArgument(i))
                name, SqlValue.Parameter p)
        { props with
            SqlQuery = [parameterizedString]
            Parameters = parameters }

    let prepare  props = { props with NeedPrepare = true }
    /// Provides the SQL parameters for the query
    let parameters ls props = { props with Parameters = ls }
    /// When using the Npgsql.FSharp.Analyzer, this function annotates the code to tell the analyzer to ignore and skip the SQL analyzer against the database.
    let skipAnalysis (props: SqlProps) = props
    /// Creates or returns the SQL connection used to execute the SQL commands
    let createConnection (props: SqlProps): NpgsqlConnection =
        match props.ExecutionTarget with
        | ConnectionString connectionString ->
            let connection = new NpgsqlConnection(connectionString)
            match props.ClientCertificate with
            | Some cert ->
                connection.ProvideClientCertificatesCallback <- new ProvideClientCertificatesCallback(fun certs ->
                    certs.Add(cert) |> ignore)
            | None -> ()
            connection

        | Connection existingConnection -> existingConnection
        | Empty -> failwith "Could not create a connection from empty parameters."

    let private makeCommand (props: SqlProps) (connection: NpgsqlConnection) =
        match props.ExecutionTarget with
        | ConnectionString _
        | Connection _ -> new NpgsqlCommand(List.head props.SqlQuery, connection)
        | Empty -> failwith "Cannot create command from an empty execution target"

    let private populateRow (cmd: NpgsqlCommand) (row: (string * SqlValue) list) =
        for (paramName, value) in row do

            let normalizedParameterName =
                let paramName = paramName.Trim()
                if not (paramName.StartsWith "@")
                then sprintf "@%s" paramName
                else paramName

            let add value valueType =
                cmd.Parameters.AddWithValue(normalizedParameterName, valueType, value)
                |> ignore

            let addUntyped value =
                cmd.Parameters.AddWithValue(normalizedParameterName, value)
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
            | SqlValue.Decimal number -> add number NpgsqlDbType.Numeric
            | SqlValue.Money number -> add number NpgsqlDbType.Money
            | SqlValue.Long number -> add number NpgsqlDbType.Bigint
            | SqlValue.Bytea binary -> add binary NpgsqlDbType.Bytea
            | SqlValue.TimeWithTimeZone x -> add x NpgsqlDbType.TimeTz
            | SqlValue.Null -> cmd.Parameters.AddWithValue(normalizedParameterName, DBNull.Value) |> ignore
            | SqlValue.TinyInt x -> cmd.Parameters.AddWithValue(normalizedParameterName, x) |> ignore
            | SqlValue.Jsonb x -> add x NpgsqlDbType.Jsonb
            | SqlValue.Time x -> add x NpgsqlDbType.Time
            | SqlValue.StringArray x -> add x (NpgsqlDbType.Array ||| NpgsqlDbType.Text)
            | SqlValue.IntArray x -> add x (NpgsqlDbType.Array ||| NpgsqlDbType.Integer)
            | SqlValue.ShortArray x -> add x (NpgsqlDbType.Array ||| NpgsqlDbType.Smallint)
            | SqlValue.LongArray x -> add x (NpgsqlDbType.Array ||| NpgsqlDbType.Bigint)
            | SqlValue.Parameter x ->
                x.ParameterName <- normalizedParameterName
                ignore (cmd.Parameters.Add(x))
            | SqlValue.Point x -> add x NpgsqlDbType.Point

    let private populateCmd (cmd: NpgsqlCommand) (props: SqlProps) =
        if props.IsFunction then cmd.CommandType <- CommandType.StoredProcedure
        populateRow cmd props.Parameters

    let executeTransaction queries (props: SqlProps) =
        if List.isEmpty queries
        then [ ]
        else
        let connection = createConnection props
        try
            if not (connection.State.HasFlag ConnectionState.Open)
            then connection.Open()
            use transaction = connection.BeginTransaction()
            let affectedRowsByQuery = ResizeArray<int>()
            for (query, parameterSets) in queries do
                if List.isEmpty parameterSets
                then
                    use command = new NpgsqlCommand(query, connection, transaction)
                    // detect whether the command has parameters
                    // if that is the case, then don't execute it
                    NpgsqlCommandBuilder.DeriveParameters(command)
                    if command.Parameters.Count = 0 then
                        let affectedRows = command.ExecuteNonQuery()
                        affectedRowsByQuery.Add affectedRows
                    else
                        // parameterized query won't execute
                        // when the parameter set is empty
                        affectedRowsByQuery.Add 0
                else
                    for parameterSet in parameterSets do
                        use command = new NpgsqlCommand(query, connection, transaction)
                        populateRow command parameterSet
                        let affectedRows = command.ExecuteNonQuery()
                        affectedRowsByQuery.Add affectedRows

            transaction.Commit()
            List.ofSeq affectedRowsByQuery
        finally
            match props.ExecutionTarget with
            | ConnectionString _ -> connection.Dispose()
            | _ ->
                // leave connections open
                // when provided from outside
                ()

    let executeTransactionAsync queries (props: SqlProps) =
        task {
            if List.isEmpty queries
            then return [ ]
            else
            let connection = createConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then do! connection.OpenAsync props.CancellationToken
                use transaction = connection.BeginTransaction()
                let affectedRowsByQuery = ResizeArray<int>()
                for (query, parameterSets) in queries do
                    if List.isEmpty parameterSets then
                        use command = new NpgsqlCommand(query, connection, transaction)
                        // detect whether the command has parameters
                        // if that is the case, then don't execute it
                        NpgsqlCommandBuilder.DeriveParameters(command)
                        if command.Parameters.Count = 0 then
                            let! affectedRows = command.ExecuteNonQueryAsync props.CancellationToken
                            affectedRowsByQuery.Add affectedRows
                        else
                            // parameterized query won't execute
                            // when the parameter set is empty
                            affectedRowsByQuery.Add 0
                    else
                        for parameterSet in parameterSets do
                            use command = new NpgsqlCommand(query, connection, transaction)
                            populateRow command parameterSet
                            let! affectedRows = command.ExecuteNonQueryAsync props.CancellationToken
                            affectedRowsByQuery.Add affectedRows
                do! transaction.CommitAsync props.CancellationToken
                return List.ofSeq affectedRowsByQuery
            finally
                match props.ExecutionTarget with
                | ConnectionString _ -> connection.Dispose()
                | _ -> ()
        }

    let execute (read: RowReader -> 't) (props: SqlProps) : 't list =
        if List.isEmpty props.SqlQuery
        then raise <| MissingQueryException "No query provided to execute. Please use Sql.query"
        let connection = createConnection props
        try
            if not (connection.State.HasFlag ConnectionState.Open)
            then connection.Open()
            use command = makeCommand props connection
            do populateCmd command props
            if props.NeedPrepare then command.Prepare()
            use reader = command.ExecuteReader()
            let postgresReader = unbox<NpgsqlDataReader> reader
            let rowReader = RowReader(postgresReader)
            let result = ResizeArray<'t>()
            while reader.Read() do result.Add (read rowReader)
            List.ofSeq result
        finally
            match props.ExecutionTarget with
            | ConnectionString _ -> connection.Dispose()
            | _ -> ()

    let iter (perform: RowReader -> unit) (props: SqlProps) : unit =
        if List.isEmpty props.SqlQuery
        then raise <| MissingQueryException "No query provided to execute. Please use Sql.query"
        let connection = createConnection props
        try
            if not (connection.State.HasFlag ConnectionState.Open)
            then connection.Open()
            use command = makeCommand props connection
            do populateCmd command props
            if props.NeedPrepare then command.Prepare()
            use reader = command.ExecuteReader()
            let postgresReader = unbox<NpgsqlDataReader> reader
            let rowReader = RowReader(postgresReader)
            while reader.Read() do perform rowReader
        finally
            match props.ExecutionTarget with
            | ConnectionString _ -> connection.Dispose()
            | _ -> ()

    let executeRow (read: RowReader -> 't) (props: SqlProps) : 't =
        if List.isEmpty props.SqlQuery
        then raise <| MissingQueryException "No query provided to execute. Please use Sql.query"
        let connection = createConnection props
        try
            if not (connection.State.HasFlag ConnectionState.Open)
            then connection.Open()
            use command = makeCommand props connection
            do populateCmd command props
            if props.NeedPrepare then command.Prepare()
            use reader = command.ExecuteReader()
            let postgresReader = unbox<NpgsqlDataReader> reader
            let rowReader = RowReader(postgresReader)
            if reader.Read()
            then read rowReader
            else raise <| NoResultsException "Expected at least one row to be returned from the result set. Instead it was empty"
        finally
            match props.ExecutionTarget with
            | ConnectionString _ -> connection.Dispose()
            | _ -> ()

    let executeAsync (read: RowReader -> 't) (props: SqlProps) : Task<'t list> =
        task {
            if List.isEmpty props.SqlQuery
            then raise <| MissingQueryException "No query provided to execute. Please use Sql.query"
            let connection = createConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then do! connection.OpenAsync(props.CancellationToken)
                use command = makeCommand props connection
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                use! reader = command.ExecuteReaderAsync props.CancellationToken
                let postgresReader = unbox<NpgsqlDataReader> reader
                let rowReader = RowReader(postgresReader)
                let result = ResizeArray<'t>()
                while reader.Read() do result.Add (read rowReader)
                return List.ofSeq result
            finally
                match props.ExecutionTarget with
                | ConnectionString _ -> connection.Dispose()
                | _ -> ()
        }

    let iterAsync (perform: RowReader -> unit) (props: SqlProps) : Task =
        unitTask {
            if List.isEmpty props.SqlQuery
            then raise <| MissingQueryException "No query provided to execute. Please use Sql.query"
            let connection = createConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then do! connection.OpenAsync(props.CancellationToken)
                use command = makeCommand props connection
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                use! reader = command.ExecuteReaderAsync(props.CancellationToken)
                let postgresReader = unbox<NpgsqlDataReader> reader
                let rowReader = RowReader(postgresReader)
                while reader.Read() do perform rowReader
            finally
                match props.ExecutionTarget with
                | ConnectionString _ -> connection.Dispose()
                | _ -> ()
        }

    let executeRowAsync (read: RowReader -> 't) (props: SqlProps) : Task<'t> =
        task {
            if List.isEmpty props.SqlQuery
            then raise <| MissingQueryException "No query provided to execute. Please use Sql.query"
            let connection = createConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then do! connection.OpenAsync(props.CancellationToken)
                use command = makeCommand props connection
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                use! reader = command.ExecuteReaderAsync props.CancellationToken
                let postgresReader = unbox<NpgsqlDataReader> reader
                let rowReader = RowReader(postgresReader)
                if reader.Read()
                then return read rowReader
                else return! raise <| NoResultsException "Expected at least one row to be returned from the result set. Instead it was empty"
            finally
                match props.ExecutionTarget with
                | ConnectionString _ -> connection.Dispose()
                | _ -> ()
        }

    /// Executes the query and returns the number of rows affected
    let executeNonQuery (props: SqlProps) : int =
        if List.isEmpty props.SqlQuery
        then raise <| MissingQueryException "No query provided to execute..."
        let connection = createConnection props
        try
            if not (connection.State.HasFlag ConnectionState.Open)
            then connection.Open()
            use command = makeCommand props connection
            populateCmd command props
            if props.NeedPrepare then command.Prepare()
            command.ExecuteNonQuery()
        finally
            match props.ExecutionTarget with
            | ConnectionString _ -> connection.Dispose()
            | _ -> ()

    /// Executes the query as asynchronously and returns the number of rows affected
    let executeNonQueryAsync  (props: SqlProps) =
        task {
            if List.isEmpty props.SqlQuery
            then raise <| MissingQueryException "No query provided to execute. Please use Sql.query"
            let connection = createConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then do! connection.OpenAsync props.CancellationToken
                use command = makeCommand props connection
                populateCmd command props
                if props.NeedPrepare then command.Prepare()
                let! affectedRows = command.ExecuteNonQueryAsync props.CancellationToken
                return affectedRows
            finally
                match props.ExecutionTarget with
                | ConnectionString _ -> connection.Dispose()
                | _ -> ()
        }