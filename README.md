# Npgsql.FSharp [![Nuget](https://img.shields.io/nuget/v/Npgsql.FSharp.svg?colorB=green)](https://www.nuget.org/packages/Npgsql.FSharp) [![Build Status](https://travis-ci.com/Zaid-Ajaj/Npgsql.FSharp.svg?branch=master)](https://travis-ci.com/Zaid-Ajaj/Npgsql.FSharp)

Thin F#-friendly layer for the [Npgsql](https://github.com/npgsql/npgsql) data provider for PostgreSQL.

For an optimal developer experience, this library is made to work with [Npgsql.FSharp.Analyzer](https://github.com/Zaid-Ajaj/Npgsql.FSharp.Analyzer) which is a F# analyzer that will verify the query syntax and perform type-checking against the parameters and the types of the columns from the result set.

### Configure the connection string
```fs
open Npgsql.FSharp

// construct the connection configuration
let defaultConnection  =
    Sql.host "localhost"
    |> Sql.port 5432
    |> Sql.username "user"
    |> Sql.password "password"
    |> Sql.database "app_db"
    |> Sql.sslMode SslMode.Require
    |> Sql.config "Pooling=true" // optional Config for connection string

// You can get the connection string from the config by calling `Sql.formatConnectionString`
let connectionString : string =
    defaultConnection
    |> Sql.formatConnectionString

// construct connection string from postgres Uri
// NOTE: query string parameters are not converted
let defaultConnection : string =
    Sql.fromUri (Uri "postgresql://user:password@localhost:5432/app_db")

// Construct parts of the connection config from the Uri
// and add more from the `Sql` module. For example to connect to Heroku Postgres databases, you do the following
// NOTE: query string parameters are not converted
let herokuConfig : string =
    Sql.fromUriToConfig (Uri "postgresql://user:password@localhost:5432/app_db")
    |> Sql.sslMode SslMode.Require
    |> Sql.trustServerCertificate true
    |> Sql.formatConnectionString
```
### Sql.connect vs Sql.connectFromConfig

The function `Sql.connect` takes a connection string as input, for example if you have it configured as an environment variable.

However, `Sql.connectFromConfig` takes the connection string *builder* if you are configuring the connection string from code.

`Sql.connectFromConfig config` is just `Sql.connect (Sql.formatConnectionString config)`

### Execute query and read results as table then map the results
The main function to execute queries and return a list of a results is `Sql.execute`:
```fs
open Npgsql.FSharp
open Npgsql.FSharp.OptionWorkflow

type User = {
    Id: int
    FirstName: string
    LastName: string
}

let getAllUsers() : Result<User list, exn> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM users"
    |> Sql.execute (fun read ->
        {
            Id = read.int "user_id"
            FirstName = read.text "first_name"
            LastName = read.text "last_name"
        })
```
The function is *always* safe and will return you `Result<'t, exn>` from the execution.

### Deal with null values and provide defaults
Notice the `LastName` field becomes `string option` instead of `string`
```fs
type User = {
    Id: int
    FirstName: string
    LastName: string option // notice option here
}

let getAllUsers() : Result<User list, exn> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM users"
    |> Sql.execute (fun read ->
        {
            Id = read.int "user_id"
            FirstName = read.text "first_name"
            LastName = read.textOrNone "last_name" // reading nullable column
        })
```
### Make the reading async using `Sql.executeAsync`
```fsharp
let getAllUsers() : Async<Result<User list, exn>> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM users"
    |> Sql.executeAsync (fun read ->
        {
            Id = read.int "user_id"
            FirstName = read.text "first_name"
            LastName = read.textOrNone "last_name"
        })
```

### Parameterized queries
Provide parameters using the `Sql.parameters` function as a list of tuples. When using the analyzer, make sure you use functions from `Sql` module to initialize the values so that the analyzer can type-check them against the types of the required parameters.
```fs
let getAllUsers() : Async<Result<User list, exn>> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM users WHERE is_active = @active"
    |> Sql.parameters [ "active", Sql.bit true ]
    |> Sql.executeAsync (fun read ->
        {
            Id = read.int "user_id"
            FirstName = read.text "first_name"
            LastName = read.textOrNone "last_name"
        })
```

### Execute multiple inserts or updates in a single transaction:
```fs
connectionString
|> Sql.connect
|> Sql.executeTransaction // SqlProps -> int list
    [
        "INSERT INTO ... VALUES (@number)", [
            [ "@number", Sql.int 1 ]
            [ "@number", Sql.int 2 ]
            [ "@number", Sql.int 3 ]
        ]

        "UPDATE ... SET meta = @meta",  [
           [ "@meta", Sql.text value ]
        ]
   ]
```
### Returns number of affected rows from statement
Use the function `Sql.executeNonQuery` or its async counter part to get the number of affected rows from a query. Like always, the function is safe by default and returns `Result<int, exn>` as output.
```fs
let getAllUsers() : Result<int, exn> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "DELETE FROM users WHERE is_active = @is_active"
    |> Sql.parameters [ "is_active", Sql.bit false ]
    |> Sql.executeNonQuery
```
### Use an existing connections
Sometimes, you already have constructed a `NpgsqlConnection` and want to use with the `Sql` module. You can use the function `Sql.existingConnection` which takes a preconfigured connection from which the queries or transactions are executed. Note that this library will *open the connection* if it is not already open and it will leave the connection open (deos not dispose of it) when it finishes running. This means that you have to manage the disposal of the connection yourself:
```fs
use connection = new NpgsqlConnection("YOUR CONNECTION STRING")
connection.Open()

let users =
    connection
    |> Sql.existingConnection
    |> Sql.query "SELECT * FROM users"
    |> Sql.execute (fun read ->
        {
            Id = read.int "user_id"
            FirstName = read.text "first_name"
        })
```
Note in this example, when we write `use connection = ...` it means the connection will be disposed at the end of the scopre where this value is bound, not internally from the `Sql` module.

### Reading values from the `NpgsqlDataReader`
When running the `Sql.execute` function, you can read values directly from the `NpgsqlDataReader` as opposed to using the provided `RowReader`. Instead of writing this:
```fs
let getAllUsers() : Result<User list, exn> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM users"
    |> Sql.execute (fun read ->
        {
            Id = read.int "user_id"
            FirstName = read.text "first_name"
            LastName = read.textOrNone "last_name" // reading nullable column
        })
```
You write 
```fs
let getAllUsers() : Result<User list, exn> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM users"
    |> Sql.execute (fun read ->
        {
            Id = read.NpgsqlReader.GetInt32(read.NpgsqlReader.GetOrdinal("user_id"))
            FirstName = read.NpgsqlReader.GetString(read.NpgsqlReader.GetOrdinal("first_name"))
            LastName = read.textOrNone "last_name" // reading nullable column
        })
```
Here we are using the `NpgsqlReader` property from the `RowReader` which allows you to read or convert custom values. Usually you don't need this unless when you are using custom type handlers for the `NpgsqlConnection`.

### Custom parameters with `NpgsqlParameter`

When the built-in parameter constructors aren't enough for you (for example when you are using type handler plugins) then you can use the generic `Sql.parameter` function to provide one:
```fs
let customParameter = new NpgsqlParameter(...)

defaultConnection
|> Sql.connectFromConfig
|> Sql.query "SELECT * FROM users"
|> Sql.parameters [ "username", Sql.parameter customParameter ]
|> Sql.execute (fun read ->
    {
        Id = read.int "user_id"
        FirstName = read.text "first_name"
        LastName = read.textOrNone "last_name" // reading nullable column
    })
```