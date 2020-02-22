# Npgsql.FSharp [![Nuget](https://img.shields.io/nuget/v/Npgsql.FSharp.svg?colorB=green)](https://www.nuget.org/packages/Npgsql.FSharp) [![Build Status](https://travis-ci.com/Zaid-Ajaj/Npgsql.FSharp.svg?branch=master)](https://travis-ci.com/Zaid-Ajaj/Npgsql.FSharp)

Thin F#-friendly layer for the [Npgsql](https://github.com/npgsql/npgsql) data provider for PostgreSQL.

For an optimal developer experience, this library can be used with [Npgsql.FSharp.Analyzer](https://github.com/Zaid-Ajaj/Npgsql.FSharp.Analyzer) which is a F# analyzer that will verify the query syntax and perform type-checking againt the database schema.

This wrapper maps *raw* SQL data from the database into the `Sql` data structure making it easy to pattern match against and transform the results.

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

// You can get the connection string from the config by calling `Sql.str`
let connectionString =
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

`Sql.connectFromConfig inputConfig` is just `Sql.connect (Sql.formatConnectionString inputConfig)`

### Execute query and read results as table then map the results
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
    |> Sql.execute (fun read -> {
        Id = read.int "user_id"
        FirstName = read.text "first_name"
        LastName = read.text "last_name"
    })
```
### Deal with null values and provide defaults
```fs
type User = {
    Id: int
    FirstName: string
    LastName: string option
}

let getAllUsers() : Result<User list, exn> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM users"
    |> Sql.execute (fun read -> {
        Id = read.int "user_id"
        FirstName = read.text "first_name"
        LastName = read.textOrNull "last_name"
    })
```
### Make the reading async using `Sql.executeAsync`
```fsharp
let getAllUsers() : Async<Result<User list, exn>> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM users"
    |> Sql.executeAsync (fun read -> {
        Id = read.int "user_id"
        FirstName = read.text "first_name"
        LastName = read.textOrNull "last_name"
    })
```

### Parameterized queries
```fs
let getAllUsers() : Async<Result<User list, exn>> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM users WHERE is_active = @active"
    |> Sql.parameters [ "active", Sql.bit true ]
    |> Sql.executeAsync (fun read -> {
        Id = read.int "user_id"
        FirstName = read.text "first_name"
        LastName = read.textOrNull "last_name"
    })
```

### Execute scalar values with `Sql.executeSingleRow`
```fs
let activeUsers() : Async<Result<int, exn>> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT COUNT(*) as count FROM users WHERE is_active = @active"
    |> Sql.parameters [ "active", Sql.bit true ]
    |> Sql.executeSingleRow (fun read -> read.int "count")
```

### Execute multiple inserts or updates in a single transaction:
```fs
connectionString
|> Sql.connect
|> Sql.executeTransaction // SqlProps -> int list
    [
        "INSERT INTO ... VALUES (@number)", [
            [ "@number", SqlValue.Int 1 ]
            [ "@number", SqlValue.Int 2 ]
            [ "@number", SqlValue.Int 3 ]
        ]

        "UPDATE ... SET meta = @meta",  [
           [ "@meta", SqlValue.String value ]
        ]
   ]
```


### To Run tests

Docker must be installed

```cmd
build(.cmd or sh) StartDatabase
build RunTests
build StopDatabase
```
