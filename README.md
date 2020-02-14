# Npgsql.FSharp [![Nuget](https://img.shields.io/nuget/v/Npgsql.FSharp.svg?colorB=green)](https://www.nuget.org/packages/Npgsql.FSharp) [![Build Status](https://travis-ci.com/Zaid-Ajaj/Npgsql.FSharp.svg?branch=master)](https://travis-ci.com/Zaid-Ajaj/Npgsql.FSharp)

Thin F# wrapper for [Npgsql](https://github.com/npgsql/npgsql), data provider for PostgreSQL.

This wrapper maps *raw* SQL data from the database into the `Sql` data structure making it easy to pattern match against and transform the results.

Given the types:
```fs
[<RequireQualifiedAccess>]
type SqlValue =
    | Null
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
    | Timestamp of DateTime
    | TimestampWithTimeZone of DateTime
    | Time of TimeSpan
    | TimeWithTimeZone of DateTimeOffset
    | Jsonb of string
    | StringArray of string array
    | IntArray of int array

// A row is a list of key/value pairs
type SqlRow = list<string * SqlValue>

// A table is list of rows
type SqlTable = list<SqlRow>
```
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
    |> Sql.str

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
    |> Sql.str
```
### Sql.connect vs Sql.connectFromConfig

The function `Sql.connect` takes a connection string as input, for example if you have it configured as an environment variable.

However, `Sql.connectFromConfig` takes the connection string *builder* if you are configuring the connection string from code.

`Sql.connectFromConfig` will internally call `Sql.connect (Sql.str inputConfig)`

### Execute query and read results as table then map the results
```fs
open Npgsql.FSharp
open Npgsql.FSharp.OptionWorkflow

type User = {
    Id: int
    FirstName: string
    LastName: string
}

let getAllUsers() : User list =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM users"
    |> Sql.executeReader (fun reader ->
        let row = Sql.readRow reader
        option {
            let! id = Sql.readInt "user_id" row
            let! fname = Sql.readString "first_name" row
            let! lname = Sql.readString "last_name" row
            return { Id = id; FirstName = fname; LastName = lname }
        })
```
### Deal with null values and provide defaults
Notice we are not using `let bang` but just `let` instead

```fs
let getAllUsers() : User list =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM users"
    |> Sql.executeReader (fun reader ->
        let row = Sql.readRow reader
        option {
            let! id = Sql.readInt "user_id" row
            let fname = Sql.readString "first_name" row
            let lname = Sql.readString "last_name" row
            return {
                Id = id;
                FirstName = defaultArg fname ""
                LastName = defaultArg lname ""
            }
        })
```
### Make the reading async using `Sql.executeReaderAsync`
```fsharp
let getAllUsers() : User list =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM users"
    |> Sql.executeReaderAsync (fun reader ->
        let row = Sql.readRow reader
        option {
            let! id = Sql.readInt "user_id" row
            let fname = Sql.readString "first_name" row
            let lname = Sql.readString "last_name" row
            return {
                Id = id;
                FirstName = defaultArg fname ""
                LastName = defaultArg lname ""
            }
        })
```
### Execute a function with parameters
```fs
/// Check whether or not a user exists by his username
let userExists (name: string) : bool =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.func "user_exists"
    |> Sql.parameters ["username", Sql.Value name]
    |> Sql.executeScalar // SqlValue
    |> Sql.toBool
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

### Async: Execute a function with parameters
```fs
/// Check whether or not a user exists by his username
let userExists (name: string) : Async<bool> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.func "user_exists"
    |> Sql.parameters ["username", Sql.Value name]
    |> Sql.executeScalarAsync
    |> Async.map Sql.toBool
```


### Parameterize queries with complex parameters like hstore
```fs
// Insert a book with it's attributes stored as HStore values
let bookAttributes =
    Map.empty
    |> Map.add "isbn" "46243425212"
    |> Map.add "page-count" "423"
    |> Map.add "weight" "500g"

defaultConnection
|> Sql.connectFromConfig
|> Sql.query "INSERT INTO books (id,title,attrs) VALUES (@bookId, @title, @attributes)"
|> Sql.parameters
    [ "bookId", Sql.Value 20
      "title", Sql.Value "Lord of the rings"
      "attributes", Sql.Value bookAttributes ]
|> Sql.prepare       // optionnal, see http://www.npgsql.org/doc/prepare.html
|> Sql.executeNonQuery
```
### Retrieve single value safely
```fs
// ping the database
let serverTime() : Option<DateTime> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT NOW()"
    |> Sql.executeScalarSafe
    |> function
        | Ok (SqlValue.Timestamp time) -> Some time
        | _ -> None
```
### Retrieve single value safely asynchronously
```fs
// ping the database
let serverTime() : Async<Option<DateTime>> =
    async {
        let! result =
          defaultConnection
          |> Sql.connectFromConfig
          |> Sql.query "SELECT NOW()"
          |> Sql.executeScalarSafeAsync

        match result with
        | Ok (SqlValue.Timestamp time) -> return Some time
        | otherwise -> return None
    }
```
### Batch queries in a single roundtrip to the database
```fs
defaultConnection
|> Sql.connectFromConfig
|> Sql.queryMany
    ["SELECT * FROM users"
     "SELECT * FROM products"]
|> Sql.executeMany // returns list<SqlTable>
|> function
    | [ firstTable; secondTable ] -> (* do stuff *)
    | otherwise -> failwith "should not happen"
```
### Variants of returns types for the execute methods
```fs
// read results as tables
Sql.executeTable // SqlTable
Sql.executeTableSafe // Result<SqlTable, exn>
Sql.executeTableTask // Task<SqlTable>
Sql.executeTableAsync // Async<SqlTable>
Sql.executeTableSafeAsync // Async<Result<SqlTable, exn>>
Sql.executeTableSafeTask // Task<Result<SqlTable, exn>>

// read results as scalar values
Sql.executeScalar // Sql
Sql.executeScalarSafe // Result<Sql, exn>
Sql.executeScalarAsync // Async<Sql>
Sql.executeScalarTask // Task<Sql>
Sql.executeTableSafeAsync // Async<Result<Sql, exn>>
Sql.executeTableSafeTask // Task<Result<Sql, exn>>

// execute and count rows affected
Sql.executeNonQuery // int
Sql.executeNonQueryAsync // Async<int>
Sql.executeNonQueryTask // Task<int>
Sql.executeNonQuerySafe // Result<int, exn>
Sql.executeNonQuerySafeAsync // Async<Result<int, exn>>
Sql.executeNonQuerySafeTask // Task<Result<int, exn>>
```

You can also try automated parsing:

```fs
type User = {
    UserId : int
    FirstName: string
    LastName: string
}

let getAllUsers() : User list =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM users"
    |> Sql.executeTable // SqlTable
    |> Sql.parseEachRow<User>
```

Though watch out, as this is a relatively new feature and still needs some time and love:
* The type parameter must be a record.
* Fields' names must match exactly columns headers.
* Only simple types are supported (see the definition of the "Sql" type).
* You can turn a field into an option if it's defined as "Nullable" in your table:

```fs
type User = {
    UserId : int
    FirstName : string
    LastName : string
    Nickname : string option
}
```

### To Run tests

Docker must be installed

```cmd
build(.cmd or sh) StartDatabase
build RunTests
build StopDatabase
```
