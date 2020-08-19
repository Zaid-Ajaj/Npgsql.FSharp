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

### `Sql.execute`: Execute query and read results as table then map the results
The main function to execute queries and return a list of a results is `Sql.execute`:
```fs
open Npgsql.FSharp

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
The function is *always* safe and will return you `Result<'t list, exn>` from the execution.

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
Then you can use `defaultArg` or other functions from the `Option` to provide default values when needed.
### Make the reading async using `Sql.executeAsync`
The exact definition is used, except that `Sql.execute` becomes `Sql.executeAsync`
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

### `Sql.parameters`: Parameterized queries
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
### `Sql.executeSingleRow`: Execute a query and read a single row back
Use the function `Sql.executeRow` or its async counter part to read a single row of the output result. For example, to read the number of rows from a table:
```fs
let numberOfUsers() : Result<int64, exn> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT COUNT(*) as user_count FROM users"
    |> Sql.executeSingleRow (fun read -> read.int64 "user_count")
```
> Notice here we alias the result of `COUNT(*)` as a column named `user_count`. This is recommended when reading scalar result sets so that we work against a named column instead of its index.

### `Sql.executeTransaction`: Execute multiple inserts or updates in a single transaction
Both queries in the example below are executed within a single transaction and if one of them fails, the entire transaction is rolled back.
```fs
connectionString
|> Sql.connect
|> Sql.executeTransaction
    [
        // This query is executed 3 times
        // using three different set of parameters
        "INSERT INTO ... VALUES (@number)", [
            [ "@number", Sql.int 1 ]
            [ "@number", Sql.int 2 ]
            [ "@number", Sql.int 3 ]
        ]

        // This query is executed once
        "UPDATE ... SET meta = @meta",  [
           [ "@meta", Sql.text value ]
        ]
   ]
```
### `Sql.executeNonQuery`: Returns number of affected rows from statement
Use the function `Sql.executeNonQuery` or its async counter part to get the number of affected rows from a query. Like always, the function is safe by default and returns `Result<int, exn>` as output.
```fs
let getAllUsers() : Result<int, exn> =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "DELETE FROM users WHERE is_active = @is_active"
    |> Sql.parameters [ "is_active", Sql.bit false ]
    |> Sql.executeNonQuery
```

### `Sql.iter`: Iterating through the result set
The functions `Sql.execute` and `Sql.executeAsync` by default return you a `list<'t>` type which for many cases works quite well. However, for really large datasets (> 100K of rows) using F# lists might not be ideal for performance. This library provides the function `Sql.iter` which allows you to *do* something with the row reader like adding rows to `ResizeArray<'t>` as follows without using an intermediate F# `list<'t>`:
```fs
let filmTitles(connectionString: string) =
    let titles = ResizeArray<string>()
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT title FROM film"
    |> Sql.iter (fun read -> titles.Add(read.text "title"))
    |> function
        | Ok() -> Ok titles
        | Error error -> Error error
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

### Reading values from the underlying `NpgsqlDataReader`
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

### Migrating from reflection-based libraries

If you are migrating from libraries that use reflection to map database results to objects, it might seem like manually creating your mapping functions is a lot of work.  Our position is that this work is worthwhile in terms of

* clarity,
* maintainability, and
* flexibility

However, if you do need some automated reflection-based generation, writing such a wrapper is not impossible. Something like this gets you almost all of the way there:

```fs
// generate a function of type RowReader -> 't that looks for fields to map based on lowercase field names
let autoGeneratedRecordReader<'t> =
    let createRecord = FSharpValue.PreComputeRecordConstructor typeof<'t>
    let make values = createRecord values :?> 't
    let fields = FSharpType.GetRecordFields typeof<'t> |> Array.map (fun p -> p.Name, p.PropertyType)

    let readField (r: RowReader) (n: string) (t: System.Type) =
        if   t = typeof<int> then r.int n |> box
        elif t = typeof<int option> then r.intOrNone n |> box
        elif t = typeof<int16> then r.int16 n |> box
        elif t = typeof<int16 option> then r.int16OrNone n |> box
        elif t = typeof<int []> then r.intArray n |> box
        elif t = typeof<int [] option> then r.intArrayOrNone n |> box
        elif t = typeof<string []> then r.stringArray n |> box
        elif t = typeof<string [] option> then r.stringArrayOrNone n |> box
        elif t = typeof<int64> then r.int64 n |> box
        elif t = typeof<int64 option> then r.int64OrNone n |> box
        elif t = typeof<string> then r.string n |> box
        elif t = typeof<string option> then r.stringOrNone n |> box
        elif t = typeof<bool> then r.bool n |> box
        elif t = typeof<bool option> then r.boolOrNone n |> box
        elif t = typeof<decimal> then r.decimal n |> box
        elif t = typeof<decimal option> then r.decimalOrNone n |> box
        elif t = typeof<double> then r.double n |> box
        elif t = typeof<double option> then r.doubleOrNone n |> box
        elif t = typeof<DateTime> then r.dateTime n |> box
        elif t = typeof<DateTime option> then r.dateTimeOrNone n |> box
        elif t = typeof<Guid> then r.uuid n |> box
        elif t = typeof<Guid option> then r.uuidOrNone n |> box
        elif t = typeof<byte[]> then r.bytea n |> box
        elif t = typeof<byte[] option> then r.byteaOrNone n |> box
        elif t = typeof<float> then r.float n |> box
        elif t = typeof<float option> then r.floatOrNone n |> box
        elif t = typeof<Guid []> then r.uuidArray n |> box
        elif t = typeof<Guid [] option> then r.uuidArrayOrNone n |> box
        else
            failwithf "Could not read column '%s' as %s" n t.FullName

    fun (r: RowReader) ->
        let values =
            [| for (name, ty) in fields do readField r (name.ToLowerInvariant()) ty |]
        make values
```

This reader maps the fields' lower-case name only, but if you have custom naming requirements you can of course alter that to fit your circumstances.

It would be used something like

```fs
type Car = { make: string; model: string; year: int }

let all_cars =
    defaultConnection
    |> Sql.connectFromConfig
    |> Sql.query "SELECT * FROM cars"
    |> Sql.execute autoGeneratedRecordReader<Car>
```
