# Npgsql.FSharp [![Nuget](https://img.shields.io/nuget/v/Npgsql.FSharp.svg?colorB=green)](https://www.nuget.org/packages/Npgsql.FSharp)

Thin F# wrapper for Npqsql, the PostgreSQL database driver. 

This wrapper still works with *raw* SQL but it maps the data from the database into the `Sql` data structure making it easy to pattern match and transform the results.

TODO:
 - More `Sql` types that map to Postgres data types
 - More config options when for connection string builder

Given the types:
```fs
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
  | (* TODO: other data types *)

// A row is a list of key/value pairs
type SqlRow = list<string * Sql>

// A table is list of rows
type SqlTable = list<SqlRow>
```
### Configure the connection string
```fs
open Npqsql.FSharp

// construct the connection string
let defaultConnection : string = 
    Sql.host "localhost"
    |> Sql.port 5432
    |> Sql.username "user"
    |> Sql.password "password"
    |> Sql.database "app_db"
    |> Sql.str
```

### Execute query and read results as table then map the results
```fs
type User = {
    UserId : int
    FirstName: string
    LastName: string
}

let getAllUsers() : User list = 
    defaultConnection
    |> Sql.connect
    |> Sql.query "SELECT * FROM \"users\""
    |> Sql.executeTable // SqlTable
    |> Sql.mapEachRow (function
        | [ "user_id", Int id
            "first_name", String fname
            "last_name", String lname ] -> 
          let user =  
            { UserId = id; 
              FirstName = fname; 
              LastName = lname }
          Some user
        | _ -> None)
```
### Execute a function with parameters
```fs
/// Check whether or not a user exists by his username
let userExists (name: string) : bool = 
    defaultConnection
    |> Sql.connect
    |> Sql.func "user_exists"
    |> Sql.parameters ["username", String name]
    |> Sql.executeScalar // Sql
    |> Sql.toBool
```
### Retrieve single value safely
```fs
// ping the database
let serverTime() : Option<DateTime> = 
    defaultConnection
    |> Sql.connect
    |> Sql.query "SELECT NOW()"
    |> Sql.executeScalarSafe
    |> function
        | Ok (Date time) -> Some time
        | _ -> None
```
### Retrieve single value safely asynchronously
```fs
// ping the database
let serverTime() : Async<Option<DateTime>> =
    async {
        let! result =
          defaultConnection
          |> Sql.connect
          |> Sql.query "SELECT NOW()"
          |> Sql.executeScalarSafeAsync
        
        match result with
        | Ok (Date time) -> return Some time
        | otherwise -> return None
    }
```
### Batch queries in a single roundtrip to the database
```fs
defaultConnection
|> Sql.connect
|> Sql.queryMany 
    ["SELECT * FROM \"users\""
     "SELECT * FROM \"products\""]
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
Sql.executeTableAsync // Async<SqlTable>
Sql.executeTableSafeAsync // Async<Result<SqlTable, exn>>

// read results as scalar values
Sql.executeScalar // Sql
Sql.executeScalarSafe // Result<Sql, exn> 
Sql.executeScalarAsync // Async<Sql>
Sql.executeTableSafeAsync // Async<Result<Sql, exn>>

// execute and count rows affected
Sql.executeNonQuery // int
Sql.executeNonQueryAsync // Async<int>
Sql.executeNonQuerySafe // Result<int, exn>
Sql.executeNonQuerySafeAsync // Async<Result<int, exn>>
```