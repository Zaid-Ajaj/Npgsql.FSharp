# Counting rows

Count the number of rows using the `COUNT(*)` function which will return a single row with a single value (scalar) which we can read using `Sql.executeRow`
```fsharp
open Npgsql.FSharp

let numberOfUsers (connectionString: string) : int64 =
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT COUNT(*) AS user_count FROM users"
    |> Sql.executeRow (fun read -> read.int64 "user_count")
```
Notice how we give the returned column an alias called `user_count` so that we are able to read it from `Sql.executeRow`.