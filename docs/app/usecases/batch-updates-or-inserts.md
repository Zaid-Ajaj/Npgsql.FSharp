# Batch updates or inserts

Using `Sql.executeTransaction` you can execute queries that execute multiple times per parameter set as follows:
```fsharp
open Npgsql.FSharp

let executeBatch(connectionString: string) =
    connectionString
    |> Sql.connect
    |> Sql.executeTransaction
        [
            // This query is executed 3 times
            // using three different set of parameters
            "INSERT INTO totals (number) VALUES (@number)", [
                [ "@number", Sql.int 1 ]
                [ "@number", Sql.int 2 ]
                [ "@number", Sql.int 3 ]
            ]

            // This query is executed once
            "UPDATE posts SET meta = @meta WHERE post_id = @post_id",  [
                [ "@meta", Sql.text "metavalue"; "@post_id", Sql.int 42 ]
            ]
        ]
```
The syntax above executes two queries multiple times per parameter set
  - `INSERT INTO totals (number) VALUES (@number)` executed 3 times
  - `UPDATE posts SET meta = @meta WHERE post_id = @post_id` executed once

The nice thing about `Sql.executeTransaction` is that it runs all queries within a [transaction](https://www.postgresql.org/docs/8.3/tutorial-transactions.html). If _any_ of the operations fails due to database integrity violations, the entire thing is rolled back. Basically an _all or nothing_ operation.