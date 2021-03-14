# Returing inserted rows

When adding rows into a table, we usually run `Sql.executeNonQuery` which returns the number of rows added. However, sometimes we actually want to retrieve the added rows. In PostgreSQL, this is super simple using the [RETURNING CLAUSE](). Using `RETURNING` in an `INSERT` , `UPDATE` or `DELETE` query makes it _return_ the actual affected rows instead of just the number of affected rows.

Take this `users` table.
```sql
create table users (
    user_id serial primary key,
    username text not null,
    email text not null
);
```
When we add a new user, the `user_id` is automatically generated and we want to return that added user immediately:
```fsharp {highlight: [10]}
open Npgsql.FSharp

type User = { Id: int; Username: string; Email: string }

let addUser (connectionString: string, username:string, email:string) : User =
    connectionString
    |> Sql.connect
    |> Sql.query "INSERT INTO users (username, email) VALUES (@username, @email) RETURNING *"
    |> Sql.parameters [ "@username", Sql.text username; "@email", Sql.text email ]
    |> Sql.executeRow (fun read ->
        {
            Id = read.int "user_id" // the generated user id
            Username = read.text "username"
            Email = read.text "email"
        })
```
Besides ending the query with `RETURNING *` we also use the `Sql.executeRow` function which reads a single row from the returned result set. This works perfectly in case of an insert because we know a succesfull query will return one row with the data of the added user.

