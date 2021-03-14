# Inserting a row into a table

Adding new rows to a table is a matter of using an `INSERT` query and providing the required parameters. Required parameters are the ones for columns that are non-nullable. Take this table for example:
```sql
create table users (
    user_id serial primary key,
    username text not null,
    email text not null,
    active boolean
);
```
The `user_id` is serial and it is auto-generated so we don't need to provide a parameter for it. The `active` column is nullable which means if we don't provide a value, it just stays as null. We can insert a row into the `users` table as follows:
```fsharp
open Npgsql.FSharp

let addUser (connectionString: string, username:string, email:string) : int =
    connectionString
    |> Sql.connect
    |> Sql.query "INSERT INTO users (username, email) VALUES (@username, @email)"
    |> Sql.parameters [ "@username", Sql.text username; "@email", Sql.text email ]
    |> Sql.executeNonQuery
```
Because we run the query using `Sql.executeNonQuery`, we get the an integer back that represents the number of _affected rows_. In this case, if the query is succesfull, the affected rows are always 1 because we are inserting a single row.

