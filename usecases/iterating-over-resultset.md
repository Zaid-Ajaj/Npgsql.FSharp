# Iterating over the result set

The function `Sql.execute` by default returns `'a list` when mapping each reader row into `'a`. This is fine for most cases but sometimes you don't want to get `list` back but something different such as a `ResizeArray<'a>` or even `Map<>` of some kind depending on the data you are trying to query from the database.

That is where the `Sql.iter` function comes into play. It allows you to _do something_ with the reader row. Here is an example returning a `ResizeArray<'a>` from a result set.

Take this table definition:
```sql
create table users (
    user_id serial primary key,
    username text not null,
    email text not null
);
```
Then read the `users` table as follows:
```fsharp
open Npgsql.FSharp

type User = { Id: int; Username: string; Email: string }

let readUsers (connectionString: string) : ResizeArray<User> =
    let users = ResizeArray<User>()
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT * FROM users"
    |> Sql.iter (fun read ->
        users.Add {
            Id = read.int "user_id"
            Username = read.text "username"
            Email = read.text "email"
        })

    users
```
The `Sql.iter` function takes a `RowReader -> unit` function.