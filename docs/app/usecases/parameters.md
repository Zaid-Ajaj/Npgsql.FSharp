# Queries With Parameters

Use `Sql.parameters` to provide a list of parameters to your query. Take the following table definition:
```sql
create table users (
    user_id serial primary key,
    username text not null,
    email text not null,
    active boolean not null
);
```
You can query the users where the value is `active` column is true:
```fsharp {highlight: [9]}
open Npgsql.FSharp

type User = { Id: int; Username: string; Email: string }

let activeUsers (connectionString: string) : User list =
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT * FROM users WHERE active = @active"
    |> Sql.parameters [ "@active", Sql.bool true ]
    |> Sql.execute (fun read ->
        {
            Id = read.int "user_id"
            Username = read.text "username"
            Email = read.text "email"
        })
```
The `Sql.parameters` function takes a list of types. You can provide multiple parameters as follows:
```fsharp
Sql.parameters [ "@active", Sql.bool true; "@user_id", Sql.int 42 ]
```