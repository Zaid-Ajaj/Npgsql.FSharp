### Querying a table

Given the following database table:
```sql
create table users (
    user_id serial primary key,
    username text not null,
    email text not null
);
```
You can query the `users` table as follows:
```fsharp
open Npgsql.FSharp

type User = { Id: int; Username: string; Email: string }

let readUsers (connectionString: string) : User list =
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT * FROM users"
    |> Sql.execute (fun read ->
        {
            Id = read.int "user_id"
            Username = read.text "username"
            Email = read.text "email"
        })
```
The function `readUsers` takes the connection string as a parameter. You can use it as follows:

```fsharp
let connectionString = "..."
let users = readUsers connectionString
for user in users do
    printfn "User(%d) -> {%s}" user.Id user.Email
```