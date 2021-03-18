# Dealing With Nullable Columns

A lot of the times, table columns can be nullable. This means that the values we read from the columns can be null sometimes. When reading these values from F# code, we want to map the values to `Option<'t>` instead. Take this table definition as an example:

```sql {highlight: [5, 6]}
create table users (
    user_id serial primary key,
    username text not null,
    email text not null,
    first_name text,
    last_name text
);
```
The columns `first_name` and `last_name` are nullable columns of type `text`. To read those column values, we use the reader functions that end with `orNone` as follows:
```fsharp {highlight: [7, 8, 20, 21]}
open Npgsql.FSharp

type User = {
    Id: int;
    Username: string;
    Email: string
    FirstName: string option
    LastName: string option
}

let readUsers (connectionString: string) : User list =
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT * FROM users"
    |> Sql.execute (fun read ->
        {
            Id = read.int "user_id"
            Username = read.text "username"
            Email = read.text "email"
            FirstName = read.textOrNone "first_name"
            LastName = read.textOrNone "last_name"
        })
```