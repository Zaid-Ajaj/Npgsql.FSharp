# Acquire connection string

The very first thing you need to connect to your PostgreSQL database is a _connection string_. This connection string specifies connection configuration such as where the database server is hosted and which database to connect to among other things. Here are several ways of how a connection string can be constructed:

```fsharp
// (1) hardcoded
let connectionString = "Host=localhost; Database=dvdrental; Username=postgres; Password=postgres;"
// the library also accepts URI postgres connection format (NOTE: not all query string parameters are converted)
let connectionString = "postgres://username:password@localhost/dvdrental";
// (2) from environment variables
let connectionString = System.Environment.GetEnvironmentVariable "DATABASE_CONNECTION_STRING"
// (3) using the connection string builder API
let connectionString : string =
    Sql.host "localhost"
    |> Sql.database "dvdrental"
    |> Sql.username "postgres"
    |> Sql.password "postgres"
    |> Sql.port 5432
    |> Sql.formatConnectionString
```