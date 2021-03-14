# Use an existing connection

By default, when you use the `Sql` module starting from a connection string like this:
```fsharp
connectionString
|> Sql.connect
|> Sql.query "..."
|> Sql.execute (...)
```
The library will create a connection internally of type `NpgsqlConnection`, use it and eventually dispose of it automatically so you don't have to think about these things. Don't worry about opening and closing too many connections, the underlying `Npgsql` library has an internal connection pool that reuses connections efficiently.

You can however, create your own `NpgsqlConnection` and use it with the `Sql` module. For that you should use the `Sql.existingConnection`function as follows:
```fsharp
// construct the connection
use connection = new NpgsqlConnection(connectionString)
connection.Open()
// use it here
connection
|> Sql.existingConnection
|> Sql.query "SELECT * FROM users"
|> Sql.execute (fun read -> read.int "user_id")
```
When using an existing connection, this library will _NOT_ dispose of the connection. Instead you do that yourself because you created it. In the example above, we create the connection with the `use` keyword which will automatically dispose of the connection at the end of the current scope.

It is equivalent to the following
```fsharp
let connection = new NpgsqlConnection(connectionString)
try
    connection.Open()
    connection
    |> Sql.existingConnection
    |> Sql.query "SELECT * FROM users"
    |> Sql.execute (fun read -> read.int "user_id")
finally
    connection.Dispose()
```
Learn more about the `use` keyword from the [official docs](https://docs.microsoft.com/en-us/dotnet/fsharp/language-reference/resource-management-the-use-keyword).