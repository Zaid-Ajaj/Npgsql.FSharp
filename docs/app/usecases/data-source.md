# Use a data source

.NET 7 implemented a new `DbDataSource` abstraction to serve as an intelligent connection factory. Npgsql's implementation of this abstraction is `NpgsqlDataSource`. There are several ways to construct a data source, but the most straightforward is to use its static `Create` method with a connection string:
```fsharp
use dataSource = NpgsqlDataSource.Create(connectionString)
```
_Note: `NpgsqlDataSource` is disposable, but its intended use is for one instance to be used per application and data source. If you are creating application-lifetime objects, you may want to use `let` rather than `use`._

Once you have an `NpgsqlDataSource` instance, you can provide it to the library, and it will use that source to create connections as needed. The library also handles disposing these connections.

```fsharp
// construct the data source
use dataSource = NpgsqlDataSource.Create(connectionString)
// use it here
dataSource
|> Sql.fromDataSource
|> Sql.query "SELECT * FROM users"
|> Sql.execute (fun read -> read.int "user_id")
```

To learn more about `DbDataSource`, you can [read its documentation](https://learn.microsoft.com/en-us/dotnet/api/system.data.common.dbdatasource) or [watch a video about it](https://youtu.be/vRUtHeUpU44?t=154) from Microsoft's .NET data team. For `NpgsqlDataSource` specifically, you can review [the documentation](https://www.npgsql.org/doc/basic-usage.html#data-source) or [the object's API](https://www.npgsql.org/doc/api/Npgsql.NpgsqlDataSource.html).
