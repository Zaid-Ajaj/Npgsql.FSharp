# Migrating from V3 to V4

### Problems with v3
In the latest versions of Npgsql.FSharp v3 we introduced two namespaces:
 - `Npgsql.FSharp`
 - `Npgsql.FSharp.Tasks`

In V3, `Npgsql.FSharp` had synchronous functions such as `Sql.execute` return `Result<'t list, exn>` and the asynchronous equivalent return `Async<Result<'t, exn>>`. The namespace `Npgsql.FSharp.Tasks` on the other hand had the same functions but simpler where the synchronous `Sql.execute` would simply return `'t list` and doesn't do any error catching into `Result` and the asynchronous `Sql.executeAsync` function would return `Task<' list>`.

This was all a bit too confusing because the `Npgsql.FSharp` namespace sounded like the default namespace but it was a bit too much in the sense that users didn't always need to wrap failing operations into `Result` and `Async` was mostly not really needed and had to be converted to `Task` when used inside web applications. This is why the other namespace emerged to solve these issues.

### Changes in v4
Now in v4 the namespace `Npgsql.FSharp.Tasks` will become the default and only namespace `Npgsql.FSharp` and will expose functions that don't return `Result` nor `Async`. Instead just values of `'t` and `Task<'t>`. This simplifies the usage a lot and brings the public API surface to a minimum.

In v4 we also no longer have `Sql.transaction` because it was redundant. Using `Sql.existingConnection` was sufficient because once you initiated a transaction on a connection, the connection becomes bound with that transaction.

In v4 some reader functions are removed that used to expose low-level `Npgsql` types such as `timestamptz`, `timestamp`, `date` etc. as these would return `Npgsql.NpgsqlDateTime` which I believe would confuse users that already have functions `datetimeOffset` and `dateTime` to read timestamp and date values from the database. Moreover, if you need these functions, you can always access the underlying `NpgsqlReader` from the `RowReader` when iterating over the rows.

This release also comes with this documentation website that you are reading which will serve more elaborated and detailed use cases that are easy to understand for beginners. I hope you can help me add more useful examples to it if you have had to work with non-trivial problems working with Npgsql.FSharp.