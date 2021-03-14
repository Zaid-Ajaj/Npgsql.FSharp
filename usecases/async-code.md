# Asynchronous operations

Besides the synchronous functions, the library also provides their asynchronous equivalent:

  - `Sql.execute` -> `Sql.executeAsync`
  - `Sql.executeNonQuery` -> `Sql.executeNonQueryAsync`
  - `Sql.executeRow` -> `Sql.executeRowAsync`
  - `Sql.iter` -> `Sql.iterAsync`
  - `Sql.executeTransaction` -> `Sql.executeTransactionAsync`

Typically you would use the asynchoronous functions when you are running your code inside a `task { }` or `async { }` computation expressions. Asynchronous functions make better use of threads, especially in the context of web applications. Moreover, asynchronous operations also support _cancellation_ via [Cancellation Tokens](https://docs.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken?view=net-5.0).

Take this table definition for example
```sql
create table users (
    user_id serial primary key,
    username text not null,
    email text not null
);
```
You can query the table asynchoronously as follows:
```fsharp
open Npgsql.FSharp
open System.Threading.Tasks

type User = { Id: int; Username: string; Email: string }

let readUsers (connectionString: string) : Task<User list> =
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT * FROM users"
    |> Sql.executeAsync (fun read ->
        {
            Id = read.int "user_id"
            Username = read.text "username"
            Email = read.text "email"
        })
```
Use the function inside a `task { }` expression later on:
```fsharp
task {
    let connectionString = "..."
    let! users = readUsers(connectionString)
    return users
}
```
Use the function from inside an `async { }` expression:
```fsharp {highlight: [3]}
async {
    let connectionString = "..."
    let! users = readUsers(connectionString) |> Async.AwaitTask
    return users
}
```
### Cancellation

Asynchoronous operations can be cancelled. This is useful for example in web applications where the user has triggered an expensive operation but decides to close the tab because they were waiting for too long. In the ideal case, you want the stop the expensive operation once the request is aborted. Typically, the web framework you are using will give you a cancellation token that gets "cancelled" when the request connection is aborted. You can use that cancellation token as part of the asynchoronous database operations:
```fsharp {highlight: [9]}
open Npgsql.FSharp
open System.Threading
open System.Threading.Tasks

type User = { Id: int; Username: string; Email: string }

let readUsers (connectionString: string, token: CancellationToken) : Task<User list> =
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT * FROM users"
    |> Sql.cancellationToken token
    |> Sql.executeAsync (fun read ->
        {
            Id = read.int "user_id"
            Username = read.text "username"
            Email = read.text "email"
        })
```