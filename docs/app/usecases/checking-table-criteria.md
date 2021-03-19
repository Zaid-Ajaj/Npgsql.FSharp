# Checking table criteria

Sometimes you want to know whether a table matches some criteria. Take this `users` table for example:
```sql
create table users (
    user_id serial primary key,
    username text not null,
    email text not null
);
```
When adding a new user, we first want to know whether the `username` or `email` already exist. Of course we can enforce this on the table level using the constraints `UNIQUE (username)` and `UNIQUE (email)` (read more about [UNIQUE](https://www.postgresqltutorial.com/postgresql-unique-constraint/)) but sometimes it is just easier to query the table before changing it and this pattern applies to any table criteria, not just column uniqueness:

```sql
SELECT EXISTS (
    SELECT 1
    FROM users
    WHERE username = 'username' OR email = 'email'
) AS email_or_username_already_exist
```
This pattern of [SELECT EXISTS (subquery)](https://www.postgresqltutorial.com/postgresql-exists/) returns a boolean expression, a scalar. We give it an alias in the query above.

Using this from F# as follows:
```fsharp
open Npgsql.FSharp

let [<Literal>] usernameOrEmailExistQuery = """
SELECT EXISTS (
    SELECT 1
    FROM users
    WHERE username = @username OR email = @email
) AS username_or_email_already_exist
"""

let usernameOrEmailAlreadyExist (connectionString, username, email) : bool =
    connectionString
    |> Sql.connect
    |> Sql.query usernameOrEmailExistQuery
    |> Sql.parameters [ "@username", Sql.text username; "@email", Sql.text email ]
    |> Sql.executeRow (fun read -> read.bool "username_or_email_already_exist")
```
Another way to achieve the same goal is using `COUNT(*)` to count how many rows actually match the criteria and checking that number is greater than zero:
```fsharp
open Npgsql.FSharp

let usernameOrEmailAlreadyExist (connectionString, username, email) : bool =
    connectionString
    |> Sql.connect
    |> Sql.query "SELECT COUNT(*) AS matches FROM users WHERE username = @username OR email = @email"
    |> Sql.parameters [ "@username", Sql.text username; "@email", Sql.text email ]
    |> Sql.executeRow (fun read -> read.int64 "matches" > 0L)
```
The former query using `SELECT EXISTS` is probably faster than `SELECT COUNT(*)` depending on your table indices but the idea is the same.

> Keep in mind that when using `Sql.executeRow` there should at least be one row! Otherwise the function will throw an exception. If your result set is expecting zero or more rows then using `Sql.execute (...) |> List.tryHead` is a better option for you.