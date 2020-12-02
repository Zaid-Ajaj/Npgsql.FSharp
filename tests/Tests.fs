module Main

open Expecto
open Npgsql.FSharp
open System
open ThrowawayDb.Postgres
open Npgsql
open System.Data
open System.Linq

type FsTest = {
    test_id: int
    test_name: string
}

type TimeSpanTest = {
    id: int
    at: TimeSpan
}

type StringArrayTest = {
    id: int
    values:string array
}

type IntArrayTest = {
    id: int
    integers: int array
}

type UuidArrayTest = {
    id: int
    guids: Guid array
}

type PointTest = {
    id: int
    point: NpgsqlTypes.NpgsqlPoint
}

type NullablePointTest = {
    id: int
    nullablepoint: NpgsqlTypes.NpgsqlPoint option
}

let buildDatabaseConnection handleInfinity : ThrowawayDatabase =
    let createFSharpTable = "create table if not exists fsharp_test (test_id int, test_name text)"
    let createJsonbTable = "create table if not exists data_with_jsonb (data jsonb)"
    let createTimestampzTable = "create table if not exists timestampz_test (version integer, date1 timestamptz, date2 timestamptz)"
    let createTimespanTable = "create table if not exists timespan_test (id int, at time without time zone)"
    let createStringArrayTable = "create table if not exists string_array_test (id int, values text [])"
    let createUuidArrayTable = "create table if not exists uuid_array_test (id int, values uuid [])"
    let createIntArrayTable = "create table if not exists int_array_test (id int, integers int [])"
    let createPointTable = "create table if not exists point_test (id int, test_point point)"
    let createExtensionHStore = "create extension if not exists hstore"
    let createExtensionUuid = "create extension if not exists \"uuid-ossp\""

    // Travis CI uses an empty string for the password of the database
    let databasePassword =
        let runningTravis = Environment.GetEnvironmentVariable "TESTING_IN_TRAVISCI"
        if isNull runningTravis || String.IsNullOrWhiteSpace runningTravis
        then "postgres" // for local tests
        else "" // for Travis CI

    let connection =
        Sql.host "localhost"
        |> Sql.port 5432
        |> Sql.username "postgres"
        |> Sql.password databasePassword
        |> Sql.convertInfinityDateTime handleInfinity
        |> Sql.formatConnectionString

    let database = ThrowawayDatabase.Create(connection)

    database.ConnectionString
    |> Sql.connect
    |> Sql.executeTransaction [
        createFSharpTable, [ ]
        createJsonbTable, [ ]
        createTimestampzTable, [ ]
        createTimespanTable, [ ]
        createStringArrayTable, [ ]
        createExtensionHStore, [ ]
        createIntArrayTable, [ ]
        createExtensionUuid, [ ]
        createUuidArrayTable, []
        createPointTable, []
    ]
    |> ignore

    database

let buildDatabase() = buildDatabaseConnection false
let buildInfinityDatabase() = buildDatabaseConnection true

let tests =
    testList "Integration tests" [
        testList "RowReader tests used in Sql.read and Sql.readAsync" [
            test "Sql.executeTransaction works" {
                use db = buildDatabase()
                Sql.connect db.ConnectionString
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQuery
                |> ignore

                Sql.connect db.ConnectionString
                |> Sql.executeTransaction [
                    "INSERT INTO users (username, active, salary) VALUES (@username, @active, @salary)", [
                        [ ("@username", Sql.text "first"); ("active", Sql.bit true); ("salary", Sql.money 1.0M)  ]
                        [ ("@username", Sql.text "second"); ("active", Sql.bit false); ("salary", Sql.money 1.0M) ]
                        [ ("@username", Sql.text "third"); ("active", Sql.bit true);("salary", Sql.money 1.0M) ]
                    ]
                ]
                |> ignore

                let expected = [
                    {| userId = 1; username = "first"; active = true; salary = 1.0M  |}
                    {| userId = 2; username = "second"; active = false ; salary = 1.0M |}
                    {| userId = 3; username = "third"; active = true ; salary = 1.0M |}
                ]

                Sql.connect db.ConnectionString
                |> Sql.query "SELECT * FROM users"
                |> Sql.execute (fun read ->
                    {|
                        userId = read.int "user_id"
                        username = read.string "username"
                        active = read.bool "active"
                        salary = read.decimal "salary"
                    |})
                |> function
                | Error err -> raise err
                | Ok users -> Expect.equal users expected "Users can be read correctly"
            }

            test "Sql.executeRow works" {
                use db = buildDatabase()
                db.ConnectionString
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                db.ConnectionString
                |> Sql.connect
                |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                |> Sql.executeRow (fun read -> read.int64 "user_count")
                |> function
                     | Ok count -> Expect.equal count 0L "Count is zero"
                     | Error err -> raise err
            }

            test "Sql.iter works" {
                use db = buildDatabase()
                db.ConnectionString
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                let mutable count = -1
                db.ConnectionString
                |> Sql.connect
                |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                |> Sql.iter (fun read -> count <- read.int "user_count")
                |> function
                    | Ok() -> Expect.equal count 0 "The count is zero"
                    | Error err -> raise err
            }

            test "Manual transaction handling works with Sql.executeNonQuery" {
                use db = buildDatabase()

                db.ConnectionString
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                use connection = new NpgsqlConnection(db.ConnectionString)
                connection.Open()
                use transaction = connection.BeginTransaction()
                let results = ResizeArray()

                for username in ["John"; "Jane"] do
                    Sql.transaction transaction
                    |> Sql.query "INSERT INTO users (username) VALUES(@username)"
                    |> Sql.parameters [ "@username", Sql.text username ]
                    |> Sql.executeNonQuery
                    |> results.Add

                if (results.Any(fun result -> match result with | Error _ -> true | _ -> false)) then
                    transaction.Rollback()
                else
                    transaction.Commit()

                db.ConnectionString
                |> Sql.connect
                |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                |> Sql.executeRow (fun read -> read.int "user_count")
                |> function
                    | Error error -> raise error
                    | Ok count -> Expect.equal 2 count "There are 2 users added"
            }

            test "Manual transaction handling works with Sql.executeNonQuery and can be rolled back" {
                use db = buildDatabase()

                db.ConnectionString
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                use connection = new NpgsqlConnection(db.ConnectionString)
                connection.Open()
                use transaction = connection.BeginTransaction()
                let results = ResizeArray()

                for username in [Some "John"; Some "Jane"; None] do
                    Sql.transaction transaction
                    |> Sql.query "INSERT INTO users (username) VALUES(@username)"
                    |> Sql.parameters [ "@username", Sql.textOrNone username ]
                    |> Sql.executeNonQuery
                    |> results.Add

                if (results.Any(fun result -> match result with | Error _ -> true | _ -> false)) then
                    transaction.Rollback()
                else
                    transaction.Commit()

                db.ConnectionString
                |> Sql.connect
                |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                |> Sql.executeRow (fun read -> read.int "user_count")
                |> function
                    | Error error -> raise error
                    | Ok count -> Expect.equal 0 count "There are 0 users added because the transaction is rolled back"
            }

            testAsync "Sql.iterAsync works" {
                use db = buildDatabase()
                db.ConnectionString
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                let mutable count = -1

                let! result =
                    db.ConnectionString
                    |> Sql.connect
                    |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                    |> Sql.iterAsync (fun read -> count <- read.int "user_count")

                match result with
                | Ok() -> Expect.equal count 0 "The count is zero"
                | Error err -> raise err
            }


            test "Reading count as int works" {
                use db = buildDatabase()
                db.ConnectionString
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                db.ConnectionString
                |> Sql.connect
                |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                |> Sql.executeRow (fun read -> read.int "user_count")
                |> function
                     | Ok count -> Expect.equal count 0 "Count is zero"
                     | Error err -> raise err
            }

            test "Paramater names can contain trailing spaces" {
                use db = buildDatabase()
                use connection = new NpgsqlConnection(db.ConnectionString)
                connection.Open()
                Sql.existingConnection connection
                |> Sql.query "CREATE TABLE test (test_id serial primary key, integers int [])"
                |> Sql.executeNonQuery
                |> ignore


                let result =
                    Sql.existingConnection connection
                    |> Sql.executeTransaction [
                        "INSERT INTO test (integers) VALUES (@integers)", [
                            [ ("@integers        ", Sql.intArray [| 1; 3; 7; |] ) ]
                            [ ("    @integers"    , Sql.intArray [| 1; 3; 7; |] ) ]
                            [ ("   integers      ", Sql.intArray [| 1; 3; 7; |] ) ]
                        ]
                    ]

                Expect.equal result (Ok [1; 1; 1;]) "paramaters can contain trailing spaces"
            }

            testAsync "Sql.executeRowAsync works" {
                use db = buildDatabase()
                db.ConnectionString
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                let! content =
                    db.ConnectionString
                    |> Sql.connect
                    |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                    |> Sql.executeRowAsync (fun read -> read.int64 "user_count")

                match content with
                | Ok count -> Expect.equal count 0L "Count is zero"
                | Error err -> raise err
            }

            test "Sql.executeTransaction doesn't error out on parameterized queries with empty parameter sets" {
                use db = buildDatabase()
                db.ConnectionString
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                db.ConnectionString
                |> Sql.connect
                |> Sql.executeTransaction [
                    "INSERT INTO users (username) VALUES (@username)", [ ]
                ]
                |> function
                    | Error error -> raise error
                    | Ok affectedRows -> Expect.equal affectedRows [0] "No rows will be affected"
            }

            testAsync "Sql.executeTransactionAsync doesn't error out on parameterized queries with empty parameter sets" {
                use db = buildDatabase()
                db.ConnectionString
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                let! affectedRows =
                    db.ConnectionString
                    |> Sql.connect
                    |> Sql.executeTransactionAsync [
                        "INSERT INTO users (username) VALUES (@username)", [ ]
                    ]

                match affectedRows with
                | Error error -> raise error
                | Ok affectedRows -> Expect.equal affectedRows [0] "No rows will be affected"
            }

            test "Sql.executeTransaction works with existing open connection" {
                use db = buildDatabase()
                use connection = new NpgsqlConnection(db.ConnectionString)
                connection.Open()
                Sql.existingConnection connection
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQuery
                |> ignore

                Sql.existingConnection connection
                |> Sql.executeTransaction [
                    "INSERT INTO users (username, active, salary) VALUES (@username, @active, @salary)", [
                        [ ("@username", Sql.text "first"); ("active", Sql.bit true); ("salary", Sql.money 1.0M)  ]
                        [ ("@username", Sql.text "second"); ("active", Sql.bit false); ("salary", Sql.money 1.0M) ]
                        [ ("@username", Sql.text "third"); ("active", Sql.bit true);("salary", Sql.money 1.0M) ]
                    ]
                ]
                |> ignore

                let expected = [
                    {| userId = 1; username = "first"; active = true; salary = 1.0M  |}
                    {| userId = 2; username = "second"; active = false ; salary = 1.0M |}
                    {| userId = 3; username = "third"; active = true ; salary = 1.0M |}
                ]

                Sql.existingConnection connection
                |> Sql.query "SELECT * FROM users"
                |> Sql.execute (fun read ->
                    {|
                        userId = read.int "user_id"
                        username = read.string "username"
                        active = read.bool "active"
                        salary = read.decimal "salary"
                    |})
                |> function
                | Error err -> raise err
                | Ok users -> Expect.equal users expected "Users can be read correctly"
            }

            test "Sql.executeTransaction works with existing connection" {
                use db = buildDatabase()
                use connection = new NpgsqlConnection(db.ConnectionString)
                Sql.existingConnection connection
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQuery
                |> ignore

                Sql.existingConnection connection
                |> Sql.executeTransaction [
                    "INSERT INTO users (username, active, salary) VALUES (@username, @active, @salary)", [
                        [ ("@username", Sql.text "first"); ("active", Sql.bit true); ("salary", Sql.money 1.0M)  ]
                        [ ("@username", Sql.text "second"); ("active", Sql.bit false); ("salary", Sql.money 1.0M) ]
                        [ ("@username", Sql.text "third"); ("active", Sql.bit true);("salary", Sql.money 1.0M) ]
                    ]
                ]
                |> ignore

                let expected = [
                    {| userId = 1; username = "first"; active = true; salary = 1.0M  |}
                    {| userId = 2; username = "second"; active = false ; salary = 1.0M |}
                    {| userId = 3; username = "third"; active = true ; salary = 1.0M |}
                ]

                Sql.existingConnection connection
                |> Sql.query "SELECT * FROM users"
                |> Sql.execute (fun read ->
                    {|
                        userId = read.int "user_id"
                        username = read.string "username"
                        active = read.bool "active"
                        salary = read.decimal "salary"
                    |})
                |> function
                | Error err -> raise err
                | Ok users -> Expect.equal users expected "Users can be read correctly"
            }

            test "Sql.executeTransaction leaves existing connection open" {
                use db = buildDatabase()
                use connection = new NpgsqlConnection(db.ConnectionString)
                connection.Open()
                Sql.existingConnection connection
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQuery
                |> ignore

                Sql.existingConnection connection
                |> Sql.executeTransaction [
                    "INSERT INTO users (username, active, salary) VALUES (@username, @active, @salary)", [
                        [ ("@username", Sql.text "first"); ("active", Sql.bit true); ("salary", Sql.money 1.0M)  ]
                        [ ("@username", Sql.text "second"); ("active", Sql.bit false); ("salary", Sql.money 1.0M) ]
                        [ ("@username", Sql.text "third"); ("active", Sql.bit true);("salary", Sql.money 1.0M) ]
                    ]
                ]
                |> ignore

                let expected = [
                    {| userId = 1; username = "first"; active = true; salary = 1.0M  |}
                    {| userId = 2; username = "second"; active = false ; salary = 1.0M |}
                    {| userId = 3; username = "third"; active = true ; salary = 1.0M |}
                ]

                Sql.existingConnection connection
                |> Sql.query "SELECT * FROM users"
                |> Sql.execute (fun read ->
                    {|
                        userId = read.int "user_id"
                        username = read.string "username"
                        active = read.bool "active"
                        salary = read.decimal "salary"
                    |})
                |> ignore

                Expect.equal ConnectionState.Open connection.State "Check existing connection is still open after executeTransaction"
            }

            test "Sql.executeNonQuery works" {
                use db = buildDatabase()
                Sql.connect db.ConnectionString
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQuery
                |> ignore

                Sql.connect db.ConnectionString
                |> Sql.executeTransaction [
                    "INSERT INTO users (username, active, salary) VALUES (@username, @active, @salary)", [
                        [ ("@username", Sql.text "first"); ("active", Sql.bit true); ("salary", Sql.money 1.0M)  ]
                        [ ("@username", Sql.text "second"); ("active", Sql.bit false); ("salary", Sql.money 1.0M) ]
                        [ ("@username", Sql.text "third"); ("active", Sql.bit true);("salary", Sql.money 1.0M) ]
                    ]
                ]
                |> ignore

                Sql.connect db.ConnectionString
                |> Sql.query "DELETE FROM users"
                |> Sql.executeNonQuery
                |> function
                    | Error error -> raise error
                    | Ok rowsAffected -> Expect.equal 3 rowsAffected "Three entries are deleted"
            }

            test "Sql.executeNonQuery works with existing connection" {
                use db = buildDatabase()
                use connection = new NpgsqlConnection(db.ConnectionString)
                connection.Open()
                Sql.existingConnection connection
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQuery
                |> ignore

                Sql.existingConnection connection
                |> Sql.executeTransaction [
                    "INSERT INTO users (username, active, salary) VALUES (@username, @active, @salary)", [
                        [ ("@username", Sql.text "first"); ("active", Sql.bit true); ("salary", Sql.money 1.0M)  ]
                        [ ("@username", Sql.text "second"); ("active", Sql.bit false); ("salary", Sql.money 1.0M) ]
                        [ ("@username", Sql.text "third"); ("active", Sql.bit true);("salary", Sql.money 1.0M) ]
                    ]
                ]
                |> ignore

                Sql.existingConnection connection
                |> Sql.query "DELETE FROM users"
                |> Sql.executeNonQuery
                |> function
                    | Error error -> raise error
                    | Ok rowsAffected -> Expect.equal 3 rowsAffected "Three entries are deleted"
            }

            test "Sql.executeNonQuery leaves existing connection open" {
                use db = buildDatabase()
                use connection = new NpgsqlConnection(db.ConnectionString)
                connection.Open()
                Sql.existingConnection connection
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQuery
                |> ignore

                Sql.existingConnection connection
                |> Sql.executeTransaction [
                    "INSERT INTO users (username, active, salary) VALUES (@username, @active, @salary)", [
                        [ ("@username", Sql.text "first"); ("active", Sql.bit true); ("salary", Sql.money 1.0M)  ]
                        [ ("@username", Sql.text "second"); ("active", Sql.bit false); ("salary", Sql.money 1.0M) ]
                        [ ("@username", Sql.text "third"); ("active", Sql.bit true);("salary", Sql.money 1.0M) ]
                    ]
                ]
                |> ignore

                Sql.existingConnection connection
                |> Sql.query "DELETE FROM users"
                |> Sql.executeNonQuery
                |> ignore

                Expect.equal ConnectionState.Open connection.State "Check existing connection is still open after executeNonQuery"
            }
        ]

        testAsync "async query execution works" {
            use db = buildDatabase()
            do!
                Sql.connect db.ConnectionString
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQueryAsync
                |> Async.Ignore

            do!
                Sql.connect db.ConnectionString
                |> Sql.executeTransactionAsync [
                    "INSERT INTO users (username, active, salary) VALUES (@username, @active, @salary)", [
                        [ ("@username", Sql.text "first"); ("active", Sql.bit true); ("salary", Sql.money 1.0M)  ]
                        [ ("@username", Sql.text "second"); ("active", Sql.bit false); ("salary", Sql.money 1.0M) ]
                        [ ("@username", Sql.text "third"); ("active", Sql.bit true);("salary", Sql.money 1.0M) ]
                    ]
                ]
                |> Async.Ignore

            let expected = [
                {| userId = 1; username = "first"; active = true; salary = 1.0M  |}
                {| userId = 2; username = "second"; active = false ; salary = 1.0M |}
                {| userId = 3; username = "third"; active = true ; salary = 1.0M |}
            ]

            let! results =
                Sql.connect db.ConnectionString
                |> Sql.query "SELECT * FROM users"
                |> Sql.executeAsync (fun read ->
                    {|
                        userId = read.int "user_id"
                        username = read.string "username"
                        active = read.bool "active"
                        salary = read.decimal "salary"
                    |})

            match results with
            | Error err -> raise err
            | Ok users -> Expect.equal users expected "Users can be read correctly"
        }

        testList "Query-only parallel tests without recreating database" [
            test "Null roundtrip" {
                use db = buildDatabase()
                let connection : string = db.ConnectionString
                connection
                |> Sql.connect
                |> Sql.query "SELECT @nullValue::text as output"
                |> Sql.parameters [ "nullValue", Sql.dbnull ]
                |> Sql.execute (fun read -> read.textOrNone "output")
                |> function
                    | Error error -> raise error
                    | Ok output -> Expect.isNone output.[0] "Output was null"
            }

            test "Bytea roundtrip" {
                use db = buildDatabase()
                let input : array<byte> = [1 .. 5] |> List.map byte |> Array.ofList
                db.ConnectionString
                |> Sql.connect
                |> Sql.query "SELECT @manyBytes as output"
                |> Sql.parameters [ "manyBytes", Sql.bytea input ]
                |> Sql.execute (fun read -> read.bytea "output")
                |> function
                    | Error error -> raise error
                    | Ok output -> Expect.equal input output.[0] "Check bytes read from database are the same sent"
            }

            test "bit/bool roundtrip" {
                use db = buildDatabase()
                db.ConnectionString
                |> Sql.connect
                |> Sql.query "SELECT @logical as output"
                |> Sql.parameters [ "logical", Sql.bit true ]
                |> Sql.execute (fun read -> read.bool "output")
                |> function
                    | Error error -> raise error
                    | Ok output -> Expect.equal true output.[0] "Check bytes read from database are the same sent"
            }

            test "Uuid roundtrip" {
                use db = buildDatabase()
                let id : Guid = Guid.NewGuid()
                db.ConnectionString
                |> Sql.connect
                |> Sql.query "SELECT @uuid_input as output"
                |> Sql.parameters [ "uuid_input", Sql.uuid id ]
                |> Sql.execute (fun read -> read.uuid "output")
                |> function
                    | Error error -> raise error
                    | Ok output -> Expect.equal id output.[0] "Check uuid read from database is the same sent"
            }

            test "Money roundtrip with @ sign" {
                use db = buildDatabase()
                db.ConnectionString
                |> Sql.connect
                |> Sql.query "SELECT @money_input::money as value"
                |> Sql.parameters [ "@money_input", Sql.money 12.5M ]
                |> Sql.execute (fun read -> read.decimal "value")
                |> function
                    | Error error -> raise error
                    | Ok money -> Expect.equal money.[0] 12.5M "Check money as decimal read from database is the same sent"
            }

            test "DateTimeOffset roundtrip when input is UTC" {
                use db = buildDatabase()

                let value = DateTimeOffset.UtcNow

                db.ConnectionString
                |> Sql.connect
                |> Sql.query "SELECT @timestamp::timestamptz as value"
                |> Sql.parameters [ "@timestamp", Sql.timestamptz value ]
                |> Sql.executeRow (fun read -> read.datetimeOffset "value")
                |> function
                    | Error error -> raise error
                    | Ok timestamp -> Expect.equal (timestamp.ToUnixTimeSeconds()) (value.ToUnixTimeSeconds()) "The values are the same"
            }

            test "DateTimeOffset roundtrip when input is local" {
                use db = buildDatabase()

                let value = DateTimeOffset.Now

                db.ConnectionString
                |> Sql.connect
                |> Sql.query "SELECT @timestamp::timestamptz as value"
                |> Sql.parameters [ "@timestamp", Sql.timestamptz value ]
                |> Sql.executeRow (fun read -> read.datetimeOffset "value")
                |> function
                    | Error error -> raise error
                    | Ok timestamp -> Expect.equal (timestamp.ToUnixTimeSeconds()) (value.ToUnixTimeSeconds()) "The values are the same"
            }

            test "uuid_generate_v4()" {
                use db = buildDatabase()
                db.ConnectionString
                |> Sql.connect
                |> Sql.query "SELECT uuid_generate_v4() as id"
                |> Sql.execute (fun read -> read.uuid "id")
                |> function
                    | Error error -> raise error
                    | Ok [ uuid ] ->  Expect.isNotNull (uuid.ToString()) "Check database generates an UUID"
                    | Ok _ -> failwith "Should not happpen"
            }

            test "String option roundtrip" {
                use db = buildDatabase()
                let connection : string = db.ConnectionString
                let a : string option = Some "abc"
                let b : string option = None
                let row =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT @a::text as first, @b::text as second"
                    |> Sql.parameters [ "a", Sql.textOrNone a; "b", Sql.textOrNone b ]
                    |> Sql.execute (fun read -> read.textOrNone "first", read.textOrNone "second")

                match row with
                | Ok [ (Some output, None) ] ->
                    Expect.equal a (Some output) "Check Option value read from database is the same as the one sent"
                | Ok (_) ->
                    failwith "Unexpected results"
                | Error error ->
                    raise error
            }

            test "String option roundtrip with existing connection" {
                use db = buildDatabase()
                use connection = new NpgsqlConnection(db.ConnectionString)
                connection.Open()
                let a : string option = Some "abc"
                let b : string option = None
                let row =
                    connection
                    |> Sql.existingConnection
                    |> Sql.query "SELECT @a::text as first, @b::text as second"
                    |> Sql.parameters [ "a", Sql.textOrNone a; "b", Sql.textOrNone b ]
                    |> Sql.execute (fun read -> read.textOrNone "first", read.textOrNone "second")

                match row with
                | Ok [ (Some output, None) ] ->
                    Expect.equal a (Some output) "Check Option value read from database is the same as the one sent"
                | Ok (_) ->
                    failwith "Unexpected results"
                | Error error ->
                    raise error
            }

            test "String option roundtrip leaves existing connection open" {
                use db = buildDatabase()
                use connection = new NpgsqlConnection(db.ConnectionString)
                connection.Open()
                let a : string option = Some "abc"
                let b : string option = None
                connection
                |> Sql.existingConnection
                |> Sql.query "SELECT @a::text as first, @b::text as second"
                |> Sql.parameters [ "a", Sql.textOrNone a; "b", Sql.textOrNone b ]
                |> Sql.execute (fun read -> read.textOrNone "first", read.textOrNone "second")
                |> ignore

                Expect.equal ConnectionState.Open connection.State "Check existing connection is still open after query"
            }
        ]

        testList "Sequential tests that update database state" [

            test "Sql.execute" {
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.executeTransaction [
                        "INSERT INTO fsharp_test (test_id, test_name) values (@id, @name)", [
                            [ "@id", Sql.int 1; "@name", Sql.text "first test" ]
                            [ "@id", Sql.int 2; "@name", Sql.text "second test" ]
                            [ "@id", Sql.int 3; "@name", Sql.text "third test" ]
                        ]
                    ]
                    |> ignore
                use db = buildDatabase()
                let connection : string = db.ConnectionString
                seedDatabase connection

                let table =
                    Sql.connect connection
                    |> Sql.query "SELECT * FROM fsharp_test"
                    |> Sql.prepare
                    |> Sql.execute (fun read -> {
                        test_id = read.int "test_id";
                        test_name = read.string "test_name"
                    })

                let expected = [
                    { test_id = 1; test_name = "first test" }
                    { test_id = 2; test_name = "second test" }
                    { test_id = 3; test_name = "third test" }
                ]

                match table with
                | Error err -> raise err
                | Ok table -> Expect.equal expected table "Check all rows from `fsharp_test` table using a Reader"
            }

            test "Sql.execute with existing connection" {
                let seedDatabase (connection: NpgsqlConnection) =
                    connection
                    |> Sql.existingConnection
                    |> Sql.executeTransaction [
                        "INSERT INTO fsharp_test (test_id, test_name) values (@id, @name)", [
                            [ "@id", Sql.int 1; "@name", Sql.text "first test" ]
                            [ "@id", Sql.int 2; "@name", Sql.text "second test" ]
                            [ "@id", Sql.int 3; "@name", Sql.text "third test" ]
                        ]
                    ]
                    |> ignore
                use db = buildDatabase()
                use connection = new NpgsqlConnection(db.ConnectionString)
                connection.Open()
                seedDatabase connection

                let table =
                    Sql.existingConnection connection
                    |> Sql.query "SELECT * FROM fsharp_test"
                    |> Sql.prepare
                    |> Sql.execute (fun read -> {
                        test_id = read.int "test_id";
                        test_name = read.string "test_name"
                    })

                let expected = [
                    { test_id = 1; test_name = "first test" }
                    { test_id = 2; test_name = "second test" }
                    { test_id = 3; test_name = "third test" }
                ]

                match table with
                | Error err -> raise err
                | Ok table -> Expect.equal expected table "Check all rows from `fsharp_test` table using a Reader"
            }

            test "Create table with Jsonb data" {
                let seedDatabase (connection: string) (json: string) =
                    connection
                    |> Sql.connect
                    |> Sql.query "INSERT INTO data_with_jsonb (data) VALUES (@jsonb)"
                    |> Sql.parameters ["jsonb", SqlValue.Jsonb json]
                    |> Sql.executeNonQuery
                    |> ignore
                let jsonData = "value from F#"
                let inputJson = "{\"property\": \"" + jsonData + "\"}"
                use db = buildDatabase()
                let connection : string = db.ConnectionString
                seedDatabase connection inputJson

                let dbJson =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT data ->> 'property' as property FROM data_with_jsonb"
                    |> Sql.execute(fun read -> read.text "property")

                match dbJson with
                | Error error -> raise error
                | Ok json -> Expect.equal json.[0] jsonData "Check json read from database"
            }

            test "Create table with Jsonb data with existing connection" {
                let seedDatabase (connection: NpgsqlConnection) (json: string) =
                    connection
                    |> Sql.existingConnection
                    |> Sql.query "INSERT INTO data_with_jsonb (data) VALUES (@jsonb)"
                    |> Sql.parameters ["jsonb", SqlValue.Jsonb json]
                    |> Sql.executeNonQuery
                    |> ignore
                let jsonData = "value from F#"
                let inputJson = "{\"property\": \"" + jsonData + "\"}"
                use db = buildDatabase()
                use connection = new NpgsqlConnection(db.ConnectionString)
                connection.Open()
                seedDatabase connection inputJson

                let dbJson =
                    connection
                    |> Sql.existingConnection
                    |> Sql.query "SELECT data ->> 'property' as property FROM data_with_jsonb"
                    |> Sql.execute(fun read -> read.text "property")

                match dbJson with
                | Error error -> raise error
                | Ok json -> Expect.equal json.[0] jsonData "Check json read from database"
            }

            test "Infinity time" {
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.query "INSERT INTO timestampz_test (version, date1, date2) values (1, 'now', 'infinity')"
                    |> Sql.executeNonQuery
                    |> ignore
                use db = buildDatabase()
                let connection : string = db.ConnectionString
                seedDatabase connection

                let dataTable =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM timestampz_test"
                    |> Sql.execute (fun read -> read.timestamptz "date2")

                Expect.isOk dataTable "Should be able to get results"
            }

            //test "Handle infinity connection" {
            //    let seedDatabase (connection: string) =
            //        connection
            //        |> Sql.connect
            //        |> Sql.query "INSERT INTO timestampz_test (version, date1, date2) values (1, 'now', 'infinity')"
            //        |> Sql.executeNonQuery
            //        |> ignore
            //    use db = buildInfinityDatabase()
            //    let connection : string = db.ConnectionString
            //    seedDatabase connection
            //    let dataTable =
            //        connection
            //        |> Sql.connect
            //        |> Sql.query "SELECT date2 FROM timestampz_test"
            //        |> Sql.executeSingleRow (fun read -> read.timestamptz "date2")
//
            //    match dataTable with
            //    | Error error -> raise error
            //    | Ok timestamp -> Expect.isTrue timestamp.IsInfinity "Returned timestamp is infinity"
            //}

            test "Handle String Array" {
                let getString () =
                    let temp = Guid.NewGuid()
                    temp.ToString("N")
                let a = [| getString() |]
                let b = [| getString(); getString() |]
                let c : string array = [||]
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.executeTransaction [
                        "INSERT INTO string_array_test (id, values) values (@id, @values)", [
                            [ "@id", Sql.int 1; "@values", Sql.stringArray a ]
                            [ "@id", Sql.int 2; "@values", Sql.stringArray b ]
                            [ "@id", Sql.int 3; "@values", Sql.stringArray c ]
                        ]
                    ]
                    |> ignore

                use db = buildDatabase()
                let connection : string = db.ConnectionString
                seedDatabase connection

                let table =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM string_array_test"
                    |> Sql.execute (fun read -> {
                        id = read.int "id"
                        values = read.stringArray "values"
                    })

                let expected = [
                    { id = 1; values = a }
                    { id = 2; values = b }
                    { id = 3; values = c }
                ]

                match table with
                | Error error -> raise error
                | Ok values -> Expect.equal expected values "All rows from `string_array_test` table"
            }

            test "Handle String Array with existing connection" {
                let getString () =
                    let temp = Guid.NewGuid()
                    temp.ToString("N")
                let a = [| getString() |]
                let b = [| getString(); getString() |]
                let c : string array = [||]
                let seedDatabase (connection: NpgsqlConnection) =
                    connection
                    |> Sql.existingConnection
                    |> Sql.executeTransaction [
                        "INSERT INTO string_array_test (id, values) values (@id, @values)", [
                            [ "@id", Sql.int 1; "@values", Sql.stringArray a ]
                            [ "@id", Sql.int 2; "@values", Sql.stringArray b ]
                            [ "@id", Sql.int 3; "@values", Sql.stringArray c ]
                        ]
                    ]
                    |> ignore

                use db = buildDatabase()
                use connection = new NpgsqlConnection(db.ConnectionString)
                connection.Open()
                seedDatabase connection

                let table =
                    connection
                    |> Sql.existingConnection
                    |> Sql.query "SELECT * FROM string_array_test"
                    |> Sql.execute (fun read -> {
                        id = read.int "id"
                        values = read.stringArray "values"
                    })

                let expected = [
                    { id = 1; values = a }
                    { id = 2; values = b }
                    { id = 3; values = c }
                ]

                match table with
                | Error error -> raise error
                | Ok values -> Expect.equal expected values "All rows from `string_array_test` table"
            }

            test "Handle int Array" {
                let a = [| 1; 2 |]
                let b = [| for i in 0..10 do yield i |]
                let c : int array = [||]
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.executeTransaction [
                        "INSERT INTO int_array_test (id, integers) values (@id, @integers)", [
                            [ "@id", Sql.int 1; "@integers", Sql.intArray a ]
                            [ "@id", Sql.int 2; "@integers", Sql.intArray b ]
                            [ "@id", Sql.int 3; "@integers", Sql.intArray c ]
                        ]
                    ]
                    |> ignore

                use db = buildDatabase()
                let connection : string = db.ConnectionString
                seedDatabase connection

                let table =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM int_array_test"
                    |> Sql.execute (fun read -> {
                        id = read.int "id"
                        integers = read.intArray "integers"
                    })

                let expected = [
                    { id = 1; integers = a }
                    { id = 2; integers = b }
                    { id = 3; integers = c }
                ]

                match table with
                | Error error -> raise error
                | Ok table -> Expect.equal expected table  "All rows from `int_array_test` table"
            }

            test "Handle int Array with existing connection" {
                let a = [| 1; 2 |]
                let b = [| for i in 0..10 do yield i |]
                let c : int array = [||]
                let seedDatabase (connection: NpgsqlConnection) =
                    connection
                    |> Sql.existingConnection
                    |> Sql.executeTransaction [
                        "INSERT INTO int_array_test (id, integers) values (@id, @integers)", [
                            [ "@id", Sql.int 1; "@integers", Sql.intArray a ]
                            [ "@id", Sql.int 2; "@integers", Sql.intArray b ]
                            [ "@id", Sql.int 3; "@integers", Sql.intArray c ]
                        ]
                    ]
                    |> ignore

                use db = buildDatabase()
                use connection = new NpgsqlConnection(db.ConnectionString)
                connection.Open()
                seedDatabase connection

                let table =
                    connection
                    |> Sql.existingConnection
                    |> Sql.query "SELECT * FROM int_array_test"
                    |> Sql.execute (fun read -> {
                        id = read.int "id"
                        integers = read.intArray "integers"
                    })

                let expected = [
                    { id = 1; integers = a }
                    { id = 2; integers = b }
                    { id = 3; integers = c }
                ]

                match table with
                | Error error -> raise error
                | Ok table -> Expect.equal expected table  "All rows from `int_array_test` table"
            }

            test "Handle UUID Array" {
                let getUUID () = Guid.NewGuid()
                let a = [| getUUID() |]
                let b = [| getUUID(); getUUID() |]
                let c : Guid array = [||]
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.executeTransaction [
                        "INSERT INTO uuid_array_test (id, values) values (@id, @values)", [
                            [ "@id", Sql.int 1; "@values", Sql.uuidArray a ]
                            [ "@id", Sql.int 2; "@values", Sql.uuidArray b ]
                            [ "@id", Sql.int 3; "@values", Sql.uuidArray c ]
                        ]
                    ]
                    |> ignore

                use db = buildDatabase()
                let connection : string = db.ConnectionString
                seedDatabase connection

                let table =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM uuid_array_test"
                    |> Sql.execute (fun read -> {
                        id = read.int "id"
                        guids = read.uuidArray "values"
                    })

                let expected = [
                    { id = 1; guids = a }
                    { id = 2; guids = b }
                    { id = 3; guids = c }
                ]

                match table with
                | Error error -> raise error
                | Ok values -> Expect.equal expected values "All rows from `uuid_array_test` table"
            }

            test "Handle NpgsqlPoint" {
                let a = NpgsqlTypes.NpgsqlPoint(10., 20.)
                let b = NpgsqlTypes.NpgsqlPoint(55.234, 7.2134)
                let c = NpgsqlTypes.NpgsqlPoint(28.00843, 24.2345)
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.executeTransaction [
                        "INSERT INTO point_test (id, test_point) values (@id, @point)", [
                            [ "@id", Sql.int 1; "@point", Sql.point a ]
                            [ "@id", Sql.int 2; "@point", Sql.point b ]
                            [ "@id", Sql.int 3; "@point", Sql.point c ]
                        ]
                    ]
                    |> ignore

                use db = buildDatabase()
                let connection : string = db.ConnectionString
                seedDatabase connection

                let table =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM point_test"
                    |> Sql.execute (fun read -> {
                        id = read.int "id"
                        point = read.point "test_point"
                    })

                let expected = [
                    { id = 1; point = a}
                    { id = 2; point = b}
                    { id = 3; point = c}
                ]

                match table with
                | Error error -> raise error
                | Ok values -> Expect.equal expected values "All rows from `point_test` table"
            }

            test "Handle nullable NpgsqlPoint" {
                let a = NpgsqlTypes.NpgsqlPoint(10., 20.)
                let b = NpgsqlTypes.NpgsqlPoint(55.234, 7.2134)
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.executeTransaction [
                        "INSERT INTO point_test (id, test_point) values (@id, @point)", [
                            [ "@id", Sql.int 1; "@point", Sql.point a ]
                            [ "@id", Sql.int 2; "@point", Sql.point b ]
                            [ "@id", Sql.int 3; "@point", Sql.dbnull ]
                        ]
                    ]
                    |> ignore

                use db = buildDatabase()
                let connection : string = db.ConnectionString
                seedDatabase connection

                let table =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM point_test"
                    |> Sql.execute (fun read -> {
                        id = read.int "id"
                        nullablepoint = read.pointOrNone "test_point"
                    })

                let expected = [
                    { id = 1; nullablepoint = Some a }
                    { id = 2; nullablepoint = Some b }
                    { id = 3; nullablepoint = None }
                ]

                match table with
                | Error error -> raise error
                | Ok values -> Expect.equal expected values "All rows from `point_test` table"
            }
        ] |> testSequenced

    ]


module Result =
    let throwIfError<'t,'terr when 'terr :> exn> (x: Result<'t,'terr>) =
        match x with
        | Ok ok -> ok
        | Error e -> raise e

let dummyRead = ignore
let testable f x =
    f x |> Result.throwIfError |> ignore
let asyncTestable f x =
    f x |> Async.RunSynchronously |> Result.throwIfError |> ignore

let missingQueryTests =
        [ "Sql.execute", testable <| Sql.execute dummyRead
          "Sql.iter", testable <| Sql.iter dummyRead
          "Sql.executeRow", testable <| Sql.executeRow dummyRead
          "Sql.executeNonQuery", testable Sql.executeNonQuery
          "Sql.executeAsync", asyncTestable <| Sql.executeAsync dummyRead
          "Sql.iterAsync", asyncTestable <| Sql.iterAsync dummyRead
          "Sql.executeNonQueryAsync", asyncTestable Sql.executeNonQueryAsync ]
        |> List.map
            (fun (name, func) ->
                test (sprintf "%s fails with MissingQueryException for missing query" name) {
                    use db = buildDatabase()

                    Expect.throwsT<MissingQueryException>
                        (fun () -> db.ConnectionString |> Sql.connect |> func)
                        "Check missing query fails with expected exception type"
                })

let noResultsTests =
    [ "Sql.executeRow", testable <| Sql.executeRow dummyRead
      "Sql.executeRowAsync", asyncTestable <| Sql.executeRowAsync dummyRead]
    |> List.map
        (fun (name, func) ->
            test (sprintf "%s fails with NoResultsException if no results are returned" name) {
                use db = buildDatabase()

                Expect.throwsT<NoResultsException>
                    (fun () ->
                         db.ConnectionString
                         |> Sql.connect
                         |> Sql.query "SELECT * FROM fsharp_test WHERE test_id = 9999"
                         |> func)
                    "Check no results fails with NoResultsException"
           })

let unknownColumnTest =
    test "RowReader raises UnknownColumnException when trying to read unknown column" {
        use db = buildDatabase()

        Expect.throwsT<UnknownColumnException>
            (fun () ->
                 db.ConnectionString
                 |> Sql.connect
                 |> Sql.query "SELECT * FROM UNNEST(ARRAY ['hello', 'world'])"
                 |> Sql.executeRow (fun read -> read.string "not_a_real_column")
                 |> Result.throwIfError
                 |> ignore)
            "Check invalid column fails with expected exception type"
    }

let errorTests =
    testList "Custom Exception tests" ( unknownColumnTest::missingQueryTests@noResultsTests )
let allTests = testList "All tests" [ tests; errorTests ]

[<EntryPoint>]
let main args = runTestsWithArgs defaultConfig args allTests
