module NgpsqlFSharpTests

open Expecto
open Npgsql.FSharp
open System
open Npgsql
open System.Data
open System.Linq
open Testcontainers.PostgreSql

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

type DoubleArrayTest = {
    id: int
    doubles: double array
}

type DecimalArrayTest = {
    id: int
    decimals: decimal array
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

[<CLIMutable>]
type JsonBlob =
  {
    prop1: int
    prop2: string
  }

let buildDatabase() : PostgreSqlContainer =
    let createFSharpTable = "create table if not exists fsharp_test (test_id int, test_name text)"
    let createJsonbTable = "create table if not exists data_with_jsonb (data jsonb)"
    let createTimestampzTable = "create table if not exists timestampz_test (version integer, date1 timestamptz, date2 timestamptz)"
    let createTimespanTable = "create table if not exists timespan_test (id int, at time without time zone)"
    let createStringArrayTable = "create table if not exists string_array_test (id int, values text [])"
    let createUuidArrayTable = "create table if not exists uuid_array_test (id int, values uuid [])"
    let createIntArrayTable = "create table if not exists int_array_test (id int, integers int [])"
    let createDoubleArrayTable = "create table if not exists double_array_test (id int, doubles double precision [])"
    let createDecimalArrayTable = "create table if not exists decimal_array_test (id int, decimals money [])"
    let createPointTable = "create table if not exists point_test (id int, test_point point)"
    let createExtensionHStore = "create extension if not exists hstore"
    let createExtensionUuid = "create extension if not exists \"uuid-ossp\""


    let postgreSqlContainer = (new PostgreSqlBuilder()).WithImage("postgres:16").Build();

    postgreSqlContainer.StartAsync().Wait()

    postgreSqlContainer.GetConnectionString()
    |> Sql.connect
    |> Sql.executeTransaction [
        createFSharpTable, [ ]
        createJsonbTable, [ ]
        createTimestampzTable, [ ]
        createTimespanTable, [ ]
        createStringArrayTable, [ ]
        createExtensionHStore, [ ]
        createIntArrayTable, [ ]
        createDoubleArrayTable, [ ]
        createDecimalArrayTable, [ ]
        createExtensionUuid, [ ]
        createUuidArrayTable, []
        createPointTable, []
    ]
    |> ignore

    postgreSqlContainer

let tests =
    
    testList "Integration tests" [
        testList "RowReader tests used in Sql.read and Sql.readAsync" [
            test "Sql.executeTransaction works" {
                
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect 
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQuery
                |> ignore
                
                db.GetConnectionString()
                |> Sql.connect 
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

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT * FROM users"
                |> Sql.execute (fun read ->
                    {|
                        userId = read.int "user_id"
                        username = read.string "username"
                        active = read.bool "active"
                        salary = read.decimal "salary"
                    |})
                |> fun users -> Expect.equal users expected "Users can be read correctly"
            }

            test "Sql.executeRow works" {
                let db = buildDatabase()

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                |> Sql.executeRow (fun read -> read.int64 "user_count")
                |> fun count -> Expect.equal count 0L "Count is zero"
            }

            test "Sql.iter works" {
                let db = buildDatabase()

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                let mutable count = -1
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                |> Sql.iter (fun read -> count <- read.int "user_count")
                |> fun () -> Expect.equal count 0 "The count is zero"
            }

            test "Manual transaction handling works with Sql.executeNonQuery" {
                let db = buildDatabase()

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                use connection = new NpgsqlConnection(db.GetConnectionString())
                connection.Open()
                use transaction = connection.BeginTransaction()
                let results = ResizeArray()

                for username in ["John"; "Jane"] do
                    connection
                    |> Sql.existingConnection
                    |> Sql.query "INSERT INTO users (username) VALUES(@username)"
                    |> Sql.parameters [ "@username", Sql.text username ]
                    |> Sql.executeNonQuery
                    |> results.Add

                if (results.Sum() <> 2) then
                    transaction.Rollback()
                else
                    transaction.Commit()

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                |> Sql.executeRow (fun read -> read.int "user_count")
                |> fun count -> Expect.equal 2 count "There are 2 users added"
            }

            test "Manual transaction handling works with Sql.executeNonQuery and can be rolled back" {
                let db = buildDatabase()

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                use connection = new NpgsqlConnection(db.GetConnectionString())
                connection.Open()
                use transaction = connection.BeginTransaction()
                let results = ResizeArray()

                try

                    for username in [Some "John"; Some "Jane"; None] do
                        connection
                        |> Sql.existingConnection
                        |> Sql.query "INSERT INTO users (username) VALUES(@username)"
                        |> Sql.parameters [ "@username", Sql.textOrNone username ]
                        |> Sql.executeNonQuery
                        |> results.Add

                    transaction.Commit()
                with
                | _ -> transaction.Rollback()

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                |> Sql.executeRow (fun read -> read.int "user_count")
                |> fun count -> Expect.equal 0 count "There are 0 users added because the transaction is rolled back"
            }

            testAsync "Sql.iterAsync works" {
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                let mutable count = -1

                do!
                    db.GetConnectionString()
                    |> Sql.connect
                    |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                    |> Sql.iterAsync (fun read -> count <- read.int "user_count")
                    |> Async.AwaitTask

                Expect.equal count 0 "The count is zero"
            }


            test "Reading count as int works" {
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                |> Sql.executeRow (fun read -> read.int "user_count")
                |> fun count -> Expect.equal count 0 "Count is zero"
            }

            test "Sql.executeTransaction works with DateTime" {
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, birthdate date)"
                |> Sql.executeNonQuery
                |> ignore

                let date = DateTime.Now

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.executeTransaction [
                    "INSERT INTO users (birthdate) VALUES (@birthdate)", [
                        [ ("@birthdate", Sql.dateOrNone (Some date)) ]
                        [ ("@birthdate", Sql.dateOrNone Option<DateTime>.None) ]
                    ]
                ]
                |> ignore
            }

            test "Sql.executeTransaction works with DateOnly" {
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, birthdate date)"
                |> Sql.executeNonQuery
                |> ignore

                let date = DateOnly.FromDateTime DateTime.Now

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.executeTransaction [
                    "INSERT INTO users (birthdate) VALUES (@birthdate)", [
                        [ ("@birthdate", Sql.dateOrNone (Some date)) ]
                        [ ("@birthdate", Sql.dateOrNone Option<DateOnly>.None) ]
                    ]
                ]
                |> ignore
            }

            test "Parameter names can contain trailing spaces" {
                let db = buildDatabase()
                use connection = new NpgsqlConnection(db.GetConnectionString())
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

                Expect.equal result [3] "parameters can contain trailing spaces"
            }

            testAsync "Sql.executeRowAsync works" {
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                let! count =
                    db.GetConnectionString()
                    |> Sql.connect
                    |> Sql.query "SELECT COUNT(*) as user_count FROM users"
                    |> Sql.executeRowAsync (fun read -> read.int64 "user_count")
                    |> Async.AwaitTask

                Expect.equal count 0L "Count is zero"
            }

            test "Sql.executeTransaction doesn't error out on parameterized queries with empty parameter sets" {
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.executeTransaction [
                    "INSERT INTO users (username) VALUES (@username)", [ ]
                ]
                |> fun affectedRows -> Expect.equal affectedRows [0] "No rows will be affected"
            }

            testAsync "Sql.executeTransactionAsync doesn't error out on parameterized queries with empty parameter sets" {
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null)"
                |> Sql.executeNonQuery
                |> ignore

                let! affectedRows =
                    db.GetConnectionString()
                    |> Sql.connect
                    |> Sql.executeTransactionAsync [
                        "INSERT INTO users (username) VALUES (@username)", [ ]
                    ]
                    |> Async.AwaitTask

                Expect.equal affectedRows [0] "No rows will be affected"
            }

            test "Sql.executeTransaction works with existing open connection" {
                let db = buildDatabase()
                use connection = new NpgsqlConnection(db.GetConnectionString())
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
                |> fun users -> Expect.equal users expected "Users can be read correctly"
            }

            test "Sql.executeTransaction works with existing connection" {
                let db = buildDatabase()
                use connection = new NpgsqlConnection(db.GetConnectionString())
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
                |> fun users -> Expect.equal users expected "Users can be read correctly"
            }

            test "Sql.executeTransaction leaves existing connection open" {
                let db = buildDatabase()
                use connection = new NpgsqlConnection(db.GetConnectionString())
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

            test "Sql.executeTransaction works with data source" {
                let db = buildDatabase()


                use dataSource = NpgsqlDataSource.Create(db.GetConnectionString())
                Sql.fromDataSource dataSource
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQuery
                |> ignore

                Sql.fromDataSource dataSource
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

                Sql.fromDataSource dataSource
                |> Sql.query "SELECT * FROM users"
                |> Sql.execute (fun read ->
                    {|
                        userId = read.int "user_id"
                        username = read.string "username"
                        active = read.bool "active"
                        salary = read.decimal "salary"
                    |})
                |> fun users -> Expect.equal users expected "Users can be read correctly"
            }

            test "Sql.executeNonQuery works" {
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQuery
                |> ignore

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.executeTransaction [
                    "INSERT INTO users (username, active, salary) VALUES (@username, @active, @salary)", [
                        [ ("@username", Sql.text "first"); ("active", Sql.bit true); ("salary", Sql.money 1.0M)  ]
                        [ ("@username", Sql.text "second"); ("active", Sql.bit false); ("salary", Sql.money 1.0M) ]
                        [ ("@username", Sql.text "third"); ("active", Sql.bit true);("salary", Sql.money 1.0M) ]
                    ]
                ]
                |> ignore

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "DELETE FROM users"
                |> Sql.executeNonQuery
                |> fun rowsAffected -> Expect.equal 3 rowsAffected "Three entries are deleted"
            }

            test "Sql.executeNonQuery works with existing connection" {
                let db = buildDatabase()
                use connection = new NpgsqlConnection(db.GetConnectionString())
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
                |> fun rowsAffected -> Expect.equal 3 rowsAffected "Three entries are deleted"
            }

            test "Sql.executeNonQuery leaves existing connection open" {
                let db = buildDatabase()
                use connection = new NpgsqlConnection(db.GetConnectionString())
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

            test "Sql.executeNonQuery works with data source" {
                let db = buildDatabase()
                use dataSource = NpgsqlDataSource.Create(db.GetConnectionString())
                Sql.fromDataSource dataSource
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQuery
                |> ignore

                Sql.fromDataSource dataSource
                |> Sql.executeTransaction [
                    "INSERT INTO users (username, active, salary) VALUES (@username, @active, @salary)", [
                        [ ("@username", Sql.text "first"); ("active", Sql.bit true); ("salary", Sql.money 1.0M)  ]
                        [ ("@username", Sql.text "second"); ("active", Sql.bit false); ("salary", Sql.money 1.0M) ]
                        [ ("@username", Sql.text "third"); ("active", Sql.bit true);("salary", Sql.money 1.0M) ]
                    ]
                ]
                |> ignore

                Sql.fromDataSource dataSource
                |> Sql.query "DELETE FROM users"
                |> Sql.executeNonQuery
                |> fun rowsAffected -> Expect.equal 3 rowsAffected "Three entries are deleted"
            }

            test "Sql.toSeq works" {
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQuery
                |> ignore

                db.GetConnectionString()
                |> Sql.connect
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
                
                let sequence =
                    db.GetConnectionString()
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM users"
                    |> Sql.toSeq (fun read ->
                        {|
                            userId = read.int "user_id"
                            username = read.string "username"
                            active = read.bool "active"
                            salary = read.decimal "salary"
                        |})
                
                let expectCorrectResults message =
                    sequence
                    |> Seq.toList
                    |> List.sortBy (fun u -> u.userId)
                    |> fun users -> Expect.equal users expected message

                // Iterate over the sequence the first time
                expectCorrectResults "Users can be read correctly on first iteration"

                // Iterate over the sequence a second time
                expectCorrectResults "Users can be read correctly on second iteration"
            }
        ]

        testAsync "async query execution works" {
            let db = buildDatabase()
            do!
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "CREATE TABLE users (user_id serial primary key, username text not null, active bit not null, salary money not null)"
                |> Sql.executeNonQueryAsync
                |> Async.AwaitTask
                |> Async.Ignore

            do!
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.executeTransactionAsync [
                    "INSERT INTO users (username, active, salary) VALUES (@username, @active, @salary)", [
                        [ ("@username", Sql.text "first"); ("active", Sql.bit true); ("salary", Sql.money 1.0M)  ]
                        [ ("@username", Sql.text "second"); ("active", Sql.bit false); ("salary", Sql.money 1.0M) ]
                        [ ("@username", Sql.text "third"); ("active", Sql.bit true);("salary", Sql.money 1.0M) ]
                    ]
                ]
                |> Async.AwaitTask
                |> Async.Ignore

            let expected = [
                {| userId = 1; username = "first"; active = true; salary = 1.0M  |}
                {| userId = 2; username = "second"; active = false ; salary = 1.0M |}
                {| userId = 3; username = "third"; active = true ; salary = 1.0M |}
            ]

            let! users =
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT * FROM users"
                |> Sql.executeAsync (fun read ->
                    {|
                        userId = read.int "user_id"
                        username = read.string "username"
                        active = read.bool "active"
                        salary = read.decimal "salary"
                    |})
                |> Async.AwaitTask

            Expect.equal users expected "Users can be read correctly"
        }

        testList "Query-only parallel tests without recreating database" [
            test "Null roundtrip" {
                let db = buildDatabase()
                let connection : string = db.GetConnectionString()
                connection
                |> Sql.connect
                |> Sql.query "SELECT @nullValue::text as output"
                |> Sql.parameters [ "nullValue", Sql.dbnull ]
                |> Sql.execute (fun read -> read.textOrNone "output")
                |> fun output -> Expect.isNone output.[0] "Output was null"
            }

            test "Bytea roundtrip" {
                let db = buildDatabase()
                let input : array<byte> = [1 .. 5] |> List.map byte |> Array.ofList
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT @manyBytes as output"
                |> Sql.parameters [ "manyBytes", Sql.bytea input ]
                |> Sql.execute (fun read -> read.bytea "output")
                |> fun output -> Expect.equal input output.[0] "Check bytes read from database are the same sent"
            }

            test "bit/bool roundtrip" {
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT @logical as output"
                |> Sql.parameters [ "logical", Sql.bit true ]
                |> Sql.execute (fun read -> read.bool "output")
                |> fun output -> Expect.equal true output.[0] "Check bytes read from database are the same sent"
            }

            test "Uuid roundtrip" {
                let db = buildDatabase()
                let id : Guid = Guid.NewGuid()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT @uuid_input as output"
                |> Sql.parameters [ "uuid_input", Sql.uuid id ]
                |> Sql.execute (fun read -> read.uuid "output")
                |> fun output -> Expect.equal id output.[0] "Check uuid read from database is the same sent"
            }

            test "Interval roundtrip" {
                let db = buildDatabase()
                let oneHourInterval : TimeSpan = TimeSpan.FromHours 1.0
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT @interval_input as output"
                |> Sql.parameters [ "interval_input", Sql.interval oneHourInterval ]
                |> Sql.execute (fun read -> read.interval "output")
                |> fun output -> Expect.equal oneHourInterval output.[0] "Check interval read from database is the same sent"
            }

            test "Money roundtrip with @ sign" {
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT @money_input::money as value"
                |> Sql.parameters [ "@money_input", Sql.money 12.5M ]
                |> Sql.execute (fun read -> read.decimal "value")
                |> fun money -> Expect.equal money.[0] 12.5M "Check money as decimal read from database is the same sent"
            }

            test "DateTimeOffset roundtrip when input is UTC" {
                let db = buildDatabase()

                let value = DateTimeOffset.UtcNow

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT @timestamp::timestamptz as value"
                |> Sql.parameters [ "@timestamp", Sql.timestamptz value ]
                |> Sql.executeRow (fun read -> read.datetimeOffset "value")
                |> fun timestamp -> Expect.equal (timestamp.ToUnixTimeSeconds()) (value.ToUnixTimeSeconds()) "The values are the same"
            }

            test "DateTime as date roundtrip" {
                let db = buildDatabase()

                let value = DateTime.Today

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT @date::date as value"
                |> Sql.parameters [ "@date", Sql.date value ]
                |> Sql.executeRow (fun read -> read.dateTime "value")
                |> fun dateTime -> Expect.equal dateTime value "The values are the same"
            }

            test "DateOnly as date roundtrip" {
                let db = buildDatabase()

                let value = DateOnly.FromDateTime DateTime.Now

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT @date::date as value"
                |> Sql.parameters [ "@date", Sql.date value ]
                |> Sql.executeRow (fun read -> read.dateTime "value")
                |> fun dateTime -> Expect.equal (DateOnly.FromDateTime dateTime) value "The values are the same"
            }

            test "None DateOnly as date roundtrip" {
                let db = buildDatabase()

                let value: DateOnly option = None

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT @date::date as value"
                |> Sql.parameters [ "@date", Sql.dateOrNone value ]
                |> Sql.executeRow (fun read -> read.dateOnlyOrNone "value")
                |> fun date -> Expect.equal date value "The values are the same"
            }

            test "ValueSome DateOnly option as date roundtrip" {
                let db = buildDatabase()

                let value = DateOnly.FromDateTime DateTime.Now |> ValueSome

                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT @date::date as value"
                |> Sql.parameters [ "@date", Sql.dateOrValueNone value ]
                |> Sql.executeRow (fun read -> read.dateOnlyOrValueNone "value")
                |> fun date -> Expect.equal date value "The values are the same"
            }

            test "uuid_generate_v4()" {
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT uuid_generate_v4() as id"
                |> Sql.execute (fun read -> read.uuid "id")
                |> function
                    | [ uuid ] ->  Expect.isNotNull (uuid.ToString()) "Check database generates an UUID"
                    | _ -> failwith "Should not happpen"
            }

            test "String option roundtrip" {
                let db = buildDatabase()
                let connection : string = db.GetConnectionString()
                let a : string option = Some "abc"
                let b : string option = None
                let row =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT @a::text as first, @b::text as second"
                    |> Sql.parameters [ "a", Sql.textOrNone a; "b", Sql.textOrNone b ]
                    |> Sql.execute (fun read -> read.textOrNone "first", read.textOrNone "second")

                match row with
                | [ (Some output, None) ] ->
                    Expect.equal a (Some output) "Check Option value read from database is the same as the one sent"
                | _ ->
                    failwith "Unexpected results"
            }

            test "String option roundtrip with existing connection" {
                let db = buildDatabase()
                use connection = new NpgsqlConnection(db.GetConnectionString())
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
                | [ (Some output, None) ] ->
                    Expect.equal a (Some output) "Check Option value read from database is the same as the one sent"
                | _ ->
                    failwith "Unexpected results"
            }

            test "String option roundtrip leaves existing connection open" {
                let db = buildDatabase()
                use connection = new NpgsqlConnection(db.GetConnectionString())
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

            test "String option roundtrip with data source" {
                let db = buildDatabase()
                use dataSource = NpgsqlDataSource.Create(db.GetConnectionString())
                let a : string option = Some "abc"
                let b : string option = None
                let row =
                    dataSource
                    |> Sql.fromDataSource
                    |> Sql.query "SELECT @a::text as first, @b::text as second"
                    |> Sql.parameters [ "a", Sql.textOrNone a; "b", Sql.textOrNone b ]
                    |> Sql.execute (fun read -> read.textOrNone "first", read.textOrNone "second")

                match row with
                | [ (Some output, None) ] ->
                    Expect.equal a (Some output) "Check Option value read from database is the same as the one sent"
                | _ ->
                    failwith "Unexpected results"
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
                let db = buildDatabase()
                let connection : string = db.GetConnectionString()
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

                Expect.equal expected table "Check all rows from `fsharp_test` table using a Reader"
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

                let db = buildDatabase()
                use connection = new NpgsqlConnection(db.GetConnectionString())
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

                Expect.equal expected table "Check all rows from `fsharp_test` table using a Reader"
            }

            test "Sql.execute with data source" {
                let seedDatabase (dataSource: NpgsqlDataSource) =
                    dataSource
                    |> Sql.fromDataSource
                    |> Sql.executeTransaction [
                        "INSERT INTO fsharp_test (test_id, test_name) values (@id, @name)", [
                            [ "@id", Sql.int 1; "@name", Sql.text "first test" ]
                            [ "@id", Sql.int 2; "@name", Sql.text "second test" ]
                            [ "@id", Sql.int 3; "@name", Sql.text "third test" ]
                        ]
                    ]
                    |> ignore

                let db = buildDatabase()
                use dataSource = NpgsqlDataSource.Create(db.GetConnectionString())
                seedDatabase dataSource

                let table =
                    Sql.fromDataSource dataSource
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

                Expect.equal expected table "Check all rows from `fsharp_test` table using a Reader"
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
                let db = buildDatabase()
                let connection : string = db.GetConnectionString()
                seedDatabase connection inputJson

                let json =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT data ->> 'property' as property FROM data_with_jsonb"
                    |> Sql.execute(fun read -> read.text "property")

                Expect.equal json.[0] jsonData "Check json read from database"
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
                let db = buildDatabase()
                use connection = new NpgsqlConnection(db.GetConnectionString())
                connection.Open()
                seedDatabase connection inputJson

                let dbJson =
                    connection
                    |> Sql.existingConnection
                    |> Sql.query "SELECT data ->> 'property' as property FROM data_with_jsonb"
                    |> Sql.execute(fun read -> read.text "property")

                Expect.equal dbJson.[0] jsonData "Check json read from database"
            }

            test "Create table with Jsonb data with data source" {
                let seedDatabase (dataSource: NpgsqlDataSource) (json: string) =
                    dataSource
                    |> Sql.fromDataSource
                    |> Sql.query "INSERT INTO data_with_jsonb (data) VALUES (@jsonb)"
                    |> Sql.parameters ["jsonb", SqlValue.Jsonb json]
                    |> Sql.executeNonQuery
                    |> ignore
                let jsonData = "value from F#"
                let inputJson = "{\"property\": \"" + jsonData + "\"}"
                let db = buildDatabase()
                use dataSource = NpgsqlDataSource.Create(db.GetConnectionString())
                seedDatabase dataSource inputJson

                let dbJson =
                    dataSource
                    |> Sql.fromDataSource
                    |> Sql.query "SELECT data ->> 'property' as property FROM data_with_jsonb"
                    |> Sql.execute(fun read -> read.text "property")

                Expect.equal dbJson.[0] jsonData "Check json read from database"
            }

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

                let db = buildDatabase()
                let connection : string = db.GetConnectionString()
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

                Expect.equal expected table "All rows from `string_array_test` table"
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

                let db = buildDatabase()
                use connection = new NpgsqlConnection(db.GetConnectionString())
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

                Expect.equal expected table "All rows from `string_array_test` table"
            }

            test "Handle String Array with data source" {
                let getString () =
                    let temp = Guid.NewGuid()
                    temp.ToString("N")
                let a = [| getString() |]
                let b = [| getString(); getString() |]
                let c : string array = [||]
                let seedDatabase (dataSource: NpgsqlDataSource) =
                    dataSource
                    |> Sql.fromDataSource
                    |> Sql.executeTransaction [
                        "INSERT INTO string_array_test (id, values) values (@id, @values)", [
                            [ "@id", Sql.int 1; "@values", Sql.stringArray a ]
                            [ "@id", Sql.int 2; "@values", Sql.stringArray b ]
                            [ "@id", Sql.int 3; "@values", Sql.stringArray c ]
                        ]
                    ]
                    |> ignore

                let db = buildDatabase()
                use dataSource = NpgsqlDataSource.Create(db.GetConnectionString())
                seedDatabase dataSource

                let table =
                    dataSource
                    |> Sql.fromDataSource
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

                Expect.equal expected table "All rows from `string_array_test` table"
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

                let db = buildDatabase()
                let connection : string = db.GetConnectionString()
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

                Expect.equal expected table  "All rows from `int_array_test` table"
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

                let db = buildDatabase()
                use connection = new NpgsqlConnection(db.GetConnectionString())
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

                Expect.equal expected table  "All rows from `int_array_test` table"
            }

            test "Handle int Array with data source" {
                let a = [| 1; 2 |]
                let b = [| for i in 0..10 do yield i |]
                let c : int array = [||]
                let seedDatabase (dataSource: NpgsqlDataSource) =
                    dataSource
                    |> Sql.fromDataSource
                    |> Sql.executeTransaction [
                        "INSERT INTO int_array_test (id, integers) values (@id, @integers)", [
                            [ "@id", Sql.int 1; "@integers", Sql.intArray a ]
                            [ "@id", Sql.int 2; "@integers", Sql.intArray b ]
                            [ "@id", Sql.int 3; "@integers", Sql.intArray c ]
                        ]
                    ]
                    |> ignore

                let db = buildDatabase()
                use dataSource = NpgsqlDataSource.Create(db.GetConnectionString())
                seedDatabase dataSource

                let table =
                    dataSource
                    |> Sql.fromDataSource
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

                Expect.equal expected table  "All rows from `int_array_test` table"
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

                let db = buildDatabase()
                let connection : string = db.GetConnectionString()
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

                Expect.equal expected table "All rows from `uuid_array_test` table"
            }
            
            test "Handle nullable UUID Array" {
                let db = buildDatabase()
                let connection : string = db.GetConnectionString()
                
                let table =
                    connection
                    |> Sql.connect
                    |> Sql.query """select
    values
from (
    values (null), ('{97236cb5-fecc-4cee-8602-36d460e547b7}'::uuid[]), ('{}'::uuid[])
) s(values)"""
                    |> Sql.execute (fun read -> read.uuidArrayOrNone "values")

                let expected = [
                    None
                    Some [| Guid "97236cb5-fecc-4cee-8602-36d460e547b7" |]
                    Some [|  |]
                ]

                Expect.equal expected table "All rows"
            }

            test "Handle double Array" {
                let a = [| 1.; 2. |]
                let b = [| for i in 0..10 do yield (double i) |]
                let c : double array = [||]
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.executeTransaction [
                        "INSERT INTO double_array_test (id, doubles) values (@id, @doubles)", [
                            [ "@id", Sql.int 1; "@doubles", Sql.doubleArray a ]
                            [ "@id", Sql.int 2; "@doubles", Sql.doubleArray b ]
                            [ "@id", Sql.int 3; "@doubles", Sql.doubleArray c ]
                        ]
                    ]
                    |> ignore

                let db = buildDatabase()
                seedDatabase (db.GetConnectionString())

                let table =
                    db.GetConnectionString()
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM double_array_test"
                    |> Sql.execute (fun read -> {
                        id = read.int "id"
                        doubles = read.doubleArray "doubles"
                    })

                let expected = [
                    { id = 1; doubles = a }
                    { id = 2; doubles = b }
                    { id = 3; doubles = c }
                ]

                Expect.equal expected table  "All rows from `double_array_test` table"
            }

            test "Handle decimal Array" {
                let a = [| 1m; 2m |]
                let b = [| for i in 0..10 do yield (decimal i) |]
                let c : decimal array = [||]
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.executeTransaction [
                        "INSERT INTO decimal_array_test (id, decimals) values (@id, @decimals)", [
                            [ "@id", Sql.int 1; "@decimals", Sql.decimalArray a ]
                            [ "@id", Sql.int 2; "@decimals", Sql.decimalArray b ]
                            [ "@id", Sql.int 3; "@decimals", Sql.decimalArray c ]
                        ]
                    ]
                    |> ignore

                let db = buildDatabase()
                seedDatabase (db.GetConnectionString())
                let table =
                    db.GetConnectionString()
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM decimal_array_test"
                    |> Sql.execute (fun read -> {
                        id = read.int "id"
                        decimals = read.decimalArray "decimals"
                    })

                let expected = [
                    { id = 1; decimals = a }
                    { id = 2; decimals = b }
                    { id = 3; decimals = c }
                ]

                Expect.equal table expected  "All rows from `decimal_array_test` table"
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

                let db = buildDatabase()
                let connection : string = db.GetConnectionString()
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

                Expect.equal expected table "All rows from `point_test` table"
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

                let db = buildDatabase()
                let connection : string = db.GetConnectionString()
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

                Expect.equal expected table "All rows from `point_test` table"
            }

            test "Sql returned types" {
                //Int
                let data = 1
                let value = Sql.int data
                Expect.equal (SqlValue.Int data) value "Unexpected value Sql.int"

                let value = Sql.intOrNone (Some data)
                Expect.equal (SqlValue.Int data) value "Unexpected value Sql.intOrNone (Some)"

                let value = Sql.intOrNone None
                Expect.equal SqlValue.Null value "Unexpected value Sql.intOrNone (None)"

                let value = Sql.intOrValueNone (ValueSome data)
                Expect.equal (SqlValue.Int data) value "Unexpected value Sql.intOrValueNone (ValueSome)"

                let value = Sql.intOrValueNone ValueNone
                Expect.equal SqlValue.Null value "Unexpected value Sql.intOrValueNone (ValueNone)"

                //String
                let data = "str"
                let value = Sql.string data
                Expect.equal (SqlValue.String data) value "Unexpected value Sql.string"

                let value = Sql.stringOrNone (Some data)
                Expect.equal (SqlValue.String data) value "Unexpected value Sql.stringOrNone (Some)"

                let value = Sql.stringOrNone None
                Expect.equal SqlValue.Null value "Unexpected value Sql.stringOrNone (None)"

                let value = Sql.stringOrValueNone (ValueSome data)
                Expect.equal (SqlValue.String data) value "Unexpected value Sql.stringOrValueNone (ValueSome)"

                let value = Sql.stringOrValueNone ValueNone
                Expect.equal SqlValue.Null value "Unexpected value Sql.stringOrValueNone (ValueNone)"

                //Text
                let data = "text"
                let value = Sql.text data
                Expect.equal (SqlValue.String data) value "Unexpected value Sql.text"

                let value = Sql.textOrNone (Some data)
                Expect.equal (SqlValue.String data) value "Unexpected value Sql.textOrNone (Some)"

                let value = Sql.textOrNone None
                Expect.equal SqlValue.Null value "Unexpected value Sql.textOrNone (None)"

                let value = Sql.textOrValueNone (ValueSome data)
                Expect.equal (SqlValue.String data) value "Unexpected value Sql.textOrValueNone (ValueSome)"

                let value = Sql.textOrValueNone ValueNone
                Expect.equal SqlValue.Null value "Unexpected value Sql.textOrValueNone (ValueNone)"

                //bit
                let data = true
                let value = Sql.bit data
                Expect.equal (SqlValue.Bit data) value "Unexpected value Sql.bit"

                let value = Sql.bitOrNone (Some data)
                Expect.equal (SqlValue.Bit data) value "Unexpected value Sql.bitOrNone (Some)"

                let value = Sql.bitOrNone None
                Expect.equal SqlValue.Null value "Unexpected value Sql.bitOrNone (None)"

                let value = Sql.bitOrValueNone (ValueSome data)
                Expect.equal (SqlValue.Bit data) value "Unexpected value Sql.bitOrValueNone (ValueSome)"

                let value = Sql.bitOrValueNone ValueNone
                Expect.equal SqlValue.Null value "Unexpected value Sql.bitOrValueNone (ValueNone)"

                //bool
                let data = true
                let value = Sql.bool data
                Expect.equal (SqlValue.Bool data) value "Unexpected value Sql.bool"

                let value = Sql.boolOrNone (Some data)
                Expect.equal (SqlValue.Bool data) value "Unexpected value Sql.boolOrNone (Some)"

                let value = Sql.boolOrNone None
                Expect.equal SqlValue.Null value "Unexpected value Sql.boolOrNone (None)"

                let value = Sql.boolOrValueNone (ValueSome data)
                Expect.equal (SqlValue.Bool data) value "Unexpected value Sql.boolOrValueNone (ValueSome)"

                let value = Sql.boolOrValueNone ValueNone
                Expect.equal SqlValue.Null value "Unexpected value Sql.boolOrValueNone (ValueNone)"

                //double
                let data = 7.
                let value = Sql.double data
                Expect.equal (SqlValue.Number data) value "Unexpected value Sql.double"

                let value = Sql.doubleOrNone (Some data)
                Expect.equal (SqlValue.Number data) value "Unexpected value Sql.doubleOrNone (Some)"

                let value = Sql.doubleOrNone None
                Expect.equal SqlValue.Null value "Unexpected value Sql.doubleOrNone (None)"

                let value = Sql.doubleOrValueNone (ValueSome data)
                Expect.equal (SqlValue.Number data) value "Unexpected value Sql.doubleOrValueNone (ValueSome)"

                let value = Sql.doubleOrValueNone ValueNone
                Expect.equal SqlValue.Null value "Unexpected value Sql.doubleOrValueNone (ValueNone)"

                // real
                let data = 14.f
                let value = Sql.real data
                Expect.equal (SqlValue.Real data) value "Unexpected value Sql.real"

                let value = Sql.realOrNone (Some data)
                Expect.equal (SqlValue.Real data) value "Unexpected value Sql.realOrNone (Some)"

                let value = Sql.realOrNone None
                Expect.equal SqlValue.Null value "Unexpected value Sql.realOrNone (None)"

                let value = Sql.realOrValueNone (ValueSome data)
                Expect.equal (SqlValue.Real data) value "Unexpected value Sql.realOrValueNone (ValueSome)"

                let value = Sql.realOrValueNone ValueNone
                Expect.equal SqlValue.Null value "Unexpected value Sql.realOrValueNone (ValueNone)"

                //decimal
                let data = 9M
                let value = Sql.decimal data
                Expect.equal (SqlValue.Decimal data) value "Unexpected value Sql.decimal"

                let value = Sql.decimalOrNone (Some data)
                Expect.equal (SqlValue.Decimal data) value "Unexpected value Sql.decimalOrNone (Some)"

                let value = Sql.decimalOrNone None
                Expect.equal SqlValue.Null value "Unexpected value Sql.decimalOrNone (None)"

                let value = Sql.decimalOrValueNone (ValueSome data)
                Expect.equal (SqlValue.Decimal data) value "Unexpected value Sql.decimalOrValueNone (ValueSome)"

                let value = Sql.decimalOrValueNone ValueNone
                Expect.equal SqlValue.Null value "Unexpected value Sql.decimalOrValueNone (ValueNone)"

                //timestamp
                let data = DateTime(2021, 02, 28)
                let value = Sql.timestamp data
                Expect.equal (SqlValue.Timestamp data) value "Unexpected value Sql.timestamp"

                let value = Sql.timestampOrNone (Some data)
                Expect.equal (SqlValue.Timestamp data) value "Unexpected value Sql.timestampOrNone (Some)"

                let value = Sql.timestampOrNone None
                Expect.equal SqlValue.Null value "Unexpected value Sql.timestampOrNone (None)"

                let value = Sql.timestampOrValueNone (ValueSome data)
                Expect.equal (SqlValue.Timestamp data) value "Unexpected value Sql.timestampOrValueNone (ValueSome)"

                let value = Sql.timestampOrValueNone ValueNone
                Expect.equal SqlValue.Null value "Unexpected value Sql.timestampOrValueNone (ValueNone)"
            }

            test "jsonb support works" {
                let db = buildDatabase()

                let dataSource = (new NpgsqlDataSourceBuilder(db.GetConnectionString())).EnableDynamicJson().Build()
                
                dataSource
                |> Sql.fromDataSource 
                |> Sql.query "CREATE TABLE json_test (id serial primary key, blob jsonb not null)"
                |> Sql.executeNonQuery
                |> ignore

                dataSource
                |> Sql.fromDataSource 
                |> Sql.executeTransaction [
                    "INSERT INTO json_test (blob) VALUES (@blob)", [
                        [ ("@blob", Sql.jsonb """{"prop1": 123, "prop2": "something"}"""); ]
                    ]
                ]
                |> ignore

                let expected = [
                    {| id = 1; blob = {prop1=123; prop2="something"} |}
                ]

                dataSource
                |> Sql.fromDataSource 
                |> Sql.query "SELECT * FROM json_test"
                |> Sql.execute (fun read ->
                    {|
                        id = read.int "id"
                        blob = read.fieldValue<JsonBlob> "blob"
                    |})
                |> fun blobs -> Expect.equal blobs expected "Json can be read correctly"
            }
        ] |> testSequenced

    ]

let unknownColumnTest =
    test "RowReader raises UnknownColumnException when trying to read unknown column" {
        Expect.throws
            (fun () ->
                let db = buildDatabase()
                db.GetConnectionString()
                |> Sql.connect
                |> Sql.query "SELECT * FROM UNNEST(ARRAY ['hello', 'world'])"
                |> Sql.executeRow (fun read -> read.string "not_a_real_column")
                |> ignore)
            "Check invalid column fails with expected exception type"
    }


let allTests = testList "Npgsql.FSharp" [ tests; unknownColumnTest ]
