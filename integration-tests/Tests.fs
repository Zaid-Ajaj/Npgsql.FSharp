module Main

open Expecto
open Npgsql.FSharp
open Npgsql.FSharp.OptionWorkflow
open System

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

let defaultConnection() =
    Sql.host "localhost"
    |> Sql.port 5432
    |> Sql.username "postgres"
    |> Sql.password "postgres"
    |> Sql.database "postgres"
    |> Sql.str

let handleInfinityConnection() =
    Sql.host "localhost"
    |> Sql.port 5432
    |> Sql.username "postgres"
    |> Sql.password "postgres"
    |> Sql.database "postgres"
    |> Sql.convertInfinityDateTime true
    |> Sql.str

let buildDatabase (connection: string) : unit =
    let createFSharpTable = "create table fsharp_test (test_id int, test_name text)"
    let createJsonbTable = "create table data_with_jsonb (data jsonb)"
    let createTimestampzTable = "create table timestampz_test (version integer, date1 timestamptz, date2 timestamptz)"
    let createTimespanTable = "create table if not exists timespan_test (id int, at time without time zone)"
    let createStringArrayTable = "create table if not exists string_array_test (id int, values text [])"
    let createIntArrayTable = "create table if not exists int_array_test (id int, integers int [])"
    let createExtensionHStore = "create extension if not exists hstore"
    let createExtensionUuid = "create extension if not exists \"uuid-ossp\""
    connection
    |> Sql.connect
    |> Sql.queryMany [
        createFSharpTable
        createJsonbTable
        createTimestampzTable
        createTimespanTable
        createStringArrayTable
        createIntArrayTable
        createExtensionHStore
        createExtensionUuid
    ]
    |> Sql.executeMany
    |> ignore

let cleanDatabase (connection: string) : unit =
    // WARNING: Dropping HStore extension throws similar exception to the following one 
    // in the test that uses it:
    //   Npgsql.PostgresException (0x80004005): XX000: cache lookup failed for type xxxxx
    let dropFSharpTable = "drop table if exists fsharp_test"
    let dropIntArrayTable = "drop table if exists int_array_test"
    let dropStringArrayTable = "drop table if exists string_array_test"
    let dropTimespanTable = "drop table if exists timespan_test"
    let dropTimestampzTable = "drop table if exists timestampz_test"
    let dropJsonbTable = "drop table if exists data_with_jsonb"
    let dropExtensionUuid = "drop extension if exists \"uuid-ossp\""
    connection
    |> Sql.connect
    |> Sql.queryMany [
        dropExtensionUuid
        dropIntArrayTable
        dropStringArrayTable
        dropTimespanTable
        dropTimestampzTable
        dropJsonbTable
        dropFSharpTable
    ]
    |> Sql.executeMany
    |> ignore

let tests =

    testList "Integration tests" [
        
        // Setup: Run once for all tests.
        let connection : string = defaultConnection()
        cleanDatabase connection
        buildDatabase connection

        testList "Query-only parallel tests without recreating database" [

            test "Null roundtrip" {
                let queryOutput : SqlValue =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT @nullValue"
                    |> Sql.parameters [ "nullValue", SqlValue.Null ]
                    |> Sql.executeScalar
                Expect.equal SqlValue.Null queryOutput "Check null value returned from database is the same sent"
            }

            test "Reading time" {     
                let now : DateTime = DateTime.UtcNow
                let databaseNow : DateTime =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT TIMEZONE('utc', NOW())"
                    |> Sql.executeScalar
                    |> Sql.toDateTime
                let later : DateTime = now.AddMinutes(1.0)
                Expect.isAscending [now; databaseNow; later] "Check database `now` function is accurate"
            }
            
            test "Reading time with reader" {
                let now : DateTime = DateTime.UtcNow
                let databaseNowColl : list<DateTime> =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT NOW()::timestamp AS time"
                    |> Sql.executeReader (Sql.readRow >> Sql.readTimestamp "time")
                let later : DateTime = now.AddMinutes(1.0)
                Expect.equal 1 (List.length databaseNowColl) "Check list is a singleton"
                let databaseNow = List.head databaseNowColl
                Expect.isAscending [now; databaseNow; later] "Check database `now` function is accurate"
            } 

            test "Jsonb roundtrip" {
                let jsonData = "value from F#"
                let inputJson = "{\"property\": \"" + jsonData + "\"}"
                let jsonValue : SqlValue =
                    connection
                    |> Sql.connect
                    |> Sql.query "select @jsonb"
                    |> Sql.parameters ["jsonb", SqlValue.Jsonb inputJson]
                    |> Sql.executeScalar
                Expect.equal (SqlValue.String inputJson) jsonValue "Check json value returned from database is the same sent"
            }

            testAsync "Reading time with reader async" {
                let now : DateTime = DateTime.UtcNow
                let! databaseNowColl =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT NOW()::timestamp AS time"
                    |> Sql.executeReaderAsync (Sql.readRow >> Sql.readTimestamp "time")
                let later : DateTime = now.AddMinutes(1.0)
                Expect.equal 1 (List.length databaseNowColl) "Check list is a singleton"
                let databaseNow = List.head databaseNowColl
                Expect.isAscending [now; databaseNow; later] "Check database `now` function is accurate"
            }

            testAsync "Reading time with reader safe async" {
                let now : DateTime = DateTime.UtcNow
                let! databaseNowColl =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT NOW()::timestamp AS time"
                    |> Sql.executeReaderSafeAsync (Sql.readRow >> Sql.readTimestamp "time")
                let later : DateTime = now.AddMinutes(1.0)
                Expect.isOk databaseNowColl "Check Result value from database"
                databaseNowColl 
                |> Result.map (fun coll -> 
                    Expect.equal 1 (List.length coll) "Check list is a singleton"
                    let databaseNow = List.head coll
                    Expect.isAscending [now; databaseNow; later] "Check database `now` function is accurate")
                |> ignore
            }

            test "Bytea roundtrip" {
                let bytesInput : array<byte> = [1 .. 5] |> List.map byte |> Array.ofList
                let dbBytes : SqlValue = 
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT @manyBytes"
                    |> Sql.parameters [ "manyBytes", Sql.Value bytesInput ]
                    |> Sql.executeScalar
                Expect.equal (SqlValue.Bytea bytesInput) dbBytes "Check bytes read from database are the same sent"   
            }

            test "Uuid roundtrip" {
                let guid : Guid = Guid.NewGuid()
                let dbUuid : SqlValue =
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT @uuid_input"
                    |> Sql.parameters [ "uuid_input", Sql.Value guid ]
                    |> Sql.executeScalar
                Expect.equal (SqlValue.Uuid guid) dbUuid "Check uuid read from database is the same sent" 
            }

            test "Money roundtrip" {
                let dbMoney : SqlValue =
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT @money_input::money"
                    |> Sql.parameters [ "money_input", Sql.Value 12.5M ]
                    |> Sql.executeScalar
                Expect.equal (SqlValue.Decimal 12.5M) dbMoney "Check money as decimal read from database is the same sent"        
            }

            test "uuid_generate_v4()" {
                let dbUuid : SqlValue =
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT uuid_generate_v4()"
                    |> Sql.executeScalar
                match dbUuid with
                | SqlValue.Uuid uuid -> Expect.isNotNull (uuid.ToString()) "Check database generates an UUID"
                | _ -> failwith "Invalid branch"
            }

            test "Local UTC time" {
                let now : DateTime = DateTime.UtcNow
                let nowTime : TimeSpan = now.TimeOfDay
                let dbTime = 
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT localtime"
                    |> Sql.executeScalar
                    |> Sql.toTime
                let later : TimeSpan = now.AddMinutes(1.0).TimeOfDay
                Expect.isAscending [nowTime; dbTime; later] "Check database `localtime` function is accurate"
            }

            test "String option roundtrip" {
                let a : string option = Some "abc"
                let b : string option = None
                let table =
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT @a, @b"
                    |> Sql.parameters [ "a", Sql.Value a; "b", Sql.Value b ]
                    |> Sql.executeTable
                match table with
                | [[(_, SqlValue.String output); (_, SqlValue.Null)]] ->
                    Expect.equal a (Some output) "Check Option value read from database is the same sent" 
                | _ -> failwith "Invalid branch"  
            }

            // Unhandled Exception: System.NotSupportedException: Npgsql 3.x removed support 
            // for writing a parameter with an IEnumerable value, use .ToList()/.ToArray() instead.
            // Need to add a NpgsqlTypeHandler for Map ?
            test "HStore roundtrip" {
                let inputMap : Map<string, string> = Map ["property", "value from F#"]
                let value = 
                    connection
                    |> Sql.connect
                    |> Sql.query "select @map"
                    |> Sql.parameters ["map", Sql.Value inputMap]
                    |> Sql.executeScalar
                Expect.equal (SqlValue.HStore inputMap) value "Check hstore value read from database is the same sent"                           
            }

        ]

        testList "Sequencial tests that update database state" [

            test "Simple select and Sql.executeTable" {
                let seedDatabase (connection: string) : unit =
                    connection
                    |> Sql.connect
                    |> Sql.executeTransaction [
                        "INSERT INTO fsharp_test (test_id, test_name) values (@id, @name)", [
                            [ "@id", Sql.Value 1; "@name", Sql.Value "first test" ]
                            [ "@id", Sql.Value 2; "@name", Sql.Value "second test" ]
                            [ "@id", Sql.Value 3; "@name", Sql.Value "third test" ]
                        ]
                    ]
                    |> ignore
                cleanDatabase connection
                buildDatabase connection
                seedDatabase connection                
                let table : SqlTable = 
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM fsharp_test"
                    |> Sql.executeTable
                Expect.equal 
                    [
                        [("test_id", SqlValue.Int 1); ("test_name", SqlValue.String "first test")]
                        [("test_id", SqlValue.Int 2); ("test_name", SqlValue.String "second test")]
                        [("test_id", SqlValue.Int 3); ("test_name", SqlValue.String "third test")]
                    ]
                    table
                    "Check all rows from `fsharp_test` table"
            }

            test "Sql.mapEachRow" {
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.executeTransaction [
                        "INSERT INTO fsharp_test (test_id, test_name) values (@id, @name)", [
                            [ "@id", Sql.Value 1; "@name", Sql.Value "first test" ]
                            [ "@id", Sql.Value 2; "@name", Sql.Value "second test" ]
                            [ "@id", Sql.Value 3; "@name", Sql.Value "third test" ]
                        ]
                    ]
                    |> ignore
                cleanDatabase connection
                buildDatabase connection
                seedDatabase connection    
                let table : list<FsTest> = 
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM fsharp_test"
                    |> Sql.prepare
                    |> Sql.executeTable
                    |> Sql.mapEachRow (fun row ->
                        option {
                            let! id = Sql.readInt "test_id" row
                            let! name = Sql.readString "test_name" row
                            return { test_id = id; test_name = name }
                        })
                Expect.equal 
                    [
                        { test_id = 1; test_name = "first test" }
                        { test_id = 2; test_name = "second test" }
                        { test_id = 3; test_name = "third test" }
                    ]
                    table
                    "Check all rows from `fsharp_test` table after mapping them" 
            }

            test "Sql.executeReader" {
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.executeTransaction [
                        "INSERT INTO fsharp_test (test_id, test_name) values (@id, @name)", [
                            [ "@id", Sql.Value 1; "@name", Sql.Value "first test" ]
                            [ "@id", Sql.Value 2; "@name", Sql.Value "second test" ]
                            [ "@id", Sql.Value 3; "@name", Sql.Value "third test" ]
                        ]
                    ]
                    |> ignore
                cleanDatabase connection
                buildDatabase connection
                seedDatabase connection
                let table : list<FsTest> =
                    connection
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM fsharp_test"
                    |> Sql.prepare
                    |> Sql.executeReader (fun reader ->
                        let row = Sql.readRow reader
                        option {
                            let! id = Sql.readInt "test_id" row
                            let! name = Sql.readString "test_name" row
                            return { test_id = id; test_name = name }
                        })    
                Expect.equal 
                    [
                        { test_id = 1; test_name = "first test" }
                        { test_id = 2; test_name = "second test" }
                        { test_id = 3; test_name = "third test" }
                    ]
                    table
                    "Check all rows from `fsharp_test` table using a Reader"
            }

            test "Sql.queryMany and Sql.executeMany" {
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.executeTransaction [
                        "INSERT INTO fsharp_test (test_id, test_name) values (@id, @name)", [
                            [ "@id", Sql.Value 1; "@name", Sql.Value "first test" ]
                            [ "@id", Sql.Value 2; "@name", Sql.Value "second test" ]
                            [ "@id", Sql.Value 3; "@name", Sql.Value "third test" ]
                        ]
                    ]
                    |> ignore
                cleanDatabase connection
                buildDatabase connection
                seedDatabase connection
                let store : string = "SELECT * FROM fsharp_test"
                let storeMetadata : string =
                    Sql.multiline [
                        "select column_name, data_type"
                        "from information_schema.columns"
                        "where table_name = 'fsharp_test'"
                    ]
                let tables : list<SqlTable> = 
                    connection
                    |> Sql.connect
                    |> Sql.queryMany [store; storeMetadata]
                    |> Sql.executeMany
                Expect.equal 2 (List.length tables) "Check number of tables"
                match tables with
                | [store; metadata] ->            
                    Expect.equal 
                        [
                            [("test_id", SqlValue.Int 1); ("test_name", SqlValue.String "first test")]
                            [("test_id", SqlValue.Int 2); ("test_name", SqlValue.String "second test")]
                            [("test_id", SqlValue.Int 3); ("test_name", SqlValue.String "third test")]
                        ]
                        store
                        "Check all rows from `fsharp_test` table"
                    Expect.equal 
                        [
                            [("column_name", SqlValue.String "test_id"); ("data_type", SqlValue.String "integer")]
                            [("column_name", SqlValue.String "test_name"); ("data_type", SqlValue.String "text")]
                        ]
                        metadata
                        "Check metadata rows"
                | _ -> failwith "Invalid branch"
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
                cleanDatabase connection
                buildDatabase connection
                seedDatabase connection inputJson

                let dbJson : SqlValue =
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT data ->> 'property' FROM data_with_jsonb"
                    |> Sql.executeScalar           
                Expect.equal (SqlValue.String jsonData) dbJson "Check json read from database"   
            }

            test "Infinity time" {
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.query "INSERT INTO timestampz_test (version, date1, date2) values (1, 'now', 'infinity')"
                    |> Sql.executeNonQuery
                    |> ignore
                cleanDatabase connection
                buildDatabase connection
                seedDatabase connection

                let dataTable : Result<SqlTable, exn> = 
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM timestampz_test"
                    |> Sql.executeTableSafe
                Expect.isError dataTable "Don't convert infinite timestampz value to DateTime"            
            }

            test "Handle infinity connection" {
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.query "INSERT INTO timestampz_test (version, date1, date2) values (1, 'now', 'infinity')"
                    |> Sql.executeNonQuery
                    |> ignore
                cleanDatabase connection
                buildDatabase connection
                seedDatabase connection
                let dataTable : Result<SqlTable, exn> =
                    handleInfinityConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT date2 FROM timestampz_test"
                    |> Sql.executeTableSafe
                Expect.isOk dataTable "Check query returns Ok Result"
                match dataTable with
                | Ok [[("date2", SqlValue.TimestampWithTimeZone d)]] -> 
                    Expect.equal d DateTime.MaxValue "Database infinite is returned as max value of DateTime"
                | _ -> failwith "Invalid branch"
            }

            test "Handle TimeSpan" {
                let t1 = TimeSpan(13, 45, 23)
                let t2 = TimeSpan(16, 17, 09)
                let t3 = TimeSpan(20, 02, 56)
                let seedDatabase (connection: string) =
                    connection
                    |> Sql.connect
                    |> Sql.executeTransaction [
                        "INSERT INTO timespan_test (id, at) values (@id, @at)", [
                            [ "@id", Sql.Value 1; "@at", Sql.Value t1 ]
                            [ "@id", Sql.Value 2; "@at", Sql.Value t2 ]
                            [ "@id", Sql.Value 3; "@at", Sql.Value t3 ]
                        ]
                    ]
                    |> ignore
                cleanDatabase connection
                buildDatabase connection
                seedDatabase connection       

                // Use `parseEachRow<T>`
                let table : list<TimeSpanTest> = 
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM timespan_test"
                    |> Sql.executeTable
                    |> Sql.parseEachRow<TimeSpanTest>
                Expect.equal
                    [
                        { id = 1; at = t1 }
                        { id = 2; at = t2 }
                        { id = 3; at = t3 }
                    ]
                    table
                    "All rows from `timespan_test` table using `parseEachRow`"

                // Use `mapEachRow` + `readTime`
                let table = 
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM timespan_test"
                    |> Sql.prepare
                    |> Sql.executeTable
                    |> Sql.mapEachRow (fun row ->
                        option {
                            let! id = Sql.readInt "id" row
                            let! at = Sql.readTime "at" row
                            return { id = id; at = at }
                        })     
                Expect.equal
                    [
                        { id = 1; at = TimeSpan(13, 45, 23) }
                        { id = 2; at = TimeSpan(16, 17, 09) }
                        { id = 3; at = TimeSpan(20, 02, 56) }
                    ]
                    table
                    "All rows from `timespan_test` table using `mapEachRow`"                               
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
                            [ "@id", Sql.Value 1; "@values", Sql.Value a ]
                            [ "@id", Sql.Value 2; "@values", Sql.Value b ]
                            [ "@id", Sql.Value 3; "@values", Sql.Value c ]
                        ]
                    ]
                    |> ignore

                cleanDatabase connection
                buildDatabase connection
                seedDatabase connection                

                // Use `parseEachRow<T>`
                let table = 
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM string_array_test"
                    |> Sql.executeTable
                    |> Sql.parseEachRow<StringArrayTest>
                Expect.equal
                    [
                        { id = 1; values = a }
                        { id = 2; values = b }
                        { id = 3; values = c }
                    ]
                    table
                    "All rows from `string_array_test` table using `parseEachRow`"

                // Use `mapEachRow` + `readStringArray`
                let table = 
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM string_array_test"
                    |> Sql.prepare
                    |> Sql.executeTable
                    |> Sql.mapEachRow (fun row ->
                        option {
                            let! id = Sql.readInt "id" row
                            let! values = Sql.readStringArray "values" row
                            return { id = id; values = values }
                        })
                Expect.equal
                    [
                        { id = 1; values = a }
                        { id = 2; values = b }
                        { id = 3; values = c }
                    ]
                    table
                    "All rows from `string_array_test` table using `mapEachRow`"                     
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
                            [ "@id", Sql.Value 1; "@integers", Sql.Value a ]
                            [ "@id", Sql.Value 2; "@integers", Sql.Value b ]
                            [ "@id", Sql.Value 3; "@integers", Sql.Value c ]
                        ]
                    ]
                    |> ignore

                cleanDatabase connection
                buildDatabase connection
                seedDatabase connection

                // Use `parseEachRow<T>`
                let table = 
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM int_array_test"
                    |> Sql.executeTable
                    |> Sql.parseEachRow<IntArrayTest>
                Expect.equal
                    [
                        { id = 1; integers = a }
                        { id = 2; integers = b }
                        { id = 3; integers = c }
                    ]
                    table
                    "All rows from `int_array_test` table using `parseEachRow`"

                // Use `mapEachRow` + `readStringArray`
                let table = 
                    defaultConnection()
                    |> Sql.connect
                    |> Sql.query "SELECT * FROM int_array_test"
                    |> Sql.prepare
                    |> Sql.executeTable
                    |> Sql.mapEachRow (fun row ->
                        option {
                            let! id = Sql.readInt "id" row
                            let! integers = Sql.readIntArray "integers" row
                            return { id = id; integers = integers }
                        })
                Expect.equal
                    [
                        { id = 1; integers = a }
                        { id = 2; integers = b }
                        { id = 3; integers = c }
                    ]
                    table
                    "All rows from `int_array_test` table using `mapEachRow`"                  
            }

        ] |> testSequenced

    ]

[<EntryPoint>]
let main args =
    runTestsWithArgs defaultConfig args tests