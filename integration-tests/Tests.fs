module Main

open Npgsql.FSharp


printfn "Running Postgres integration tests"

type Actor = {
    Id : int
    FirstName : string
    LastName : string
    LastUpdate : System.DateTime
}

let defaultConnection() =
    Sql.host "localhost"
    |> Sql.port 5432
    |> Sql.username "postgres" 
    |> Sql.password "postgres"
    |> Sql.database "dvdrental" 
    |> Sql.str

let handleInfinityConnection() =
    Sql.host "localhost"
    |> Sql.port 5432
    |> Sql.username "postgres"
    |> Sql.password "postgres" 
    |> Sql.database "dvdrental"
    |> Sql.config "Convert Infinity DateTime=true;"
    |> Sql.str

type OptionBuilder() =
    member x.Bind(v,f) = Option.bind f v
    member x.Return v = Some v
    member x.ReturnFrom o = o
    member x.Zero () = None

let option = OptionBuilder()

defaultConnection()
|> Sql.connect
|> Sql.query "SELECT * FROM \"actor\""
|> Sql.executeTable
|> Sql.mapEachRow (function
    | [ "Id", SqlValue.Int id
        "UserName", SqlValue.String first_name
        "Password", SqlValue.String last_name
        "LastUpdate", SqlValue.Date last_update] ->
        let user = { Id = id; FirstName = first_name; LastName = last_name; LastUpdate = last_update }
        Some user
    | _ -> None)
|> List.iter (fun user -> printfn "User %s %s was found" user.FirstName user.LastName)

defaultConnection()
|> Sql.connect
|> Sql.query "SELECT * FROM \"actor\""
|> Sql.executeTable
|> List.iter (printfn "%A")


defaultConnection()
|> Sql.connect
|> Sql.query "SELECT * FROM \"actor\""
|> Sql.prepare
|> Sql.executeTable
|> Sql.mapEachRow (fun row -> 
    option {
        let! id = Sql.readInt "actor_id" row
        let! firstName = Sql.readString "first_name" row
        let! lastName = Sql.readString "last_name" row 
        let! lastUpdate = Sql.readDate "last_update" row
        return { 
            Id = id;
            FirstName = firstName
            LastName = lastName 
            LastUpdate = lastUpdate
        }
    })
|> printfn "%A"

defaultConnection()
|> Sql.connect
|> Sql.func "film_in_stock"
|> Sql.parameters [ "p_film_id", Sql.Value 1; "p_store_id", Sql.Value 1]
|> Sql.executeScalar
|> function
    | SqlValue.Int 1 -> printfn "Film in stock as expected"
    | _ -> failwith "Expected result to be 1"


defaultConnection()
|> Sql.connect
|> Sql.func "film_in_stock"
|> Sql.parameters  ["p_film_id", Sql.Value 1; "p_store_id", Sql.Value 42]
|> Sql.executeScalar
|> function
    | SqlValue.Null ->  printfn "User was not found as expected"
    | _ -> failwith "Table should not contain this '42' value"

printfn "Null roundtrip start"

defaultConnection()
|> Sql.connect 
|> Sql.query "SELECT @nullValue"
|> Sql.parameters [ "nullValue", SqlValue.Null ]
|> Sql.executeScalar
|> function 
    | SqlValue.Null -> printfn "Succesfully returned null"
    | otherwise -> printfn "Unexpected %A" otherwise

printfn "Null roundtrip end"

defaultConnection()
|> Sql.connect
|> Sql.query "SELECT NOW()"
|> Sql.executeScalar
|> Sql.toDateTime
|> printfn "%A"

let store = "SELECT * FROM \"store\""
let storeMetadata =
  Sql.multiline
    ["select column_name, data_type"
     "from information_schema.columns"
     "where table_name = 'store'"]

defaultConnection()
|> Sql.connect
|> Sql.queryMany [store; storeMetadata]
|> Sql.executeMany
|> List.iter (fun table ->
    printfn "Table:\n%A\n" table)


defaultConnection()
|> Sql.connect
|> Sql.query "CREATE EXTENSION IF NOT EXISTS hstore"
|> Sql.executeNonQuery
|> printfn "Create Extention hstore returned %A"

let inputMap =
    ["property", "value from F#"]
    |> Map.ofSeq

printfn "HStore roundtrip start"

defaultConnection()
|> Sql.connect
|> Sql.query "select @map"
|> Sql.parameters ["map", Sql.Value inputMap]
|> Sql.executeScalar
|> function
    | SqlValue.HStore map ->
        match Map.tryFind "property" map with
        | Some "value from F#" -> "Mapping HStore works"
        | _ -> "Something went wrong when reading HStore"
    | _ -> "Something went wrong when mapping HStore"
|> printfn "%A"

printfn "HStore roundtrip end"

let inputJson = "{\"property\": \"value from F#\"}"

printfn "Jsonb roundtrip start"

defaultConnection()
|> Sql.connect
|> Sql.query "select @jsonb"
|> Sql.parameters ["jsonb", SqlValue.Jsonb inputJson]
|> Sql.executeScalar
|> function
    | SqlValue.Jsonb json ->
        match inputJson = json with
        | true -> "Mapping Jsonb works"
        | _ -> sprintf "Something went wrong when reading Jsonb, expected %s but got %s" inputJson json
    | x -> sprintf "Something went wrong when mapping Jsonb, %A" x
|> printfn "%A"

printfn "Jsonb roundtrip end"

// Unhandled Exception: System.NotSupportedException: Npgsql 3.x removed support for writing a parameter with an IEnumerable value, use .ToList()/.ToArray() instead
// Need to add a NpgsqlTypeHandler for Map ?

let bytesInput =
    [1 .. 5]
    |> List.map byte
    |> Array.ofList

defaultConnection()
|> Sql.connect
|> Sql.query "SELECT @manyBytes"
|> Sql.parameters [ "manyBytes", Sql.Value bytesInput ]
|> Sql.executeScalar
|> function
    | SqlValue.Bytea output -> 
        if (output <> bytesInput)
        then failwith "Bytea roundtrip failed, the output was different"
        else printfn "Bytea roundtrip worked"

    | _ -> failwith "Bytea roundtrip failed"

let guid = System.Guid.NewGuid()

defaultConnection()
|> Sql.connect
|> Sql.query "SELECT @uuid_input"
|> Sql.parameters [ "uuid_input", Sql.Value guid ]
|> Sql.executeScalar
|> function
    | SqlValue.Uuid output -> 
        if (output.ToString() <> guid.ToString())
        then failwith "Uuid roundtrip failed, the output was different"
        else printfn "Uuid roundtrip worked"

    | _ -> failwith "Uuid roundtrip failed"


defaultConnection()
|> Sql.connect
|> Sql.query "SELECT @money_input::money"
|> Sql.parameters [ "money_input", Sql.Value 12.5M ]
|> Sql.executeScalar
|> function
    | SqlValue.Decimal 12.5M -> printfn "Money as decimal roundtrip worked"
    | _ -> failwith "Money as decimal roundtrip failed"


defaultConnection()
|> Sql.connect
|> Sql.query "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""
|> Sql.executeNonQuery
|> printfn "Create Extention uuid-ossp returned %A"

defaultConnection()
|> Sql.connect
|> Sql.query "SELECT uuid_generate_v4()"
|> Sql.executeScalar
|> function
    | SqlValue.Uuid output -> printfn "Uuid generated: %A" output
    | _ -> failwith "Uuid could not be read failed"

defaultConnection()
|> Sql.connect
|> Sql.query "CREATE TABLE IF NOT EXISTS data (version integer, date1 timestamptz, date2 timestamptz) "
|> Sql.executeNonQuery
|> printfn "Create Table data returned %A"

let delete = "DELETE from data"
let insert = "INSERT INTO data (version, date1, date2) values (1, 'now', 'infinity')"

defaultConnection()
|> Sql.connect
|> Sql.queryMany [ delete; insert ]
|> Sql.executeMany
|> printfn "Insert into Table data returned %A"

defaultConnection()
|> Sql.connect
|> Sql.query "SELECT * FROM data"
|> Sql.executeTableSafe
|> function
    | Ok _ -> failwith "Should be able to convert infinity to datetime"
    | Error ex -> printfn "Fails as expected with %A" ex.Message

handleInfinityConnection()
|> Sql.connect
|> Sql.query "SELECT * FROM data"
|> Sql.executeTableSafe
|> function
    | Ok r -> printfn "Succeed as expected : %A vs %A" (r.Head.Item 2) System.DateTime.MaxValue
    | Error _ -> failwith "Should not fail"

defaultConnection()
|> Sql.connect
|> Sql.query "DROP TABLE data"
|> Sql.executeNonQuery
|> printfn "Drop Table data returned %A"
