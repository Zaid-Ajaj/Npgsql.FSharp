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
    |> Sql.username "postgres" //(getEnv "PG_USER")
    |> Sql.password "postgres" //(getEnv "PG_PASS")
    |> Sql.database "dvdrental" //"sample_db"
    |> Sql.str
    |> Sql.connect

defaultConnection()
|> Sql.query "SELECT * FROM \"actor\""
|> Sql.executeTable
|> Sql.mapEachRow (function
    | [ "Id", Int id
        "UserName", String first_name
        "Password", String last_name
        "LastUpdate", Date last_update] ->
        let user = { Id = id; FirstName = first_name; LastName = last_name; LastUpdate = last_update }
        Some user
    | _ -> None)
|> List.iter (fun user -> printfn "User %s %s was found" user.FirstName user.LastName)

defaultConnection()
|> Sql.func "film_in_stock"
|> Sql.parameters ["p_film_id", Int 1; "p_store_id", Int 1]
|> Sql.executeScalar
|> function
    | Int 1 -> printfn "Film in stock as expected"
    | _ -> failwith "Expected result to be 1"

defaultConnection()
|> Sql.func "film_in_stock"
|> Sql.parameters  ["p_film_id", Int 1; "p_store_id", Int 42]
|> Sql.executeScalar
|> function
    | Null ->  printfn "User was not found as expected"
    | _ -> failwith "Table should not contain this '42' value"

defaultConnection()
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
|> Sql.queryMany [store; storeMetadata]
|> Sql.executeMany
|> List.iter (fun table ->
    printfn "Table:\n%A\n" table)

// let sqlQuery = """
// select * from
//    -- group logs by request id
//    (select json_agg(properties) as "Logs",
//            properties->'Properties'->>'RequestId' as "RequestId"
//     from logs
//     group by properties->'Properties'->>'RequestId') requests

// -- The first log is always the request log, so we filter here by Method
// where "Logs"->0->'Properties'->>'Method' = 'POST'
// """

// Sql.host "localhost"
// |> Sql.username (getEnv "PG_USER")
// |> Sql.password (getEnv "PG_PASS")
// |> Sql.database "Logs"
// |> Sql.str
// |> Sql.connect
// |> Sql.query sqlQuery
// |> Sql.executeTable
// |> printfn "%A"



defaultConnection()
|> Sql.query "CREATE EXTENSION IF NOT EXISTS hstore"
|> Sql.executeNonQuery
|> printfn "Create Extention hstore returned %A"

let inputMap =
    ["property", "value from F#"]
    |> Map.ofSeq

defaultConnection()
|> Sql.query "select @map"
|> Sql.parameters ["map", HStore inputMap]
|> Sql.executeScalar
|> function
    | HStore map ->
        match Map.tryFind "property" map with
        | Some "value from F#" -> "Mapping HStore works"
        | _ -> "Something went wrong when reading HStore"
    | _ -> "Something went wrong when mapping HStore"
|> printfn "%A"

// Unhandled Exception: System.NotSupportedException: Npgsql 3.x removed support for writing a parameter with an IEnumerable value, use .ToList()/.ToArray() instead
// Need to add a NpgsqlTypeHandler for Map ?

let bytesInput =
    [1 .. 5]
    |> List.map byte
    |> Array.ofList

defaultConnection()
|> Sql.query "SELECT @manyBytes"
|> Sql.parameters [ "manyBytes", Bytea bytesInput ]
|> Sql.executeScalar
|> function
    | Bytea output -> if (output <> bytesInput)
                      then failwith "Bytea roundtrip failed, the output was different"
                      else printfn "Bytea roundtrip worked"

    | _ -> failwith "Bytea roundtrip failed"


let guid = System.Guid.NewGuid()

defaultConnection()
|> Sql.query "SELECT @uuid_input"
|> Sql.parameters [ "uuid_input", Uuid guid ]
|> Sql.executeScalar
|> function
    | Uuid output -> if (output.ToString() <> guid.ToString())
                      then failwith "Uuid roundtrip failed, the output was different"
                      else printfn "Uuid roundtrip worked"

    | _ -> failwith "Uuid roundtrip failed"


defaultConnection()
|> Sql.query "SELECT @money_input::money"
|> Sql.parameters [ "money_input", Decimal 12.5M ]
|> Sql.executeScalar
|> function
    | Decimal 12.5M -> printfn "Money as decimal roundtrip worked"
    | _ -> failwith "Money as decimal roundtrip failed"


defaultConnection()
|> Sql.query "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""
|> Sql.executeNonQuery
|> printfn "Create Extention uuid-ossp returned %A"

defaultConnection()
|> Sql.query "SELECT uuid_generate_v4()"
|> Sql.executeScalar
|> function
    | Uuid output -> printfn "Uuid generated: %A" output
    | _ -> failwith "Uuid could not be read failed"