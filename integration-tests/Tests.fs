module Main

open Npgsql.FSharp

printfn "Running Postgres integration tests"

type User = { 
    UserId : int
    UserName : string
    Password : string 
}

let getEnv (variable :string) = 
    let variables = System.Environment.GetEnvironmentVariables()
    variables.Keys
    |> Seq.cast<string>
    |> Seq.map (fun key -> key, variables.[key] |> unbox<string>)
    |> Map.ofSeq
    |> Map.tryFind variable
    |> function 
        | Some value -> value
        | None -> failwithf "Could not find value for environment varibale %s" variable

let defaultConnection() = 
    Sql.host "localhost"
    |> Sql.username (getEnv "PG_USER")
    |> Sql.password (getEnv "PG_PASS")
    |> Sql.database "sample_db"
    |> Sql.str
    |> Sql.connect

defaultConnection()
|> Sql.query "SELECT * FROM \"Users\""
|> Sql.executeTable
|> Sql.mapEachRow (function
    | [ "UserId", Int id
        "UserName", String name
        "Password", String pass] -> 
        let user = { UserId = id; UserName = name; Password = pass }
        Some user
    | _ -> None)
|> List.iter (fun user -> printfn "User %s was found" user.UserName)

defaultConnection()
|> Sql.func "user_exists"
|> Sql.parameters ["username", String "john-doe"]
|> Sql.executeScalar
|> Sql.toBool
|> function 
    | true -> printfn "User was found as expected"
    | false -> failwith "Expected results to be true"

defaultConnection()
|> Sql.func "user_exists"
|> Sql.parameters ["username", String "non-existent"]
|> Sql.executeScalar
|> Sql.toBool
|> function 
    | true -> failwith "Table should not contain this 'non-existent' value"
    | false -> printfn "User was not found as expected"

defaultConnection()
|> Sql.query "SELECT NOW()"
|> Sql.executeScalar
|> Sql.toDateTime
|> printfn "%A" 

let users = "SELECT * FROM \"Users\""
let usersMetadata = 
  Sql.multiline
    ["select column_name, data_type" 
     "from information_schema.columns"
     "where table_name = 'Users'"]

defaultConnection()
|> Sql.queryMany [users; usersMetadata]
|> Sql.executeMany
|> List.iter (fun table -> 
    printfn "Table:\n%A\n" table)


// Reading HStore values
// CREATE TABLE test
// ALTER TABLE test ADD COLUMN attrs hstore NOT NULL DEFAULT ''::hstore
defaultConnection()
|> Sql.query "select * from \"test\""
|> Sql.executeTable
|> printfn "%A"


let inputMap = 
    ["property", "value from F#"]
    |> Map.ofSeq

defaultConnection()
|> Sql.query "insert into \"test\" (attrs) values (@map)"
|> Sql.parameters ["map", HStore inputMap]
|> Sql.executeNonQuery
|> printfn "Rows affected %d"


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

    | otherwise -> failwith "Bytea roundtrip failed"


let guid = System.Guid.NewGuid()

defaultConnection()
|> Sql.query "SELECT @uuid_input"
|> Sql.parameters [ "uuid_input", Uuid guid ]
|> Sql.executeScalar
|> function
    | Uuid output -> if (output.ToString() <> guid.ToString()) 
                      then failwith "Uuid roundtrip failed, the output was different"
                      else printfn "Uuid roundtrip worked"
                      
    | otherwise -> failwith "Uuid roundtrip failed"


defaultConnection()
|> Sql.query "SELECT @money_input::money"
|> Sql.parameters [ "money_input", Decimal 12.5M ]
|> Sql.executeScalar
|> function
    | Decimal 12.5M -> printfn "Money as decimal roundtrip worked"
    | otherwise -> failwith "Money as decimal roundtrip failed"