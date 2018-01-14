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
