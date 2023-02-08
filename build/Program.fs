module Program

open System
open System.IO
open System.Text
open System.Xml
open System.Xml.Linq
open System.Net
open System.Net.Http
open Fake.IO
open Fake.Core
open System.Linq


let path xs = Path.Combine(Array.ofList xs)

let solutionRoot = Files.findParent __SOURCE_DIRECTORY__ "Npgsql.FSharp.sln";

let src = path [ solutionRoot; "src" ]

let tests = path [ solutionRoot; "tests" ]

let build() =
    if Shell.Exec(Tools.dotnet, "build --configuration Release", solutionRoot) <> 0
    then failwith "build failed"

let test() =
    if Shell.Exec(Tools.dotnet, "run --configuration Release", tests) <> 0
    then failwith "Tests failed"

let publish() =
    Shell.deleteDir (path [ src; "bin" ])
    Shell.deleteDir (path [ src; "obj" ])

    if Shell.Exec(Tools.dotnet, "pack --configuration Release", src) <> 0 then
        failwith "Pack failed"
    else
        let nugetKey =
            match Environment.environVarOrNone "NUGET_KEY" with
            | Some nugetKey -> nugetKey
            | None -> 
                printfn "The Nuget API key was not found in a NUGET_KEY environmental variable"
                printf "Enter NUGET_KEY: "
                Console.ReadLine()

        let nugetPath =
            Directory.GetFiles(path [ src; "bin"; "Release" ])
            |> Seq.head
            |> Path.GetFullPath

        if Shell.Exec(Tools.dotnet, sprintf "nuget push %s -s https://api.nuget.org/v3/index.json -k %s" nugetPath nugetKey, src) <> 0
        then failwith "Publish failed"

[<EntryPoint>]
let main (args: string[]) =
    Console.InputEncoding <- Encoding.UTF8
    Console.OutputEncoding <- Encoding.UTF8
    try
        match args with
        | [| "build"   |] -> build()
        | [| "publish" |] -> test(); publish()
        | [| "test" |] -> test()
        | otherwise -> printfn $"Unknown build args %A{otherwise}"
        0
    with ex ->
        printfn "%A" ex
        1
