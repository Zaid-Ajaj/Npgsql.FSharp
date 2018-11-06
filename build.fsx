#r @"packages/build/FAKE/tools/FakeLib.dll"

open System
open System.IO
open Fake

let libPath = "./src"
let testsPath = "./integration-tests"
let databaseDockerContainerName = "npgsql_fsharp_db"
let databaseDockerImageName = "npgsql_fsharp_image"
let databasePassword = "postgres"



let platformTool tool winTool =
  let tool = if isUnix then tool else winTool
  tool
  |> ProcessHelper.tryFindFileOnPath
  |> function Some t -> t | _ -> failwithf "%s not found" tool

let dockerExePath = "docker"

let mutable dotnetCli = "dotnet"

let run cmd args workingDir =
  let result =
    ExecProcess (fun info ->
      info.FileName <- cmd
      info.WorkingDirectory <- workingDir
      info.Arguments <- args) TimeSpan.MaxValue
  if result <> 0 then failwithf "'%s %s' failed" cmd args

let delete file =
    if File.Exists(file)
    then DeleteFile file
    else ()


type DockerDbStatus =
    | NeverRan
    | Running of string
    | Stopped
    | DockerNotInstalled

let runDockerResults args =
    let result =
        ExecProcessAndReturnMessages(fun info ->
            info.FileName <- dockerExePath
            info.Arguments <- args) TimeSpan.MaxValue
    result

let runDocker args =
    let result =
        ExecProcess (fun info ->
            info.FileName <- dockerExePath
            info.Arguments <- args) TimeSpan.MaxValue
    if result <> 0 then failwithf "docker %s failed" args


Target "Clean" <| fun _ ->
    [ testsPath </> "bin"
      testsPath </> "obj"
      libPath </> "bin"
      libPath </> "obj" ]
    |> CleanDirs


Target "StartDatabase" (fun _ ->
    let processResult (r : ProcessResult) =
        match r.OK, r.Messages.Count with
        | true, x when x = 2 ->
            match (r.Messages.Item 1).Contains("Exit"), (r.Messages.Item 1).Contains("Up") with
            | true, false -> Stopped
            | false, true -> Running (r.Messages.Item 1)
            | _ -> failwith (sprintf "Unable to process result from docker. Message contains both Exit and Up or none : %s" (r.Messages.Item 1))
        | true, x when x = 1 -> NeverRan
        | _ -> DockerNotInstalled

    let result =
        sprintf "ps -a --filter name=%s" databaseDockerContainerName
        |> runDockerResults
        |> processResult

    match result with
    | NeverRan ->
        sprintf "build -t %s %s" databaseDockerImageName testsPath
        |> runDocker
        sprintf "run --name %s -e POSTGRES_PASSWORD=%s -p 5432:5432 -d %s" databaseDockerContainerName databasePassword databaseDockerImageName
        |> runDocker
    | Stopped ->
        sprintf "start %s" databaseDockerContainerName
        |> runDocker
    | Running s ->
        printf "Database already running : %s" s
    | DockerNotInstalled ->
        failwith "Docker not installed"
)

Target "StopDatabase" (fun _ ->

    sprintf "stop %s" databaseDockerContainerName
    |> runDocker
)

Target "RestoreLibProject" <| fun _ ->
  run dotnetCli "restore" libPath

Target "RestoreTestProject" <| fun _ ->
  run dotnetCli "restore" testsPath


let publish projectPath = fun () ->
    [ projectPath </> "bin"
      projectPath </> "obj" ] |> CleanDirs
    run dotnetCli "restore --no-cache" projectPath
    run dotnetCli "pack -c Release" projectPath
    let nugetKey =
        match environVarOrNone "NUGET_KEY" with
        | Some nugetKey -> nugetKey
        | None -> 
            printfn "The Nuget API key must be set in a NUGET_KEY environmental variable"
            System.Console.Write("Nuget API Key: ") 
            System.Console.ReadLine()
    let nupkg =
        Directory.GetFiles(projectPath </> "bin" </> "Release")
        |> Seq.head
        |> Path.GetFullPath

    let pushCmd = sprintf "nuget push %s -s nuget.org -k %s" nupkg nugetKey
    run dotnetCli pushCmd projectPath

Target "PublishNuget" (publish libPath)

Target "Build" <| fun _ -> run "dotnet" "build" libPath

Target "RunTests" <| fun _ ->
    run "dotnet" "run" testsPath

"Clean"
 ==> "RestoreTestProject"
 ==> "RunTests"

"Clean"
 ==> "RestoreLibProject"
 ==> "Build"


"Clean"
 ==> "PublishNuget"

RunTargetOrDefault "Build"