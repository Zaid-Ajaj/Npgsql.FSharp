#r @"packages/build/FAKE/tools/FakeLib.dll"

open System
open System.IO
open Fake

let libPath = "./src"
let testsPath = "./integration-tests"

let platformTool tool winTool =
  let tool = if isUnix then tool else winTool
  tool
  |> ProcessHelper.tryFindFileOnPath
  |> function Some t -> t | _ -> failwithf "%s not found" tool

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

Target "Clean" <| fun _ ->
    [ testsPath </> "bin" 
      testsPath </> "obj" 
      libPath </> "bin"
      libPath </> "obj" ]
    |> CleanDirs


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
        | None -> failwith "The Nuget API key must be set in a NUGET_KEY environmental variable"
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