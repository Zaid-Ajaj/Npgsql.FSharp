# Install

The package `Npgsql.FSharp` is available on [nuget](https://www.nuget.org/packages/Npgsql.FSharp) and can be installed into your F# project as follows:
```bash
cd ./directory/of/project
dotnet add package Npgsql.FSharp
```

### Install for scripting

As of F# 5.0, you can add nuget package refernces to your F# scripts (.fsx files) or even in your F# interactive (fsi) sessions using the following directive:
```fs
> dotnet fsi
> #r "nuget: Npgsql.FSharp";;
```
Then you should be able to start using the library.

> If you don't know which version of F# you have installed, run `dotnet --version` to see the version of the SDK and if that version is 5.0 or more recent, then referencing the package in F# interactive (fsi) should work fine.