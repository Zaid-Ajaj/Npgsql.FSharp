# Npgsql.FSharp

A library that provide a simple F# API to work with PostgreSQL databases. Built on top of [Npgsql](https://www.npgsql.org/) which is the .NET driver for Postgres databases.

For an optimal developer experience, this library is made to work with [Npgsql.FSharp.Analyzer](https://github.com/Zaid-Ajaj/Npgsql.FSharp.Analyzer) which is a F# code analyzer integrated into your IDE that will verify the query syntax and perform type-checking against the parameters and the types of the columns from the result set.

> The analyzer doens't support Rider yet