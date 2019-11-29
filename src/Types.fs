namespace Npgsql.FSharp

open System

[<RequireQualifiedAccess>]
type SqlValue =
    | Null
    | Short of int16
    | Int of int
    | Long of int64
    | String of string
    | Date of DateTime
    | Bool of bool
    | Number of double
    | Decimal of decimal
    | Bytea of byte[]
    | HStore of Map<string, string>
    | Uuid of Guid
    | Timestamp of DateTime
    | TimestampWithTimeZone of DateTime
    | Time of TimeSpan
    | TimeWithTimeZone of DateTimeOffset
    | Jsonb of string
    | StringArray of string array
    | IntArray of int array

type SqlRow = list<string * SqlValue>

type SqlTable = list<SqlRow>