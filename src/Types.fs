namespace Npgsql.FSharp

open System
open Npgsql
open NpgsqlTypes

[<RequireQualifiedAccess>]
type SqlValue =
    | Parameter of NpgsqlParameter
    | Null
    | TinyInt of int8
    | Short of int16
    | Int of int
    | Long of int64
    | String of string
    | Date of DateTime
    | Bit of bool
    | Bool of bool
    | Number of double
    | Decimal of decimal
    | Bytea of byte[]
    | Uuid of Guid
    | UuidArray of Guid []
    | Timestamp of DateTime
    | TimestampWithTimeZone of DateTime
    | Time of TimeSpan
    | TimeWithTimeZone of DateTimeOffset
    | Jsonb of string
    | StringArray of string array
    | IntArray of int array
    | Point of NpgsqlPoint
