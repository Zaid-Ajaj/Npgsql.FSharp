[<AutoOpen>]
module CommonExtensionsAndTypesForNpgsqlFSharp

open System
open System.Collections.Generic
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
    | Money of decimal
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
    | LongArray of int64 array
    | Point of NpgsqlPoint

module internal Utils =
    let sqlMap (option: 'a option) (f: 'a -> SqlValue) : SqlValue =
        Option.defaultValue SqlValue.Null (Option.map f option)

module Async =
    let map f comp =
        async {
            let! result = comp
            return f result
        }

exception MissingQueryException of string
exception NoResultsException of string
exception UnknownColumnException of string

type Sql() =
    static member int(value: int) = SqlValue.Int value
    static member intOrNone(value: int option) = Utils.sqlMap value Sql.int
    static member string(value: string) = SqlValue.String (if isNull value then String.Empty else value)
    static member stringOrNone(value: string option) = Utils.sqlMap value Sql.string
    static member text(value: string) = SqlValue.String value
    static member textOrNone(value: string option) = Sql.stringOrNone value
    static member jsonb(value: string) = SqlValue.Jsonb value
    static member jsonbOrNone(value: string option) = Utils.sqlMap value Sql.jsonb
    static member bit(value: bool) = SqlValue.Bit value
    static member bitOrNone(value: bool option) = Utils.sqlMap value Sql.bit
    static member bool(value: bool) = SqlValue.Bool value
    static member boolOrNone(value: bool option) = Utils.sqlMap value Sql.bool
    static member double(value: double) = SqlValue.Number value
    static member doubleOrNone(value: double option) = Utils.sqlMap value Sql.double
    static member decimal(value: decimal) = SqlValue.Decimal value
    static member decimalOrNone(value: decimal option) = Utils.sqlMap value Sql.decimal
    static member money(value: decimal) = SqlValue.Money value
    static member moneyOrNone(value: decimal option) = Utils.sqlMap value Sql.money
    static member int8(value: int8) = SqlValue.TinyInt value
    static member int8OrNone(value: int8 option) = Utils.sqlMap value Sql.int8
    static member int16(value: int16) = SqlValue.Short value
    static member int16OrNone(value: int16 option) = Utils.sqlMap value Sql.int16
    static member int64(value: int64) = SqlValue.Long value
    static member int64OrNone(value: int64 option) = Utils.sqlMap value Sql.int64
    static member timestamp(value: DateTime) = SqlValue.Timestamp value
    static member timestampOrNone(value: DateTime option) = Utils.sqlMap value Sql.timestamp
    static member timestamptz(value: DateTime) = SqlValue.TimestampWithTimeZone value
    static member timestamptzOrNone(value: DateTime option) = Utils.sqlMap value Sql.timestamptz
    static member timestamptz(value: DateTimeOffset) =
        let parameter = NpgsqlParameter()
        parameter.NpgsqlDbType <- NpgsqlDbType.TimestampTz
        parameter.Value <- value
        SqlValue.Parameter parameter

    static member timestamptzOrNone(value: DateTimeOffset option) =
        match value with
        | None -> SqlValue.Null
        | Some value ->
            let parameter = NpgsqlParameter()
            parameter.NpgsqlDbType <- NpgsqlDbType.TimestampTz
            parameter.Value <- value
            SqlValue.Parameter parameter

    static member uuid(value: Guid) = SqlValue.Uuid value
    static member uuidOrNone(value: Guid option) = Utils.sqlMap value Sql.uuid
    static member uuidArray(value: Guid []) = SqlValue.UuidArray value
    static member uuidArrayOrNone(value: Guid [] option) = Utils.sqlMap value Sql.uuidArray
    static member bytea(value: byte[]) = SqlValue.Bytea value
    static member byteaOrNone(value: byte[] option) = Utils.sqlMap value Sql.bytea
    static member stringArray(value: string[]) = SqlValue.StringArray value
    static member stringArrayOrNone(value: string[] option) = Utils.sqlMap value Sql.stringArray
    static member intArray(value: int[]) = SqlValue.IntArray value
    static member intArrayOrNone(value: int[] option) = Utils.sqlMap value Sql.intArray
    static member int64Array(value: int64[]) = SqlValue.LongArray value
    static member int64ArrayOrNone(value: int64[] option) = Utils.sqlMap value Sql.int64Array
    static member dbnull = SqlValue.Null
    static member parameter(genericParameter: NpgsqlParameter) = SqlValue.Parameter genericParameter
    static member point(value: NpgsqlPoint) = SqlValue.Point value


type RowReader(reader: NpgsqlDataReader) =
    let columnDict = Dictionary<string, int>()
    let columnTypes = Dictionary<string, string>()
    do
        // Populate the names of the columns into a dictionary
        // such that each read doesn't need to loop through all columns
        for fieldIndex in [0 .. reader.FieldCount - 1] do
            columnDict.Add(reader.GetName(fieldIndex), fieldIndex)
            columnTypes.Add(reader.GetName(fieldIndex), reader.GetDataTypeName(fieldIndex))

    let failToRead (column: string) (columnType: string) =
        let availableColumns =
            columnDict.Keys
            |> Seq.map (fun key -> sprintf "[%s:%s]" key columnTypes.[key])
            |> String.concat ", "

        raise
            <| UnknownColumnException
                (sprintf
                     "Could not read column '%s' as %s. Available columns are %s"
                     column
                     columnType
                     availableColumns)
    with

    member this.int(column: string) : int =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetInt32(columnIndex)
        | false, _ -> failToRead column "int"

    member this.intOrNone(column: string) : int option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetInt32(columnIndex))
        | false, _ -> failToRead column "int"

    /// Gets the value of the specified column as a 16-bit signed integer.
    ///
    /// Can be used to read columns of type `smallint` or `int16`
    member this.int16(column: string) : int16 =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetInt16(columnIndex)
        | false, _ -> failToRead column "int16"

    member this.int16OrNone(column: string) : int16 option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetInt16(columnIndex))
        | false, _ -> failToRead column "int16"

    member this.intArray(column: string) : int[] =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<int[]>(columnIndex)
        | false, _ -> failToRead column "int[]"

    member this.intArrayOrNone(column: string) : int[] option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFieldValue<int[]>(columnIndex))
        | false, _ -> failToRead column "int[]"

    member this.int64Array(column: string) : int64[] =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<int64[]>(columnIndex)
        | false, _ -> failToRead column "int64[]"

    member this.int64ArrayOrNone(column: string) : int64[] option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFieldValue<int64[]>(columnIndex))
        | false, _ -> failToRead column "int64[]"

    /// Reads the given column of type timestamptz as DateTimeOffset.
    /// PostgreSQL stores the values of timestamptz as UTC in the database.
    /// However, when Npgsql reads those values, they are converted to local offset of the machine running this code.
    /// See https://www.npgsql.org/doc/types/datetime.html#detailed-behavior-reading-values-from-the-database
    /// This function however, converts it back to UTC the same way it was stored.
    member this.datetimeOffset(column: string) : DateTimeOffset =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<DateTimeOffset>(columnIndex).ToUniversalTime()
        | false, _ -> failToRead column "DateTimeOffset"

    /// Reads the given column of type timestamptz as DateTimeOffset.
    /// PostgreSQL stores the values of timestamptz as UTC in the database.
    /// However, when Npgsql reads those values, they are converted to local offset of the machine running this code.
    /// See https://www.npgsql.org/doc/types/datetime.html#detailed-behavior-reading-values-from-the-database
    /// This function however, converts it back to UTC the same way it was stored.
    member this.datetimeOffsetOrNone(column: string) : DateTimeOffset option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFieldValue<DateTimeOffset>(columnIndex).ToUniversalTime())
        | false, _ ->
            failToRead column "DateTimeOffset"

    member this.stringArray(column: string) : string[] =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<string[]>(columnIndex)
        | false, _ -> failToRead column "string[]"

    member this.stringArrayOrNone(column: string) : string[] option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFieldValue<string[]>(columnIndex))
        | false, _ -> failToRead column "string[]"

    member this.int64(column: string) : int64 =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetInt64(columnIndex)
        | false, _ -> failToRead column "int64"

    member this.int64OrNone(column: string) : int64 option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetInt64(columnIndex))
        | false, _ -> failToRead column "int64"

    member this.string(column: string) : string =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetString(columnIndex)
        | false, _ -> failToRead column "string"

    member this.stringOrNone(column: string) : string option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetString(columnIndex))
        | false, _ -> failToRead column "string"

    member this.text(column: string) : string = this.string column
    member this.textOrNone(column: string) : string option = this.stringOrNone column

    member this.bool(column: string) : bool =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<bool>(columnIndex)
        | false, _ -> failToRead column "bool"

    member this.boolOrNone(column: string) : bool option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFieldValue<bool>(columnIndex))
        | false, _ -> failToRead column "bool"

    member this.decimal(column: string) : decimal =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetDecimal(columnIndex)
        | false, _ -> failToRead column "decimal"

    member this.decimalOrNone(column: string) : decimal option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetDecimal(columnIndex))
        | false, _ -> failToRead column "decimal"

    member this.double(column: string) : double =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetDouble(columnIndex)
        | false, _ -> failToRead column "double"

    member this.doubleOrNone(column: string) : double option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetDouble(columnIndex))
        | false, _ -> failToRead column "double"

    member this.timestamp(column: string) =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetTimeStamp(columnIndex)
        | false, _ -> failToRead column "timestamp"

    member this.timestampOrNone(column: string) =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetTimeStamp(columnIndex))
        | false, _ -> failToRead column "timestamp"

    member this.NpgsqlReader = reader

    member this.timestamptz(column: string) =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetTimeStamp(columnIndex)
        | false, _ -> failToRead column "timestamp"

    member this.timestamptzOrNone(column: string) =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetTimeStamp(columnIndex))
        | false, _ -> failToRead column "timestamp"

    /// Gets the value of the specified column as a globally-unique identifier (GUID).
    member this.uuid(column: string) : Guid =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetGuid(columnIndex)
        | false, _ -> failToRead column "guid"

    member this.uuidOrNone(column: string) : Guid option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some(reader.GetGuid(columnIndex))
        | false, _ -> failToRead column "guid"

    /// Gets the value of the specified column as a globally-unique identifier (GUID).
    member this.uuidArray(column: string) : Guid [] =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<Guid []>(columnIndex)
        | false, _ -> failToRead column "guid[]"

    member this.uuidArrayOrNone(column: string) : Guid [] option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some(reader.GetFieldValue<Guid []>(columnIndex))
        | false, _ -> failToRead column "guid[]"

    /// Gets the value of the specified column as an `NpgsqlTypes.NpgsqlDate`, Npgsql's provider-specific type for dates.
    ///
    /// PostgreSQL's date type represents dates from 4713 BC to 5874897 AD, while .NET's `DateTime` only supports years from 1 to 1999. If you require years outside this range use this accessor.
    ///
    /// See http://www.postgresql.org/docs/current/static/datatype-datetime.html to learn more
    member this.date(column: string) : NpgsqlTypes.NpgsqlDate =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetDate(columnIndex)
        | false, _ -> failToRead column "date"

    /// Gets the value of the specified column as an `NpgsqlTypes.NpgsqlDate`, Npgsql's provider-specific type for dates.
    ///
    /// PostgreSQL's date type represents dates from 4713 BC to 5874897 AD, while .NET's `DateTime` only supports years from 1 to 1999. If you require years outside this range use this accessor.
    ///
    /// See http://www.postgresql.org/docs/current/static/datatype-datetime.html to learn more
    member this.dateOrNone(column: string) : NpgsqlTypes.NpgsqlDate option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull (columnIndex)
            then None
            else Some (reader.GetDate(columnIndex))
        | false, _ -> failToRead column "date"

    /// Gets the value of the specified column as a System.DateTime object.
    member this.dateTime(column: string) : DateTime =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetDateTime(columnIndex)
        | false, _ -> failToRead column "datetime"

    /// Gets the value of the specified column as a System.DateTime object.
    member this.dateTimeOrNone(column: string) : DateTime option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetDateTime(columnIndex))
        | false, _ -> failToRead column "datetime"

    /// Gets the value of the specified column as an `NpgsqlTypes.NpgsqlTimeSpan`, Npgsql's provider-specific type for time spans.
    ///
    /// PostgreSQL's interval type has has a resolution of 1 microsecond and ranges from -178000000 to 178000000 years, while .NET's TimeSpan has a resolution of 100 nanoseconds and ranges from roughly -29247 to 29247 years.
    member this.interval(column: string) : NpgsqlTypes.NpgsqlTimeSpan =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetInterval(columnIndex)
        | false, _ -> failToRead column "interval"

    /// Reads the specified column as `byte[]`
    member this.bytea(column: string) : byte[] =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<byte[]>(columnIndex)
        | false, _ -> failToRead column "byte[]"

    /// Reads the specified column as `byte[]`
    member this.byteaOrNone(column: string) : byte[] option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFieldValue<byte[]>(columnIndex))
        | false, _ -> failToRead column "byte[]"

    /// Gets the value of the specified column as a `System.Single` object.
    member this.float(column: string) : float32 =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFloat(columnIndex)
        | false, _ -> failToRead column "float"

    /// Gets the value of the specified column as a `System.Single` object.
    member this.floatOrNone(column: string) : float32 option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFloat(columnIndex))
        | false, _ -> failToRead column "float"

    /// Gets the value of the specified column as an `NpgsqlTypes.NpgsqlPoint`
    member this.point(column: string) : NpgsqlPoint =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<NpgsqlPoint>(columnIndex)
        | false, _ -> failToRead column "npgsqlpoint"

    /// Gets the value of the specified column as an `NpgsqlTypes.NpgsqlPoint`
    member this.pointOrNone(column: string) : NpgsqlPoint option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFieldValue<NpgsqlPoint>(columnIndex))
        | false, _ -> failToRead column "npgsqlpoint"
