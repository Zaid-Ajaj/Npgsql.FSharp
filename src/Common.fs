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
    | ShortArray of int16 array
    | LongArray of int64 array
    | Point of NpgsqlPoint

module internal Utils =
    let sqlMap (option: 'a option) (f: 'a -> SqlValue) : SqlValue =
        Option.defaultValue SqlValue.Null (Option.map f option)

    let sqlValueMap (option: 'a voption) (f: 'a -> SqlValue) : SqlValue =
        ValueOption.defaultValue SqlValue.Null (ValueOption.map f option)

exception MissingQueryException of string
exception NoResultsException of string
exception UnknownColumnException of string

type Sql() =
    static member int(value: int) = SqlValue.Int value
    static member intOrNone(value: int option) = Utils.sqlMap value Sql.int
    static member intOrValueNone(value: int voption) = Utils.sqlValueMap value Sql.int
    static member string(value: string) = SqlValue.String (if isNull value then String.Empty else value)
    static member stringOrNone(value: string option) = Utils.sqlMap value Sql.string
    static member stringOrValueNone(value: string voption) = Utils.sqlValueMap value Sql.string
    static member text(value: string) = SqlValue.String value
    static member textOrNone(value: string option) = Sql.stringOrNone value
    static member textOrValueNone(value: string voption) = Sql.stringOrValueNone value
    static member jsonb(value: string) = SqlValue.Jsonb value
    static member jsonbOrNone(value: string option) = Utils.sqlMap value Sql.jsonb
    static member jsonbOrValueNone(value: string voption) = Utils.sqlValueMap value Sql.jsonb
    static member bit(value: bool) = SqlValue.Bit value
    static member bitOrNone(value: bool option) = Utils.sqlMap value Sql.bit
    static member bitOrValueNone(value: bool voption) = Utils.sqlValueMap value Sql.bit
    static member bool(value: bool) = SqlValue.Bool value
    static member boolOrNone(value: bool option) = Utils.sqlMap value Sql.bool
    static member boolOrValueNone(value: bool voption) = Utils.sqlValueMap value Sql.bool
    static member double(value: double) = SqlValue.Number value
    static member doubleOrNone(value: double option) = Utils.sqlMap value Sql.double
    static member doubleOrValueNone(value: double voption) = Utils.sqlValueMap value Sql.double
    static member decimal(value: decimal) = SqlValue.Decimal value
    static member decimalOrNone(value: decimal option) = Utils.sqlMap value Sql.decimal
    static member decimalOrValueNone(value: decimal voption) = Utils.sqlValueMap value Sql.decimal
    static member money(value: decimal) = SqlValue.Money value
    static member moneyOrNone(value: decimal option) = Utils.sqlMap value Sql.money
    static member moneyOrValueNone(value: decimal voption) = Utils.sqlValueMap value Sql.money
    static member int8(value: int8) = SqlValue.TinyInt value
    static member int8OrNone(value: int8 option) = Utils.sqlMap value Sql.int8
    static member int8OrValueNone(value: int8 voption) = Utils.sqlValueMap value Sql.int8
    static member int16(value: int16) = SqlValue.Short value
    static member int16OrNone(value: int16 option) = Utils.sqlMap value Sql.int16
    static member int16OrValueNone(value: int16 voption) = Utils.sqlValueMap value Sql.int16
    static member int64(value: int64) = SqlValue.Long value
    static member int64OrNone(value: int64 option) = Utils.sqlMap value Sql.int64
    static member int64OrValueNone(value: int64 voption) = Utils.sqlValueMap value Sql.int64
    static member timestamp(value: DateTime) = SqlValue.Timestamp value
    static member timestampOrNone(value: DateTime option) = Utils.sqlMap value Sql.timestamp
    static member timestampOrValueNone(value: DateTime voption) = Utils.sqlValueMap value Sql.timestamp
    static member timestamptz(value: DateTime) = SqlValue.TimestampWithTimeZone value
    static member timestamptzOrNone(value: DateTime option) = Utils.sqlMap value Sql.timestamptz
    static member timestamptzOrValueNone(value: DateTime voption) = Utils.sqlValueMap value Sql.timestamptz
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

    static member timestamptzOrValueNone(value: DateTimeOffset voption) =
        match value with
        | ValueNone -> SqlValue.Null
        | ValueSome value ->
            let parameter = NpgsqlParameter()
            parameter.NpgsqlDbType <- NpgsqlDbType.TimestampTz
            parameter.Value <- value
            SqlValue.Parameter parameter

    static member uuid(value: Guid) = SqlValue.Uuid value
    static member uuidOrNone(value: Guid option) = Utils.sqlMap value Sql.uuid
    static member uuidOrValueNone(value: Guid voption) = Utils.sqlValueMap value Sql.uuid
    static member uuidArray(value: Guid []) = SqlValue.UuidArray value
    static member uuidArrayOrNone(value: Guid [] option) = Utils.sqlMap value Sql.uuidArray
    static member uuidArrayOrValueNone(value: Guid [] voption) = Utils.sqlValueMap value Sql.uuidArray
    static member bytea(value: byte[]) = SqlValue.Bytea value
    static member byteaOrNone(value: byte[] option) = Utils.sqlMap value Sql.bytea
    static member byteaOrValueNone(value: byte[] voption) = Utils.sqlValueMap value Sql.bytea
    static member stringArray(value: string[]) = SqlValue.StringArray value
    static member stringArrayOrNone(value: string[] option) = Utils.sqlMap value Sql.stringArray
    static member stringArrayOrValueNone(value: string[] voption) = Utils.sqlValueMap value Sql.stringArray
    static member intArray(value: int[]) = SqlValue.IntArray value
    static member intArrayOrNone(value: int[] option) = Utils.sqlMap value Sql.intArray
    static member intArrayOrValueNone(value: int[] voption) = Utils.sqlValueMap value Sql.intArray
    static member int16Array(value: int16[]) = SqlValue.ShortArray value
    static member int16ArrayOrNone(value: int16[] option) = Utils.sqlMap value Sql.int16Array
    static member int16ArrayOrValueNone(value: int16[] voption) = Utils.sqlValueMap value Sql.int16Array
    static member int64Array(value: int64[]) = SqlValue.LongArray value
    static member int64ArrayOrNone(value: int64[] option) = Utils.sqlMap value Sql.int64Array
    static member int64ArrayOrValueNone(value: int64[] voption) = Utils.sqlValueMap value Sql.int64Array
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

    /// Get the column value as the type specified in the type parameter.
    /// Can be used when the package is used in conjuction with the Json.NET type plugin for npgsql.
    member this.fieldValue<'a>(column: string) : 'a =
        match columnDict.TryGetValue(column) with
        | true, columnIndex -> reader.GetFieldValue<'a>(columnIndex)
        | false, _ -> let typeName = typeof<'a>.ToString()
                      failToRead column typeName

    member this.fieldValueOrNone<'a>(column: string) : 'a option =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then None
            else Some (reader.GetFieldValue<'a>(columnIndex))
        | false, _ -> let typeName = typeof<'a>.ToString()
                      failToRead column typeName

    member this.fieldValueOrValueNone<'a>(column: string) : 'a voption =
        match columnDict.TryGetValue(column) with
        | true, columnIndex ->
            if reader.IsDBNull(columnIndex)
            then ValueNone
            else ValueSome (reader.GetFieldValue<'a>(columnIndex))
        | false, _ -> let typeName = typeof<'a>.ToString()
                      failToRead column typeName

    member this.int(column: string) : int =
        this.fieldValue(column)

    member this.intOrNone(column: string) : int option =
        this.fieldValueOrNone(column)

    member this.intOrValueNone(column: string) : int voption =
        this.fieldValueOrValueNone(column)

    /// Gets the value of the specified column as a 16-bit signed integer.
    ///
    /// Can be used to read columns of type `smallint` or `int16`
    member this.int16(column: string) : int16 =
        this.fieldValue(column)

    member this.int16OrNone(column: string) : int16 option =
        this.fieldValueOrNone(column)

    member this.int16OrValueNone(column: string) : int16 voption =
        this.fieldValueOrValueNone(column)

    member this.intArray(column: string) : int[] =
        this.fieldValue(column)

    member this.intArrayOrNone(column: string) : int[] option =
        this.fieldValueOrNone(column)

    member this.intArrayOrValueNone(column: string) : int[] voption =
        this.fieldValueOrValueNone(column);

    member this.int16Array(column: string) : int16[] =
        this.fieldValue(column)

    member this.int16ArrayOrNone(column: string) : int16[] option =
        this.fieldValueOrNone(column)

    member this.int16ArrayOrValueNone(column: string) : int16[] voption =
        this.fieldValueOrValueNone(column)

    member this.int64Array(column: string) : int64[] =
        this.fieldValue(column)

    member this.int64ArrayOrNone(column: string) : int64[] option =
        this.fieldValueOrNone(column)

    member this.int64ArrayOrValueNone(column: string) : int64[] voption =
        this.fieldValueOrValueNone(column)

    /// Reads the given column of type timestamptz as DateTimeOffset.
    /// PostgreSQL stores the values of timestamptz as UTC in the database.
    /// However, when Npgsql reads those values, they are converted to local offset of the machine running this code.
    /// See https://www.npgsql.org/doc/types/datetime.html#detailed-behavior-reading-values-from-the-database
    /// This function however, converts it back to UTC the same way it was stored.
    member this.datetimeOffset(column: string) : DateTimeOffset =
        this.fieldValue<DateTimeOffset>(column).ToUniversalTime()

    /// Reads the given column of type timestamptz as DateTimeOffset.
    /// PostgreSQL stores the values of timestamptz as UTC in the database.
    /// However, when Npgsql reads those values, they are converted to local offset of the machine running this code.
    /// See https://www.npgsql.org/doc/types/datetime.html#detailed-behavior-reading-values-from-the-database
    /// This function however, converts it back to UTC the same way it was stored.
    member this.datetimeOffsetOrNone(column: string) : DateTimeOffset option =
        this.fieldValueOrNone<DateTimeOffset>(column)
        |> Option.map (fun dt -> dt.ToUniversalTime())

    /// Reads the given column of type timestamptz as DateTimeOffset.
    /// PostgreSQL stores the values of timestamptz as UTC in the database.
    /// However, when Npgsql reads those values, they are converted to local offset of the machine running this code.
    /// See https://www.npgsql.org/doc/types/datetime.html#detailed-behavior-reading-values-from-the-database
    /// This function however, converts it back to UTC the same way it was stored.
    member this.datetimeOffsetOrValueNone(column: string) : DateTimeOffset voption =
        this.fieldValueOrValueNone<DateTimeOffset>(column)
        |> ValueOption.map (fun dt -> dt.ToUniversalTime())

    member this.stringArray(column: string) : string[] =
      this.fieldValue(column)

    member this.stringArrayOrNone(column: string) : string[] option =
      this.fieldValueOrNone(column)

    member this.stringArrayOrValueNone(column: string) : string[] voption =
        this.fieldValueOrValueNone(column)

    member this.int64(column: string) : int64 =
        this.fieldValue(column)

    member this.int64OrNone(column: string) : int64 option =
        this.fieldValueOrNone(column)

    member this.int64OrValueNone(column: string) : int64 voption =
        this.fieldValueOrValueNone(column)

    member this.string(column: string) : string =
        this.fieldValue(column)

    member this.stringOrNone(column: string) : string option =
        this.fieldValueOrNone(column)

    member this.stringOrValueNone(column: string) : string voption =
        this.fieldValueOrValueNone(column)

    /// <summary>Alias for reading string</summary>
    member this.text(column: string) : string = this.string column
    /// <summary>Alias for reading stringOrNone</summary>
    member this.textOrNone(column: string) : string option = this.stringOrNone column
    /// <summary>Alias for reading stringOrValueNone</summary>
    member this.textOrValueNone(column: string) : string voption = this.stringOrValueNone column
    /// <summary>Reads a column as a boolean value</summary>
    member this.bool(column: string) : bool =
        this.fieldValue(column)
    /// <summary>Reads a column as a boolean value or returns None when the column value is null</summary>
    member this.boolOrNone(column: string) : bool option =
        this.fieldValueOrNone(column)
    /// <summary>Reads a column as a boolean value or returns ValueNone when the column value is null</summary>
    member this.boolOrValueNone(column: string) : bool voption =
        this.fieldValueOrValueNone(column)
    /// <summary>Reads the column value as decimal</summary>
    member this.decimal(column: string) : decimal =
        this.fieldValue(column)
    /// <summary>Reads the column value as decimal or returns None when the column is null</summary>
    member this.decimalOrNone(column: string) : decimal option =
        this.fieldValueOrNone(column)
    /// <summary>Reads the column value as decimal or returns ValueNone when the column is null</summary>
    member this.decimalOrValueNone(column: string) : decimal voption =
        this.fieldValueOrValueNone(column)

    member this.double(column: string) : double =
        this.fieldValue(column)

    member this.doubleOrNone(column: string) : double option =
        this.fieldValueOrNone(column)

    member this.doubleOrValueNone(column: string) : double voption =
        this.fieldValueOrValueNone(column)

    member this.NpgsqlReader = reader

    /// Gets the value of the specified column as a globally-unique identifier (GUID).
    member this.uuid(column: string) : Guid =
        this.fieldValue(column)

    member this.uuidOrNone(column: string) : Guid option =
        this.fieldValueOrNone(column)

    member this.uuidOrValueNone(column: string) : Guid voption =
        this.fieldValueOrValueNone(column)

    /// Gets the value of the specified column as a globally-unique identifier (GUID).
    member this.uuidArray(column: string) : Guid [] =
        this.fieldValue(column)

    member this.uuidArrayOrNone(column: string) : Guid [] option =
        this.fieldValue(column)

    member this.uuidArrayOrValueNone(column: string) : Guid [] voption =
        this.fieldValueOrValueNone(column)

    /// Gets the value of the specified column as a System.DateTime object.
    member this.dateTime(column: string) : DateTime =
        this.fieldValue(column)

    /// Gets the value of the specified column as a System.DateTime object.
    member this.dateTimeOrNone(column: string) : DateTime option =
        this.fieldValueOrNone(column)

    member this.dateTimeOrValueNone(column: string) : DateTime voption =
        this.fieldValueOrValueNone(column)

    /// <summary>Reads the specified column as byte[]</summary>
    member this.bytea(column: string) : byte[] =
        this.fieldValue(column)

    /// <summary>Reads the specified column as byte[] or returns None when the column value is null</summary>
    member this.byteaOrNone(column: string) : byte[] option =
        this.fieldValueOrNone(column)
    /// <summary>Reads the specified column as byte[] or returns ValueNone when the column value is null</summary>
    member this.byteaOrValueNone(column: string) : byte[] voption =
        this.fieldValueOrValueNone(column)

    /// Gets the value of the specified column as a `System.Single` object.
    member this.float(column: string) : float32 =
        this.fieldValue(column)

    /// Gets the value of the specified column as a `System.Single` object.
    member this.floatOrNone(column: string) : float32 option =
        this.fieldValueOrNone(column)

    member this.floatOrValueNone(column: string) : float32 voption =
        this.fieldValueOrValueNone(column)

    /// Gets the value of the specified column as an `NpgsqlTypes.NpgsqlPoint`
    member this.point(column: string) : NpgsqlPoint =
        this.fieldValue(column)

    /// Gets the value of the specified column as an `NpgsqlTypes.NpgsqlPoint`
    member this.pointOrNone(column: string) : NpgsqlPoint option =
        this.fieldValueOrNone(column)

    member this.pointOrValueNone(column: string) : NpgsqlPoint voption =
        this.fieldValueOrValueNone(column)