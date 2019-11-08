namespace Npgsql.FSharp.OptionWorkflow

/// A simple option workflow implementation
type OptionBuilder() =
    member x.Bind(value, map) = Option.bind map value
    member x.Return value = Some value
    member x.ReturnFrom value = value
    member x.Zero () = None

[<AutoOpen>]
module OptionBuilderImplementation =
    /// A simple option workflow implementation that allows you to easily chain optionals together.
    let option = OptionBuilder()