open Expecto

let allTests = testList "All tests" [
    NgpsqlFSharpTests.allTests
]

[<EntryPoint>]
let main args = runTestsWithArgs defaultConfig args allTests