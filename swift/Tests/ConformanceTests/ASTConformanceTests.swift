import Testing

@testable import Zero

@Suite(.enabled(if: Vectors.available))
struct ASTConformanceTests {
  static func check(_ c: JSONValue) throws {
    let ast = try AST(json: c["ast"]!)
    // Decoding then encoding is lossless.
    #expect(ast.json == c["ast"]!)
    let normalized = ast.normalized()
    #expect(normalized.json == c["normalized"]!, "normalize \(c["ast"]!)")
    #expect(QueryHash.hashOfAST(ast) == c["hash"]!.stringValue!, "hash \(c["ast"]!)")
  }

  @Test func curated() throws {
    for c in try Vectors.cases("ast") { try Self.check(c) }
  }

  @Test(.enabled(if: Vectors.generatedAvailable)) func fuzz() throws {
    for c in try Vectors.generatedCases("ast-fuzz") { try Self.check(c) }
  }

  @Test func nameAndArgsHash() throws {
    for c in try Vectors.cases("name-args-hash") {
      let name = c["name"]!.stringValue!
      let args = c["args"]!.arrayValue!
      #expect(QueryHash.hashOfNameAndArgs(name, args) == c["hash"]!.stringValue!, "\(name) \(args)")
    }
  }
}
