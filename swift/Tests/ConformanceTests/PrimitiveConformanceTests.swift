import Testing

@testable import Zero

@Suite(.enabled(if: Vectors.available))
struct PrimitiveConformanceTests {
  struct StringCompareCase: Decodable {
    let a: String
    let b: String
    let utf8: Int
    let utf16: Int
  }

  @Test func stringCompare() throws {
    for c in try Vectors.load("string-compare", as: StringCompareCase.self) {
      #expect(
        StringCompare.utf8(c.a, c.b).signum() == c.utf8,
        "\(c.a.debugDescription) vs \(c.b.debugDescription)")
      #expect(
        StringCompare.utf16(c.a, c.b).signum() == c.utf16,
        "\(c.a.debugDescription) vs \(c.b.debugDescription)")
    }
  }

  @Test func compareValues() throws {
    for c in try Vectors.cases("compare-values") {
      let a = c["a"]!
      let b = c["b"]!
      let expected = c["compare"]!
      let got = Result { () throws(ValueCompare.IncomparableError) in
        try ValueCompare.compareChecked(a, b)
      }
      switch (got, expected) {
      case (.success(let n), .number(let want)):
        #expect(Double(n.signum()) == want, "compare(\(a), \(b))")
      case (.failure, .string("throws")):
        break
      default:
        Issue.record("compare(\(a), \(b)) = \(got), want \(expected)")
      }
      #expect(ValueCompare.equal(a, b) == c["equal"]!.boolValue!, "equal(\(a), \(b))")
    }
  }

  struct HashCase: Decodable {
    let input: String
    let h32: UInt32
    let h64: String
    let h128: String
  }

  @Test func hash() throws {
    for c in try Vectors.load("hash", as: HashCase.self) {
      #expect(XXHash32.h32(c.input) == c.h32, "h32(\(c.input.debugDescription))")
      #expect(String(XXHash32.h64(c.input)) == c.h64, "h64(\(c.input.debugDescription))")
      #expect(String(XXHash32.h128(c.input)) == c.h128, "h128(\(c.input.debugDescription))")
    }
  }

  @Test func primaryKeyString() throws {
    for c in try Vectors.cases("primary-key-string") {
      let table = c["table"]!.stringValue!
      let pk = c["primaryKey"]!.arrayValue!.map { $0.stringValue! }
      let row = c["row"]!.objectValue!
      #expect(
        try Keys.primaryKeyString(table: table, primaryKey: pk, row: row) == c["key"]!.stringValue!,
        "\(table) \(pk) \(row)")
    }
  }

  @Test func like() throws {
    for c in try Vectors.cases("like") {
      let pattern = c["pattern"]!.stringValue!
      let ci = c["flags"]!.stringValue! == "i"
      let compiled = Result { () throws(LikePattern.InvalidPattern) in
        try LikePattern(pattern, caseInsensitive: ci)
      }
      guard case .success(let like) = compiled else {
        #expect(c["throws"]!.boolValue!, "pattern \(pattern.debugDescription) should compile")
        continue
      }
      #expect(!c["throws"]!.boolValue!, "pattern \(pattern.debugDescription) should throw")
      for r in c["results"]!.arrayValue! {
        let input = r["input"]!.stringValue!
        #expect(
          like.matches(input) == r["match"]!.boolValue!,
          "\(input.debugDescription) \(ci ? "ILIKE" : "LIKE") \(pattern.debugDescription)")
      }
    }
  }

  @Test func ttl() throws {
    let cases = try Vectors.cases("ttl")
    let ttls = try cases.map { try TTL(json: $0["ttl"]!) }
    for (c, ttl) in zip(cases, ttls) {
      #expect(ttl.parsed == c["parsed"]!.numberValue!, "parseTTL(\(ttl))")
      #expect(ttl.normalized.json == c["normalized"]!, "normalizeTTL(\(ttl))")
      #expect(ttl.clamped == c["clamped"]!.numberValue!, "clampTTL(\(ttl))")
      let signs = c["compare"]!.arrayValue!.map { $0.numberValue! }
      for (other, want) in zip(ttls, signs) {
        let got = TTL.compare(ttl, other)
        #expect((got > 0 ? 1.0 : got < 0 ? -1.0 : 0.0) == want, "compareTTL(\(ttl), \(other))")
      }
    }
  }
}
