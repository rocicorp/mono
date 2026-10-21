import Testing

@testable import Zero

/// The zero-protocol wire messages, against the vectors of
/// `packages/zero-conformance/src/protocol/messages.test.ts`.
@Suite(.enabled(if: Vectors.available))
struct ProtocolConformanceTests {
  /// A case's message and expected output, given as JSON or, for deeply
  /// nested messages, as JSON text.
  static func messageAndOutput(_ c: JSONValue) throws -> (JSONValue, JSONValue?) {
    if let text = c["messageText"]?.stringValue {
      return (
        try JSONParser.parse(text), try c["outputText"]?.stringValue.map(JSONParser.parse)
      )
    }
    return (c["message"]!, c["output"])
  }

  static func check<M: JSONCodable & Equatable>(_ c: JSONValue, as _: M.Type) throws {
    let (message, output) = try messageAndOutput(c)
    let ok = c["ok"]!.boolValue!
    let decoded: M
    do {
      decoded = try M(json: message)
    } catch {
      #expect(!ok, "rejected \(message): \(error)")
      return
    }
    #expect(ok, "accepted \(message)")
    guard ok, let output else { return }
    #expect(decoded.json == output, "\(message)")
    // What a message encodes to decodes to the same message.
    #expect(try M(json: decoded.json) == decoded, "\(message)")
  }

  @Test func downstream() throws {
    for c in try Vectors.cases("protocol-downstream") { try Self.check(c, as: Downstream.self) }
  }

  @Test(.enabled(if: Vectors.generatedAvailable)) func downstreamFuzz() throws {
    for c in try Vectors.generatedCases("protocol-downstream-fuzz") {
      try Self.check(c, as: Downstream.self)
    }
  }

  @Test func upstream() throws {
    for c in try Vectors.cases("protocol-upstream") { try Self.check(c, as: Upstream.self) }
  }

  @Test(.enabled(if: Vectors.generatedAvailable)) func upstreamFuzz() throws {
    for c in try Vectors.generatedCases("protocol-upstream-fuzz") {
      try Self.check(c, as: Upstream.self)
    }
  }

  static func bytes(hex: String) -> [UInt8] {
    let digits = Array(hex.utf8)
    return stride(from: 0, to: digits.count, by: 2).map {
      UInt8(String(decoding: digits[$0..<$0 + 2], as: UTF8.self), radix: 16)!
    }
  }

  static func checkPokeChunks(_ c: JSONValue) {
    let messages = c["messages"]!.arrayValue!.map { bytes(hex: $0.stringValue!) }
    let ok = c["ok"]!.boolValue!
    do {
      var payloads: [ArraySlice<UInt8>] = []
      for m in messages { payloads.append(try PokeChunk.payload(ofBinaryMessage: m)) }
      let parts = try PokeChunk.decode(payloads)
      #expect(ok, "accepted \(c["messages"]!)")
      if ok { #expect(JSONValue.array(parts.map(\.json)) == c["parts"]!, "\(c["messages"]!)") }
    } catch {
      #expect(!ok, "rejected \(c["messages"]!): \(error)")
    }
  }

  @Test func pokeChunks() throws {
    for c in try Vectors.cases("protocol-poke-chunks") { Self.checkPokeChunks(c) }
  }

  @Test(.enabled(if: Vectors.generatedAvailable)) func pokeChunksFuzz() throws {
    for c in try Vectors.generatedCases("protocol-poke-chunks-fuzz") { Self.checkPokeChunks(c) }
  }

  @Test func secProtocols() throws {
    for c in try Vectors.cases("protocol-sec-protocols") {
      let encoded = c["encoded"]!.stringValue!
      let ok = c["ok"]!.boolValue!
      do {
        let decoded = try SecProtocols.decode(encoded)
        #expect(ok, "accepted \(encoded)")
        if ok { #expect(decoded == c["decoded"]!, "\(encoded)") }
      } catch {
        #expect(!ok, "rejected \(encoded): \(error)")
      }
      let message = c["initConnectionMessage"]
      let authToken = c["authToken"]?.stringValue
      guard message != nil || authToken != nil else { continue }
      // The Swift encoding differs in key order, but carries the same values.
      let body = try message.map {
        try InitConnectionBody(json: try JSONDecode.body(of: $0, tag: "initConnection"))
      }
      let swiftEncoded = SecProtocols.encode(initConnection: body, authToken: authToken)
      #expect(try SecProtocols.decode(swiftEncoded) == c["decoded"]!, "\(encoded)")
    }
  }

  @Test func clientSchema() throws {
    for c in try Vectors.cases("protocol-client-schema") {
      let schema = try ClientSchema(json: c["schema"]!)
      #expect(schema.json == c["schema"]!)
      #expect(
        JSONSerializer.stringify(schema.normalized().json) == c["normalized"]!.stringValue!,
        "\(c["schema"]!)")
    }
  }
}
