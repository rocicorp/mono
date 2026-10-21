import Testing
import Zero

@Suite(.enabled(if: Vectors.available))
struct MetaTests {
  struct Meta: Decodable {
    let protocolVersion: Int
    let minServerSupportedSyncProtocol: Int
  }

  /// The server keeps supporting every protocol from its historical floor, so
  /// the Swift client may lag the JS one but must stay inside that window.
  @Test func protocolVersionIsSupportedByTheServer() throws {
    let meta = try #require(try Vectors.load("meta", as: Meta.self).first)
    #expect(protocolVersion >= meta.minServerSupportedSyncProtocol)
    #expect(protocolVersion <= meta.protocolVersion)
  }
}
