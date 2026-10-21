/// The body of the `connected` message zero-cache sends once a connection
/// is set up (`connectedBodySchema` in `packages/zero-protocol/src/connect.ts`).
package struct ConnectedBody: Sendable, Hashable, JSONCodable {
  package var wsid: String
  package var timestamp: Double?

  package init(wsid: String, timestamp: Double? = nil) {
    self.wsid = wsid
    self.timestamp = timestamp
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "connected body")
    wsid = try r.string("wsid")
    timestamp = try r.optionalNumber("timestamp")
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    o.set("wsid", wsid)
    o.set("timestamp", timestamp)
    return o.value
  }
}

/// The body of the first message a client sends, usually in the
/// `Sec-WebSocket-Protocol` header (`initConnectionBodySchema` in
/// `packages/zero-protocol/src/connect.ts`).
package struct InitConnectionBody: Sendable, Hashable, JSONCodable {
  package var desiredQueriesPatch: UpQueriesPatch
  /// Sent only when the client has no server snapshot yet: the schema can be
  /// large, and zero-cache keeps it for the client group.
  package var clientSchema: ClientSchema?
  package var deleted: DeleteClientsBody?
  /// Configures the push endpoint of the API server.
  package var userPushURL: String?
  package var userPushHeaders: [String: String]?
  /// Configures the query endpoint of the API server.
  package var userQueryURL: String?
  package var userQueryHeaders: [String: String]?
  /// The client IDs that are running in the client group, so zero-cache can
  /// inactivate the queries of the others.
  package var activeClients: [String]?
  /// A W3C traceparent header for distributed tracing.
  package var traceparent: String?

  package init(
    desiredQueriesPatch: UpQueriesPatch, clientSchema: ClientSchema? = nil,
    deleted: DeleteClientsBody? = nil, userPushURL: String? = nil,
    userPushHeaders: [String: String]? = nil, userQueryURL: String? = nil,
    userQueryHeaders: [String: String]? = nil, activeClients: [String]? = nil,
    traceparent: String? = nil
  ) {
    self.desiredQueriesPatch = desiredQueriesPatch
    self.clientSchema = clientSchema
    self.deleted = deleted
    self.userPushURL = userPushURL
    self.userPushHeaders = userPushHeaders
    self.userQueryURL = userQueryURL
    self.userQueryHeaders = userQueryHeaders
    self.activeClients = activeClients
    self.traceparent = traceparent
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "initConnection body")
    desiredQueriesPatch = try r.required("desiredQueriesPatch", JSONDecode.upQueriesPatch)
    clientSchema = try r.optional("clientSchema", ClientSchema.init(json:))
    deleted = try r.optional("deleted", DeleteClientsBody.init(json:))
    userPushURL = try r.optionalString("userPushURL")
    userPushHeaders = try r.optional("userPushHeaders", JSONDecode.stringRecord)
    userQueryURL = try r.optionalString("userQueryURL")
    userQueryHeaders = try r.optional("userQueryHeaders", JSONDecode.stringRecord)
    activeClients = try r.optional("activeClients", JSONDecode.strings)
    traceparent = try r.optionalString("traceparent")
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    o.set("desiredQueriesPatch", .array(desiredQueriesPatch.map(\.json)))
    o.set("clientSchema", clientSchema?.json)
    o.set("deleted", deleted?.json)
    o.set("userPushURL", userPushURL)
    o.set("userPushHeaders", userPushHeaders.map(JSONValue.stringRecord))
    o.set("userQueryURL", userQueryURL)
    o.set("userQueryHeaders", userQueryHeaders.map(JSONValue.stringRecord))
    o.set("activeClients", activeClients.map(JSONValue.strings))
    o.set("traceparent", traceparent)
    return o.value
  }
}

/// The `Sec-WebSocket-Protocol` value that carries the `initConnection`
/// message and the auth token (`encodeSecProtocols` and
/// `decodeSecProtocols` in `packages/zero-protocol/src/connect.ts`): the
/// UTF-8 of `JSON.stringify({initConnectionMessage, authToken})`, base64
/// encoded, then URI encoded.
package enum SecProtocols {
  package static func encode(initConnection: InitConnectionBody?, authToken: String?) -> String {
    var o = JSONObjectBuilder()
    o.set(
      "initConnectionMessage", initConnection.map { JSONValue.message("initConnection", $0.json) })
    o.set("authToken", authToken)
    let base64 = Base64.encode(JSONSerializer.data(o.value))
    // `encodeURIComponent` of base64 only escapes `+`, `/`, and `=`.
    var out = ""
    out.reserveCapacity(base64.utf8.count)
    for c in base64.unicodeScalars {
      switch c {
      case "+": out += "%2B"
      case "/": out += "%2F"
      case "=": out += "%3D"
      default: out.unicodeScalars.append(c)
      }
    }
    return out
  }

  /// Decodes a protocol header value into the JSON it carries, as
  /// `JSON.parse` returns it: `decodeSecProtocols` does not validate it.
  package static func decode(_ secProtocol: String) throws(DecodingError) -> JSONValue {
    let base64 = try URIComponent.decode(secProtocol)
    guard let bytes = Base64.forgivingDecode(base64) else {
      throw DecodingError("The string to be decoded is not correctly encoded")
    }
    // `new TextDecoder().decode`: replaces invalid UTF-8 and drops a BOM.
    let text = String(
      decoding: bytes.starts(with: utf8BOM) ? bytes.dropFirst(3) : bytes[...], as: UTF8.self)
    return try JSONValue.parseMessage(text)
  }
}

let utf8BOM: [UInt8] = [0xEF, 0xBB, 0xBF]

/// Base64 as `btoa` and `atob` do it.
enum Base64 {
  private static let alphabet = Array(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".utf8)

  static func encode(_ bytes: [UInt8]) -> String {
    var out: [UInt8] = []
    out.reserveCapacity((bytes.count + 2) / 3 * 4)
    var i = 0
    while i + 2 < bytes.count {
      let n = UInt32(bytes[i]) << 16 | UInt32(bytes[i + 1]) << 8 | UInt32(bytes[i + 2])
      out += [
        alphabet[Int(n >> 18 & 63)], alphabet[Int(n >> 12 & 63)], alphabet[Int(n >> 6 & 63)],
        alphabet[Int(n & 63)],
      ]
      i += 3
    }
    let rest = bytes.count - i
    if rest == 1 {
      let n = UInt32(bytes[i]) << 16
      out += [alphabet[Int(n >> 18 & 63)], alphabet[Int(n >> 12 & 63)], 0x3D, 0x3D]
    } else if rest == 2 {
      let n = UInt32(bytes[i]) << 16 | UInt32(bytes[i + 1]) << 8
      out += [
        alphabet[Int(n >> 18 & 63)], alphabet[Int(n >> 12 & 63)], alphabet[Int(n >> 6 & 63)], 0x3D,
      ]
    }
    return String(decoding: out, as: UTF8.self)
  }

  /// The WHATWG "forgiving-base64 decode" that `atob` uses, or `nil` on
  /// failure.
  static func forgivingDecode(_ s: String) -> [UInt8]? {
    var data: [UInt8] = []
    for c in s.unicodeScalars {
      switch c.value {
      // ASCII whitespace.
      case 0x09, 0x0A, 0x0C, 0x0D, 0x20: continue
      case 0..<0x80: data.append(UInt8(c.value))
      default: return nil
      }
    }
    if data.count % 4 == 0 {
      if data.last == 0x3D { data.removeLast() }
      if data.last == 0x3D { data.removeLast() }
    }
    if data.count % 4 == 1 { return nil }
    var out: [UInt8] = []
    out.reserveCapacity(data.count * 3 / 4)
    var buffer: UInt32 = 0
    var bits = 0
    for c in data {
      let v: UInt32
      switch c {
      case 0x41...0x5A: v = UInt32(c - 0x41)
      case 0x61...0x7A: v = UInt32(c - 0x61 + 26)
      case 0x30...0x39: v = UInt32(c - 0x30 + 52)
      case 0x2B: v = 62
      case 0x2F: v = 63
      default: return nil
      }
      buffer = buffer << 6 | v
      bits += 6
      if bits >= 8 {
        bits -= 8
        out.append(UInt8(buffer >> UInt32(bits) & 0xFF))
      }
    }
    return out
  }
}

/// `decodeURIComponent`.
enum URIComponent {
  static func decode(_ s: String) throws(DecodingError) -> String {
    let malformed = DecodingError("URI malformed")
    var scalars = String.UnicodeScalarView()
    let input = Array(s.unicodeScalars)
    var i = 0
    func hexByte(at i: Int) -> UInt8? {
      guard i + 2 < input.count, input[i] == "%", let hi = hexValue(input[i + 1]),
        let lo = hexValue(input[i + 2])
      else {
        return nil
      }
      return hi << 4 | lo
    }
    while i < input.count {
      let c = input[i]
      guard c == "%" else {
        scalars.append(c)
        i += 1
        continue
      }
      guard let first = hexByte(at: i) else { throw malformed }
      i += 3
      if first < 0x80 {
        // `decodeURIComponent` decodes reserved characters too.
        scalars.append(Unicode.Scalar(first))
        continue
      }
      let count: Int
      switch first {
      case 0xC0...0xDF: count = 2
      case 0xE0...0xEF: count = 3
      case 0xF0...0xF7: count = 4
      default: throw malformed
      }
      var bytes = [first]
      for _ in 1..<count {
        guard let b = hexByte(at: i), b & 0xC0 == 0x80 else { throw malformed }
        bytes.append(b)
        i += 3
      }
      guard let decoded = String(validating: bytes, as: UTF8.self) else { throw malformed }
      scalars.append(contentsOf: decoded.unicodeScalars)
    }
    return String(scalars)
  }

  private static func hexValue(_ c: Unicode.Scalar) -> UInt8? {
    switch c.value {
    case 0x30...0x39: UInt8(c.value - 0x30)
    case 0x41...0x46: UInt8(c.value - 0x41 + 10)
    case 0x61...0x66: UInt8(c.value - 0x61 + 10)
    default: nil
    }
  }
}
