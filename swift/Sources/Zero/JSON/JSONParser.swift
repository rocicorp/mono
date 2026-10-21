/// Parses JSON text as `JSON.parse` does.
///
/// Differences from JS are limited to what a Swift `String` can represent: an
/// escaped lone surrogate (`"\ud800"`) becomes U+FFFD.
package struct JSONParser {
  package struct Error: Swift.Error, CustomStringConvertible, Equatable {
    package let message: String
    package let offset: Int
    package var description: String { "\(message) at offset \(offset)" }
  }

  /// Nesting deeper than this is rejected instead of overflowing the stack.
  package static let maxDepth = 5_000

  package static func parse(_ text: String) throws(Error) -> JSONValue {
    var text = text
    let result = text.withUTF8 { buffer in
      Result { () throws(Error) in try parse(buffer) }
    }
    return try result.get()
  }

  package static func parse(_ bytes: [UInt8]) throws(Error) -> JSONValue {
    let result = bytes.withUnsafeBufferPointer { buffer in
      Result { () throws(Error) in try parse(buffer) }
    }
    return try result.get()
  }

  package static func parse(_ buffer: UnsafeBufferPointer<UInt8>) throws(Error) -> JSONValue {
    var p = JSONParser(buffer)
    p.skipWhitespace()
    let value = try p.parseValue(depth: 0)
    p.skipWhitespace()
    if p.i != p.end { throw p.error("Unexpected non-whitespace character after JSON") }
    return value
  }

  private let b: UnsafeBufferPointer<UInt8>
  private var i: Int
  private let end: Int

  private init(_ buffer: UnsafeBufferPointer<UInt8>) {
    b = buffer
    i = 0
    end = buffer.count
  }

  private func error(_ message: String) -> Error { Error(message: message, offset: i) }

  private mutating func skipWhitespace() {
    while i < end {
      switch b[i] {
      case 0x20, 0x09, 0x0A, 0x0D: i += 1
      default: return
      }
    }
  }

  private mutating func parseValue(depth: Int) throws(Error) -> JSONValue {
    guard i < end else { throw error("Unexpected end of JSON input") }
    switch b[i] {
    case UInt8(ascii: "{"): return try parseObject(depth: depth)
    case UInt8(ascii: "["): return try parseArray(depth: depth)
    case UInt8(ascii: "\""): return .string(try parseString())
    case UInt8(ascii: "t"): try expectLiteral("true"); return .bool(true)
    case UInt8(ascii: "f"): try expectLiteral("false"); return .bool(false)
    case UInt8(ascii: "n"): try expectLiteral("null"); return .null
    case UInt8(ascii: "-"), UInt8(ascii: "0")...UInt8(ascii: "9"): return .number(try parseNumber())
    default: throw error("Unexpected token")
    }
  }

  private mutating func expectLiteral(_ literal: StaticString) throws(Error) {
    let n = literal.utf8CodeUnitCount
    guard end - i >= n else { throw error("Unexpected end of JSON input") }
    let p = literal.utf8Start
    for k in 0..<n where b[i + k] != p[k] {
      throw error("Unexpected token")
    }
    i += n
  }

  private mutating func parseObject(depth: Int) throws(Error) -> JSONValue {
    if depth >= Self.maxDepth { throw error("JSON nested too deeply") }
    i += 1  // {
    var object: [String: JSONValue] = [:]
    skipWhitespace()
    if i < end && b[i] == UInt8(ascii: "}") {
      i += 1
      return .object(object)
    }
    while true {
      skipWhitespace()
      guard i < end else { throw error("Unexpected end of JSON input") }
      guard b[i] == UInt8(ascii: "\"") else { throw error("Expected property name") }
      let key = try parseString()
      skipWhitespace()
      guard i < end, b[i] == UInt8(ascii: ":") else {
        throw error("Expected ':' after property name")
      }
      i += 1
      skipWhitespace()
      object[key] = try parseValue(depth: depth + 1)
      skipWhitespace()
      guard i < end else { throw error("Unexpected end of JSON input") }
      if b[i] == UInt8(ascii: ",") {
        i += 1
        continue
      }
      if b[i] == UInt8(ascii: "}") {
        i += 1
        return .object(object)
      }
      throw error("Expected ',' or '}' after property value")
    }
  }

  private mutating func parseArray(depth: Int) throws(Error) -> JSONValue {
    if depth >= Self.maxDepth { throw error("JSON nested too deeply") }
    i += 1  // [
    var array: [JSONValue] = []
    skipWhitespace()
    if i < end && b[i] == UInt8(ascii: "]") {
      i += 1
      return .array(array)
    }
    while true {
      skipWhitespace()
      array.append(try parseValue(depth: depth + 1))
      skipWhitespace()
      guard i < end else { throw error("Unexpected end of JSON input") }
      if b[i] == UInt8(ascii: ",") {
        i += 1
        continue
      }
      if b[i] == UInt8(ascii: "]") {
        i += 1
        return .array(array)
      }
      throw error("Expected ',' or ']' after array element")
    }
  }

  private mutating func parseString() throws(Error) -> String {
    i += 1  // opening quote
    let start = i
    // Fast path: no escapes.
    while i < end {
      let c = b[i]
      if c == UInt8(ascii: "\"") {
        let s = String(decoding: UnsafeBufferPointer(rebasing: b[start..<i]), as: UTF8.self)
        i += 1
        return s
      }
      if c == UInt8(ascii: "\\") { break }
      if c < 0x20 { throw error("Bad control character in string literal") }
      i += 1
    }
    var bytes = Array(UnsafeBufferPointer(rebasing: b[start..<i]))
    while i < end {
      let c = b[i]
      if c == UInt8(ascii: "\"") {
        i += 1
        return String(decoding: bytes, as: UTF8.self)
      }
      if c < 0x20 { throw error("Bad control character in string literal") }
      if c != UInt8(ascii: "\\") {
        bytes.append(c)
        i += 1
        continue
      }
      i += 1
      guard i < end else { throw error("Unterminated string") }
      let e = b[i]
      i += 1
      switch e {
      case UInt8(ascii: "\""): bytes.append(0x22)
      case UInt8(ascii: "\\"): bytes.append(0x5C)
      case UInt8(ascii: "/"): bytes.append(0x2F)
      case UInt8(ascii: "b"): bytes.append(0x08)
      case UInt8(ascii: "f"): bytes.append(0x0C)
      case UInt8(ascii: "n"): bytes.append(0x0A)
      case UInt8(ascii: "r"): bytes.append(0x0D)
      case UInt8(ascii: "t"): bytes.append(0x09)
      case UInt8(ascii: "u"):
        var unit = try parseHex4()
        var scalar: Unicode.Scalar
        if unit >= 0xD800 && unit <= 0xDBFF {
          // A high surrogate; combine with a following escaped low surrogate.
          if end - i >= 6 && b[i] == UInt8(ascii: "\\") && b[i + 1] == UInt8(ascii: "u") {
            let save = i
            i += 2
            let low = try parseHex4()
            if low >= 0xDC00 && low <= 0xDFFF {
              unit = 0x10000 + ((unit - 0xD800) << 10) + (low - 0xDC00)
              scalar = Unicode.Scalar(unit)!
            } else {
              i = save
              scalar = "\u{FFFD}"
            }
          } else {
            scalar = "\u{FFFD}"
          }
        } else if unit >= 0xDC00 && unit <= 0xDFFF {
          scalar = "\u{FFFD}"
        } else {
          scalar = Unicode.Scalar(unit)!
        }
        bytes.append(contentsOf: UTF8.encode(scalar)!)
      default:
        throw error("Bad escaped character in JSON")
      }
    }
    throw error("Unterminated string in JSON")
  }

  private mutating func parseHex4() throws(Error) -> UInt32 {
    guard end - i >= 4 else { throw error("Bad Unicode escape") }
    var v: UInt32 = 0
    for _ in 0..<4 {
      let c = b[i]
      let d: UInt32
      switch c {
      case UInt8(ascii: "0")...UInt8(ascii: "9"): d = UInt32(c - UInt8(ascii: "0"))
      case UInt8(ascii: "a")...UInt8(ascii: "f"): d = UInt32(c - UInt8(ascii: "a") + 10)
      case UInt8(ascii: "A")...UInt8(ascii: "F"): d = UInt32(c - UInt8(ascii: "A") + 10)
      default: throw error("Bad Unicode escape")
      }
      v = v << 4 | d
      i += 1
    }
    return v
  }

  private mutating func parseNumber() throws(Error) -> Double {
    let start = i
    if b[i] == UInt8(ascii: "-") { i += 1 }
    guard i < end else { throw error("No number after minus sign") }
    if b[i] == UInt8(ascii: "0") {
      i += 1
    } else if b[i] >= UInt8(ascii: "1") && b[i] <= UInt8(ascii: "9") {
      while i < end && b[i] >= UInt8(ascii: "0") && b[i] <= UInt8(ascii: "9") { i += 1 }
    } else {
      throw error("No number after minus sign")
    }
    var isInteger = true
    if i < end && b[i] == UInt8(ascii: ".") {
      isInteger = false
      i += 1
      let fracStart = i
      while i < end && b[i] >= UInt8(ascii: "0") && b[i] <= UInt8(ascii: "9") { i += 1 }
      if i == fracStart { throw error("Unterminated fractional number") }
    }
    if i < end && (b[i] == UInt8(ascii: "e") || b[i] == UInt8(ascii: "E")) {
      isInteger = false
      i += 1
      if i < end && (b[i] == UInt8(ascii: "+") || b[i] == UInt8(ascii: "-")) { i += 1 }
      let expStart = i
      while i < end && b[i] >= UInt8(ascii: "0") && b[i] <= UInt8(ascii: "9") { i += 1 }
      if i == expStart { throw error("Exponent part is missing a number") }
    }
    // Small integers are the common case (counters, versions, timestamps).
    if isInteger && i - start <= 15 {
      var v: Int64 = 0
      var k = start
      let negative = b[k] == UInt8(ascii: "-")
      if negative { k += 1 }
      while k < i {
        v = v * 10 + Int64(b[k] - UInt8(ascii: "0"))
        k += 1
      }
      // `-0` must stay negative zero, as JSON.parse("-0") does.
      if negative { return v == 0 ? -0.0 : -Double(v) }
      return Double(v)
    }
    let text = String(decoding: UnsafeBufferPointer(rebasing: b[start..<i]), as: UTF8.self)
    // Double(String) rounds correctly and overflows to infinity, as JS does.
    guard let d = Double(text) else { throw error("Bad number") }
    return d
  }
}
