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

  /// Nesting deeper than this is rejected with "JSON nested too deeply".
  ///
  /// Parsing, writing, comparing, and hashing don't recurse, but freeing a
  /// value does (in `Array`'s and `JSONObject`'s storage), so the limit
  /// keeps freeing within a 512 KB stack (a macOS test or cooperative
  /// thread) in a release build. JS has no limit short of its own stack
  /// (zero-cache writes about 4,100 levels); see JSParity.md.
  package static let maxDepth = 1_500

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
    let value = try p.parseValue()
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

  /// An array or object that a value being parsed is in.
  private enum Container {
    case array([JSONValue])
    /// An object and the key of the value being parsed.
    case object(JSONObject, key: String)
  }

  /// Parses a value without recursion, so nesting takes heap, not stack.
  /// The innermost open array or object is `array` or `object` (with
  /// `key`); the ones it is in are on `parents`. They move between the two
  /// (`take()`), so each stays uniquely referenced and adding to it doesn't
  /// copy it.
  private mutating func parseValue() throws(Error) -> JSONValue {
    var parents: [Container] = []
    var array: [JSONValue]?
    var object: JSONObject?
    var key = ""
    while true {
      guard i < end else { throw error("Unexpected end of JSON input") }
      var value: JSONValue
      switch b[i] {
      case UInt8(ascii: "{"), UInt8(ascii: "["):
        let isObject = b[i] == UInt8(ascii: "{")
        if parents.count + (array == nil && object == nil ? 0 : 1) >= Self.maxDepth {
          throw error("JSON nested too deeply")
        }
        i += 1
        skipWhitespace()
        if i < end && b[i] == (isObject ? UInt8(ascii: "}") : UInt8(ascii: "]")) {
          i += 1
          value = isObject ? .object(JSONObject()) : .array([])
          break
        }
        if let a = array.take() {
          parents.append(.array(a))
        } else if let o = object.take() {
          parents.append(.object(o, key: key))
        }
        if isObject {
          object = JSONObject()
          key = try parseKey()
        } else {
          array = []
        }
        continue
      case UInt8(ascii: "\""): value = .string(try parseString())
      case UInt8(ascii: "t"): try expectLiteral("true"); value = .bool(true)
      case UInt8(ascii: "f"): try expectLiteral("false"); value = .bool(false)
      case UInt8(ascii: "n"): try expectLiteral("null"); value = .null
      case UInt8(ascii: "-"), UInt8(ascii: "0")...UInt8(ascii: "9"):
        value = .number(try parseNumber())
      default: throw error("Unexpected token")
      }
      // Adds the value to its container, closing the containers that end
      // after it, until one has another value.
      while true {
        skipWhitespace()
        if array != nil {
          array!.append(value)
          guard i < end else { throw error("Unexpected end of JSON input") }
          if b[i] == UInt8(ascii: ",") {
            i += 1
            skipWhitespace()
            break
          }
          guard b[i] == UInt8(ascii: "]") else {
            throw error("Expected ',' or ']' after array element")
          }
          i += 1
          value = .array(array.take()!)
        } else if object != nil {
          // A repeated key keeps its first place and its last value.
          object![key] = value
          guard i < end else { throw error("Unexpected end of JSON input") }
          if b[i] == UInt8(ascii: ",") {
            i += 1
            key = try parseKey()
            break
          }
          guard b[i] == UInt8(ascii: "}") else {
            throw error("Expected ',' or '}' after property value")
          }
          i += 1
          value = .object(object.take()!)
        } else {
          return value
        }
        guard let parent = parents.popLast() else { continue }
        switch consume parent {
        case .array(let a):
          array = consume a
        case .object(let o, let k):
          object = consume o
          key = consume k
        }
      }
    }
  }

  /// A property name and its colon, and the whitespace around them.
  private mutating func parseKey() throws(Error) -> String {
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
    return key
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
