/// Encodes `JSONValue`s as `JSON.stringify` does: the same string escaping and
/// number formatting, with non-finite numbers written as `null`.
///
/// Object keys are written in the object's order, which is JS property order,
/// so the output is what `JSON.stringify` writes for the same object.
package enum JSONSerializer {
  package static func stringify(_ value: JSONValue) -> String {
    var out = ""
    write(value, into: &out)
    return out
  }

  package static func data(_ value: JSONValue) -> [UInt8] {
    Array(stringify(value).utf8)
  }

  /// An array or object being written, and the index of its next value.
  private enum Container {
    case array([JSONValue], next: Int)
    case object(JSONObject, next: Int)
  }

  /// Writes `value` without recursion: the arrays and objects it is in are
  /// on `containers`, so nesting takes heap, not stack.
  package static func write(_ value: JSONValue, into out: inout String) {
    if writeScalar(value, into: &out) { return }
    var containers: [Container] = []
    var value = value
    nested: while true {
      switch value {
      case .array(let a):
        out += "["
        containers.append(.array(a, next: 0))
      case .object(let o):
        out += "{"
        containers.append(.object(o, next: 0))
      default:
        preconditionFailure("not an array or object")
      }
      // Writes values until one is an array or object, closing the
      // containers that are done.
      while let container = containers.popLast() {
        switch container {
        case .array(let a, var i):
          while i < a.count {
            if i > 0 { out += "," }
            let e = a[i]
            i += 1
            if writeScalar(e, into: &out) { continue }
            containers.append(.array(a, next: i))
            value = e
            continue nested
          }
          out += "]"
        case .object(let o, var i):
          while i < o.count {
            if i > 0 { out += "," }
            let (key, v) = o[i]
            i += 1
            writeString(key, into: &out)
            out += ":"
            if writeScalar(v, into: &out) { continue }
            containers.append(.object(o, next: i))
            value = v
            continue nested
          }
          out += "}"
        }
      }
      return
    }
  }

  /// Writes `value` unless it is an array or object; returns whether it did.
  private static func writeScalar(_ value: JSONValue, into out: inout String) -> Bool {
    switch value {
    case .null: out += "null"
    case .bool(let b): out += b ? "true" : "false"
    case .number(let n): out += n.isFinite ? JSNumber.format(n) : "null"
    case .string(let s): writeString(s, into: &out)
    case .array, .object: return false
    }
    return true
  }

  static let hex: [Character] = Array("0123456789abcdef")

  package static func writeString(_ s: String, into out: inout String) {
    out += "\""
    var needsEscape = false
    for u in s.utf8 where u < 0x20 || u == 0x22 || u == 0x5C {
      needsEscape = true
      break
    }
    if !needsEscape {
      out += s
      out += "\""
      return
    }
    for scalar in s.unicodeScalars {
      switch scalar.value {
      case 0x22: out += "\\\""
      case 0x5C: out += "\\\\"
      case 0x08: out += "\\b"
      case 0x0C: out += "\\f"
      case 0x0A: out += "\\n"
      case 0x0D: out += "\\r"
      case 0x09: out += "\\t"
      case 0..<0x20:
        out += "\\u00"
        out.append(hex[Int(scalar.value >> 4)])
        out.append(hex[Int(scalar.value & 0xF)])
      default:
        out.unicodeScalars.append(scalar)
      }
    }
    out += "\""
  }
}
