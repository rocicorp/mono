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

  package static func write(_ value: JSONValue, into out: inout String) {
    switch value {
    case .null:
      out += "null"
    case .bool(let b):
      out += b ? "true" : "false"
    case .number(let n):
      out += n.isFinite ? JSNumber.format(n) : "null"
    case .string(let s):
      writeString(s, into: &out)
    case .array(let a):
      out += "["
      for (i, e) in a.enumerated() {
        if i > 0 { out += "," }
        write(e, into: &out)
      }
      out += "]"
    case .object(let o):
      out += "{"
      var first = true
      for (key, value) in o {
        if !first { out += "," }
        first = false
        writeString(key, into: &out)
        out += ":"
        write(value, into: &out)
      }
      out += "}"
    }
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
