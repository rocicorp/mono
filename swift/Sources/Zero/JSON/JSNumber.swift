/// JS number semantics the client has to reproduce exactly: `Number#toString`
/// (also what `JSON.stringify` writes) and `Number(string)`.
package enum JSNumber {
  /// `String(n)` in JS, i.e. ECMAScript `Number::toString(n, 10)`.
  ///
  /// Uses the shortest round-tripping digits (which Swift's `description`
  /// also produces) laid out with the JS rules: plain notation for decimal
  /// exponents in `-6...21`, exponential otherwise.
  package static func format(_ value: Double) -> String {
    if value.isNaN { return "NaN" }
    if value == 0 { return "0" }
    if value.isInfinite { return value < 0 ? "-Infinity" : "Infinity" }
    if value < 0 { return "-" + format(-value) }

    // Integers below 2^53 print exactly; skip the digit extraction.
    if value < 9_007_199_254_740_992, value == value.rounded(.towardZero) {
      return String(Int64(value))
    }

    let (digits, n) = shortestDigits(value)
    let k = digits.count

    if k <= n && n <= 21 {
      return digits + String(repeating: "0", count: n - k)
    }
    if 0 < n && n <= 21 {
      let i = digits.index(digits.startIndex, offsetBy: n)
      return String(digits[..<i]) + "." + String(digits[i...])
    }
    if -6 < n && n <= 0 {
      return "0." + String(repeating: "0", count: -n) + digits
    }
    let e = n - 1
    let exponent = (e >= 0 ? "+" : "-") + String(abs(e))
    if k == 1 {
      return digits + "e" + exponent
    }
    let first = digits.prefix(1)
    return first + "." + digits.dropFirst() + "e" + exponent
  }

  /// The shortest digit string `s` and exponent `n` with
  /// `value == 0.s × 10^n`, for a positive finite `value`.
  static func shortestDigits(_ value: Double) -> (digits: String, n: Int) {
    let text = value.description  // e.g. "123.45", "1e-07", "1.2345678e+20"
    var mantissa = Substring(text)
    var exponent = 0
    if let e = text.firstIndex(where: { $0 == "e" || $0 == "E" }) {
      mantissa = text[..<e]
      exponent = Int(text[text.index(after: e)...])!
    }
    var digits = ""
    var pointPosition: Int? = nil
    for c in mantissa {
      if c == "." {
        pointPosition = digits.count
      } else {
        digits.append(c)
      }
    }
    var n = (pointPosition ?? digits.count) + exponent
    // Strip leading zeros ("0.001" -> "1", moving the point).
    while digits.first == "0" && digits.count > 1 {
      digits.removeFirst()
      n -= 1
    }
    while digits.last == "0" && digits.count > 1 {
      digits.removeLast()
    }
    return (digits, n)
  }

  /// `Number(string)` in JS (ECMAScript `StringToNumber`): trims JS
  /// whitespace, maps the empty string to 0, and accepts decimal, `Infinity`,
  /// and `0x`/`0o`/`0b` literals. Anything else is `NaN`.
  package static func parse(_ string: String) -> Double {
    let scalars = Array(string.unicodeScalars)
    var start = 0
    var end = scalars.count
    while start < end && isJSWhitespace(scalars[start]) { start += 1 }
    while end > start && isJSWhitespace(scalars[end - 1]) { end -= 1 }
    if start == end { return 0 }
    let s = String(String.UnicodeScalarView(scalars[start..<end]))

    switch s {
    case "Infinity", "+Infinity": return .infinity
    case "-Infinity": return -.infinity
    default: break
    }

    let lower = s.lowercased()
    for (prefix, radix) in [("0x", 16), ("0o", 8), ("0b", 2)] where lower.hasPrefix(prefix) {
      let body = s.dropFirst(2)
      if body.isEmpty { return .nan }
      var result = 0.0
      for c in body {
        guard let d = c.hexDigitValue, d < radix else { return .nan }
        result = result * Double(radix) + Double(d)
      }
      return result
    }

    guard isDecimalLiteral(s) else { return .nan }
    return Double(s) ?? .nan
  }

  private static func isDecimalLiteral(_ s: String) -> Bool {
    var bytes = Substring(s).utf8[...]
    if let f = bytes.first, f == UInt8(ascii: "+") || f == UInt8(ascii: "-") {
      bytes = bytes.dropFirst()
    }
    var intDigits = 0
    while let f = bytes.first, f >= 0x30 && f <= 0x39 {
      intDigits += 1
      bytes = bytes.dropFirst()
    }
    var fracDigits = 0
    if bytes.first == UInt8(ascii: ".") {
      bytes = bytes.dropFirst()
      while let f = bytes.first, f >= 0x30 && f <= 0x39 {
        fracDigits += 1
        bytes = bytes.dropFirst()
      }
    }
    if intDigits == 0 && fracDigits == 0 { return false }
    if let f = bytes.first, f == UInt8(ascii: "e") || f == UInt8(ascii: "E") {
      bytes = bytes.dropFirst()
      if let f = bytes.first, f == UInt8(ascii: "+") || f == UInt8(ascii: "-") {
        bytes = bytes.dropFirst()
      }
      var expDigits = 0
      while let f = bytes.first, f >= 0x30 && f <= 0x39 {
        expDigits += 1
        bytes = bytes.dropFirst()
      }
      if expDigits == 0 { return false }
    }
    return bytes.isEmpty
  }

  /// ECMAScript `WhiteSpace` plus `LineTerminator`.
  static func isJSWhitespace(_ s: Unicode.Scalar) -> Bool {
    switch s.value {
    case 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, 0xA0, 0x1680, 0x2000...0x200A,
      0x2028, 0x2029, 0x202F, 0x205F, 0x3000, 0xFEFF:
      true
    default:
      false
    }
  }

  /// `(n | 0) === n` in JS: whether `n` is exactly an int32. `-0` counts,
  /// because `-0 === 0`.
  @inlinable
  package static func isInt32(_ n: Double) -> Bool {
    n >= -2_147_483_648 && n <= 2_147_483_647 && n == n.rounded(.towardZero)
  }

  /// `ToInt32(n)` in JS, for an `n` that `isInt32` accepted.
  @inlinable
  package static func int32Bits(_ n: Double) -> UInt32 {
    UInt32(bitPattern: Int32(n))
  }
}
