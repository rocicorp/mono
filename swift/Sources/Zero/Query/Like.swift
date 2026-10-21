/// `LIKE` and `ILIKE` (`packages/zql/src/builder/like.ts`).
///
/// JS compiles a pattern without wildcards to a (lowercased) string
/// comparison, and anything else to a non-Unicode `RegExp` with the `s`
/// flag. That regexp works on UTF-16 code units: `_` matches one code unit
/// (half of a surrogate pair), and `i` folds case per code unit through
/// simple uppercasing. This reproduces both paths.
package struct LikePattern: Sendable {
  package struct InvalidPattern: Error, CustomStringConvertible {
    package let description: String
  }

  enum Token: Sendable, Equatable {
    case any  // %
    case one  // _
    case unit(UInt16)
  }

  enum Matcher: Sendable {
    case exact(String)
    case lowercased(String)
    case wildcard([Token], caseInsensitive: Bool)
  }

  let matcher: Matcher

  package init(_ pattern: String, caseInsensitive: Bool) throws(InvalidPattern) {
    let units = Array(pattern.utf16)
    let hasSpecial = units.contains { $0 == 0x5F || $0 == 0x25 || $0 == 0x5C }
    if !hasSpecial {
      matcher = caseInsensitive ? .lowercased(JSCase.lowercase(pattern)) : .exact(pattern)
      return
    }
    var tokens: [Token] = []
    var i = 0
    while i < units.count {
      let c = units[i]
      switch c {
      case 0x25: tokens.append(.any)
      case 0x5F: tokens.append(.one)
      case 0x5C:
        if i == units.count - 1 {
          throw InvalidPattern(description: "LIKE pattern must not end with escape character")
        }
        i += 1
        tokens.append(.unit(caseInsensitive ? JSCase.canonicalize(units[i]) : units[i]))
      default:
        tokens.append(.unit(caseInsensitive ? JSCase.canonicalize(c) : c))
      }
      i += 1
    }
    matcher = .wildcard(tokens, caseInsensitive: caseInsensitive)
  }

  package func matches(_ input: String) -> Bool {
    switch matcher {
    case .exact(let p):
      // Code unit equality: Swift's `==` would equate canonically
      // equivalent strings, which JS does not.
      return input.utf16.elementsEqual(p.utf16)
    case .lowercased(let p):
      return JSCase.lowercase(input).utf16.elementsEqual(p.utf16)
    case .wildcard(let tokens, let ci):
      var units = Array(input.utf16)
      if ci {
        for k in units.indices { units[k] = JSCase.canonicalize(units[k]) }
      }
      return Self.match(tokens, units)
    }
  }

  /// Full-string wildcard match with backtracking to the last `%`.
  static func match(_ p: [Token], _ s: [UInt16]) -> Bool {
    var pi = 0
    var si = 0
    var starP = -1
    var starS = 0
    while si < s.count {
      if pi < p.count {
        switch p[pi] {
        case .any:
          starP = pi
          starS = si
          pi += 1
          continue
        case .one:
          pi += 1
          si += 1
          continue
        case .unit(let u) where u == s[si]:
          pi += 1
          si += 1
          continue
        default:
          break
        }
      }
      if starP >= 0 {
        pi = starP + 1
        starS += 1
        si = starS
        continue
      }
      return false
    }
    while pi < p.count, p[pi] == .any { pi += 1 }
    return pi == p.count
  }
}

/// JS case mapping.
enum JSCase {
  /// `String.prototype.toLowerCase`: full Unicode lowercase mapping, with
  /// the context-dependent final sigma.
  static func lowercase(_ s: String) -> String {
    let scalars = Array(s.unicodeScalars)
    var out = String.UnicodeScalarView()
    for (i, scalar) in scalars.enumerated() {
      if scalar == "\u{03A3}" {
        out.append(isFinalSigma(scalars, i) ? "\u{03C2}" : "\u{03C3}")
        continue
      }
      out.append(contentsOf: scalar.properties.lowercaseMapping.unicodeScalars)
    }
    return String(out)
  }

  /// Unicode's Final_Sigma condition: preceded by a cased letter (skipping
  /// case-ignorable characters) and not followed by one.
  static func isFinalSigma(_ scalars: [Unicode.Scalar], _ i: Int) -> Bool {
    var before = false
    var j = i - 1
    while j >= 0 {
      let p = scalars[j].properties
      if p.isCaseIgnorable { j -= 1; continue }
      before = p.isCased
      break
    }
    if !before { return false }
    j = i + 1
    while j < scalars.count {
      let p = scalars[j].properties
      if p.isCaseIgnorable { j += 1; continue }
      return !p.isCased
    }
    return true
  }

  /// `Canonicalize` for a non-Unicode, ignore-case `RegExp`: uppercase the
  /// code unit if that yields exactly one code unit, unless it would map a
  /// non-ASCII unit to ASCII.
  static func canonicalize(_ unit: UInt16) -> UInt16 {
    guard let scalar = Unicode.Scalar(unit) else { return unit }  // lone surrogate
    let upper = scalar.properties.uppercaseMapping.utf16
    guard upper.count == 1, let cu = upper.first else { return unit }
    if unit >= 128 && cu < 128 { return unit }
    return cu
  }
}

/// Escapes the wildcards of `value` (`%` and `_`) for a `LIKE` pattern, as
/// in `.where { $0.title.ilike("%\(escapeLike(text))%") }`
/// (`packages/zql/src/query/escape-like.ts`).
public func escapeLike(_ value: String) -> String {
  var out = ""
  for c in value.unicodeScalars {
    if c == "%" || c == "_" { out.unicodeScalars.append("\\") }
    out.unicodeScalars.append(c)
  }
  return out
}
