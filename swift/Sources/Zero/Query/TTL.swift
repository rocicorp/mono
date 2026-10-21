/// Query time-to-live (`packages/zql/src/query/ttl.ts`).
///
/// `forever` and negative durations never expire, `none` expires
/// immediately. Durations are clamped to 10 minutes when sent to the server.
public enum TTL: Sendable, Hashable {
  case none
  case forever
  case milliseconds(Double)
  /// A duration string as JS accepts it: a number followed by `s`, `m`, `h`,
  /// `d`, or `y` (for example `"5m"`).
  case string(String)

  public static func seconds(_ n: Double) -> TTL { .string(JSNumber.format(n) + "s") }
  public static func minutes(_ n: Double) -> TTL { .string(JSNumber.format(n) + "m") }
  public static func hours(_ n: Double) -> TTL { .string(JSNumber.format(n) + "h") }
  public static func days(_ n: Double) -> TTL { .string(JSNumber.format(n) + "d") }

  public static let defaultTTL: TTL = .string("5m")
  public static let defaultTTLMilliseconds: Double = 5 * 60 * 1000
  public static let defaultPreloadTTL: TTL = .none
  public static let maxTTL: TTL = .string("10m")
  public static let maxTTLMilliseconds: Double = 10 * 60 * 1000

  static let multipliers: [Character: Double] = [
    "s": 1000,
    "m": 60 * 1000,
    "h": 60 * 60 * 1000,
    "d": 24 * 60 * 60 * 1000,
    "y": 365 * 24 * 60 * 60 * 1000,
  ]

  /// `parseTTL`: milliseconds, with `-1` for never expiring.
  public var parsed: Double {
    switch self {
    case .none: return 0
    case .forever: return -1
    case .milliseconds(let n):
      if n.isNaN { return 0 }
      return !n.isFinite || n < 0 ? -1 : n
    case .string(let s):
      if s == "none" { return 0 }
      if s == "forever" { return -1 }
      guard let unit = s.last, let multiplier = Self.multipliers[unit] else { return .nan }
      return JSNumber.parse(String(s.dropLast())) * multiplier
    }
  }

  /// `compareTTL`: never-expiring sorts last.
  public static func compare(_ a: TTL, _ b: TTL) -> Double {
    let ap = a.parsed
    let bp = b.parsed
    if ap == -1 && bp != -1 { return 1 }
    if ap != -1 && bp == -1 { return -1 }
    return ap - bp
  }

  /// `normalizeTTL`: the shortest equivalent spelling of a number of
  /// milliseconds; strings are returned as is.
  public var normalized: TTL {
    guard case .milliseconds(let ttl) = self else { return self }
    if ttl < 0 { return .forever }
    if ttl == 0 { return .none }
    var shortest = JSNumber.format(ttl)
    let lengthOfNumber = shortest.count
    for unit: Character in ["y", "d", "h", "m", "s"] {
      let candidate = JSNumber.format(ttl / Self.multipliers[unit]!) + String(unit)
      if candidate.count < shortest.count {
        shortest = candidate
      }
    }
    return shortest.count < lengthOfNumber ? .string(shortest) : self
  }

  /// `clampTTL`: milliseconds, at most 10 minutes; never-expiring becomes 10
  /// minutes.
  public var clamped: Double {
    let p = parsed
    if p == -1 || p > Self.maxTTLMilliseconds {
      return Self.maxTTLMilliseconds
    }
    return p
  }
}

extension TTL: ExpressibleByStringLiteral {
  public init(stringLiteral value: String) {
    switch value {
    case "none": self = .none
    case "forever": self = .forever
    default: self = .string(value)
    }
  }
}

extension TTL: ExpressibleByIntegerLiteral {
  public init(integerLiteral value: Int) { self = .milliseconds(Double(value)) }
}

extension TTL {
  /// The JSON form JS would hold: a string or a number.
  package var json: JSONValue {
    switch self {
    case .none: "none"
    case .forever: "forever"
    case .milliseconds(let n): .number(n)
    case .string(let s): .string(s)
    }
  }

  package init(json: JSONValue) throws(DecodingError) {
    switch json {
    case .number(let n): self = .milliseconds(n)
    case .string(let s): self = TTL(stringLiteral: s)
    default: throw DecodingError("Expected TTL string or number")
    }
  }
}
