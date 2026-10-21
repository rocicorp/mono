/// The two string orders Zero uses.
///
/// - UTF-8 byte order (code point order) is what `compare-utf8` and SQLite
///   use. Row values, primary keys, and AST normalization sort this way.
/// - UTF-16 code unit order is what JS `<` and `Array#sort` use. Object keys
///   are sorted this way before hashing.
///
/// Both differ from Swift's `String <`, which compares by Unicode canonical
/// equivalence.
package enum StringCompare {
  /// `compareUTF8(a, b)`: negative, zero, or positive.
  @inlinable
  package static func utf8(_ a: String, _ b: String) -> Int {
    // No `a == b` shortcut: Swift's `==` equates canonically equivalent
    // strings ("é" and "e\u{301}"), which are distinct in Zero.
    let contiguous = a.utf8.withContiguousStorageIfAvailable { a in
      b.utf8.withContiguousStorageIfAvailable { b in compare(a, b) }
    }
    if let result = contiguous ?? nil { return result }
    var ai = a.utf8.makeIterator()
    var bi = b.utf8.makeIterator()
    while true {
      switch (ai.next(), bi.next()) {
      case (nil, nil): return 0
      case (nil, _): return -1
      case (_, nil): return 1
      case let (x?, y?):
        if x != y { return x < y ? -1 : 1 }
      }
    }
  }

  @inlinable
  package static func utf8Less(_ a: String, _ b: String) -> Bool { utf8(a, b) < 0 }

  /// Byte order: negative, zero, or positive.
  @inlinable
  static func compare(_ a: UnsafeBufferPointer<UInt8>, _ b: UnsafeBufferPointer<UInt8>) -> Int {
    let n = Swift.min(a.count, b.count)
    var i = 0
    while i < n {
      let (x, y) = (a[i], b[i])
      if x != y { return x < y ? -1 : 1 }
      i += 1
    }
    return a.count == b.count ? 0 : a.count < b.count ? -1 : 1
  }

  /// JS `a < b ? -1 : a > b ? 1 : 0` for strings.
  @inlinable
  package static func utf16(_ a: String, _ b: String) -> Int {
    var ai = a.utf16.makeIterator()
    var bi = b.utf16.makeIterator()
    while true {
      switch (ai.next(), bi.next()) {
      case (nil, nil): return 0
      case (nil, _): return -1
      case (_, nil): return 1
      case let (x?, y?):
        if x != y { return x < y ? -1 : 1 }
      }
    }
  }

  @inlinable
  package static func utf16Less(_ a: String, _ b: String) -> Bool { utf16(a, b) < 0 }

  /// Code unit equality, as JS `===` compares strings.
  @inlinable
  package static func equal(_ a: String, _ b: String) -> Bool {
    let contiguous = a.utf8.withContiguousStorageIfAvailable { a in
      b.utf8.withContiguousStorageIfAvailable { b in
        a.count == b.count && compare(a, b) == 0
      }
    }
    if let result = contiguous ?? nil { return result }
    return a.utf8.elementsEqual(b.utf8)
  }
}

/// A string key compared by code units, like a JS `Map` key. Swift's own
/// `String` equality treats canonically equivalent strings as equal.
package struct JSKey: Hashable, Sendable, CustomStringConvertible {
  package let string: String

  @inlinable
  package init(_ string: String) { self.string = string }

  @inlinable
  package static func == (a: JSKey, b: JSKey) -> Bool { StringCompare.equal(a.string, b.string) }

  @inlinable
  package func hash(into hasher: inout Hasher) {
    var s = string
    s.withUTF8 { hasher.combine(bytes: UnsafeRawBufferPointer($0)) }
  }

  package var description: String { string }
}
