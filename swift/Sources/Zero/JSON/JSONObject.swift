/// A JSON object: string keys to `JSONValue`s, with keys kept the way a JS
/// object keeps its properties.
///
/// - Keys are compared by UTF-16 code unit, as JS compares property names,
///   not by Swift's canonical equivalence: `"é"` and `"e\u{301}"` are two
///   keys.
/// - Keys are in JS property order: integer-like keys (array indices, such
///   as `"0"` and `"10"`, but not `"01"`) first in ascending numeric order,
///   then the others in the order they were first set. This is the order
///   `Object.keys` and `JSON.stringify` use.
///
/// Setting a key that is already there replaces its value and keeps its
/// place, as assigning to an existing property does in JS.
///
/// Two objects are equal when they have the same keys with equal values,
/// whatever their order.
///
/// ```swift
/// var o: JSONObject = ["b": 1, "a": 2]
/// o["c"] = true
/// for (key, value) in o { print(key, value) }  // b 1, a 2, c true
/// ```
// Unchecked: an object only writes storage it alone owns.
public struct JSONObject: @unchecked Sendable {
  /// The entries, in order, in one buffer that copies share until one of
  /// them changes. One reference, so a `JSONValue` stays as small as it was
  /// with a dictionary.
  private var storage: JSONObjectStorage

  /// Objects with more keys than this keep an index; smaller ones are
  /// faster to search.
  static let indexThreshold = 16

  /// An empty object.
  public init() {
    storage = JSONObjectStorage.empty
  }

  /// An empty object with room for `minimumCapacity` keys.
  public init(minimumCapacity: Int) {
    storage = minimumCapacity > 0 ? .make(capacity: minimumCapacity) : .empty
  }

  /// The object of `entries`, in their order. A key that repeats keeps its
  /// first place and its last value, as in a JS object literal.
  public init<S: Sequence>(_ entries: S) where S.Element == (key: String, value: JSONValue) {
    self.init(minimumCapacity: entries.underestimatedCount)
    for (key, value) in entries { self[key] = value }
  }

  /// The object of `dictionary`'s entries, with keys in UTF-16 code unit
  /// order (a dictionary has no order of its own).
  ///
  /// A `[String: JSONValue]` holds one of each set of canonically equivalent
  /// keys; build the object directly to keep them all.
  public init(_ dictionary: [String: JSONValue]) {
    self.init(minimumCapacity: dictionary.count)
    for (key, value) in dictionary.sorted(by: { StringCompare.utf16Less($0.key, $1.key) }) {
      self[key] = value
    }
  }

  /// The object of `record`'s entries, with keys in UTF-16 code unit order.
  package init<T>(_ record: [JSKey: T], _ transform: (T) -> JSONValue) {
    self.init(minimumCapacity: record.count)
    for (key, value) in record.sorted(by: { StringCompare.utf16Less($0.key.string, $1.key.string) })
    {
      self[key.string] = transform(value)
    }
  }

  /// How many keys the object has.
  public var count: Int { storage.header.count }

  public var isEmpty: Bool { count == 0 }

  /// The keys, in order.
  public var keys: [String] { map(\.key) }

  /// The values, in the order of `keys`.
  public var values: [JSONValue] { map(\.value) }

  /// The position of `key`, or nil when the object doesn't have it.
  public func index(forKey key: String) -> Int? {
    storage.index(of: key)
  }

  /// The value of `key`; setting nil removes it.
  public subscript(key: String) -> JSONValue? {
    get {
      guard let i = index(forKey: key) else { return nil }
      return storage.value(at: i)
    }
    set {
      if let newValue {
        updateValue(newValue, forKey: key)
      } else {
        removeValue(forKey: key)
      }
    }
  }

  /// The value of `key`, or `defaultValue`.
  public subscript(key: String, default defaultValue: @autoclosure () -> JSONValue) -> JSONValue {
    get { self[key] ?? defaultValue() }
    set { self[key] = newValue }
  }

  /// Sets `key` to `value`, returning the value it replaced. A new key goes
  /// after the others, or among the integer-like keys in numeric order.
  @discardableResult
  public mutating func updateValue(_ value: JSONValue, forKey key: String) -> JSONValue? {
    if let i = index(forKey: key) {
      makeUnique(capacity: count)
      return storage.replaceValue(at: i, with: value)
    }
    makeUnique(capacity: count + 1)
    if let n = Self.arrayIndex(key) {
      var at = 0
      while at < storage.header.integerKeyCount, Self.arrayIndex(storage.key(at: at))! < n {
        at += 1
      }
      storage.insert(key, value, at: at, isInteger: true)
    } else {
      storage.insert(key, value, at: count, isInteger: false)
    }
    return nil
  }

  /// Removes `key`, returning its value. The other keys keep their order.
  @discardableResult
  public mutating func removeValue(forKey key: String) -> JSONValue? {
    guard let i = index(forKey: key) else { return nil }
    makeUnique(capacity: count)
    return storage.remove(at: i)
  }

  /// Removes every key.
  public mutating func removeAll() {
    self = JSONObject()
  }

  /// The object with each value transformed, keys in the same order.
  public func mapValues(_ transform: (JSONValue) throws -> JSONValue) rethrows -> JSONObject {
    var o = JSONObject(minimumCapacity: count)
    for (key, value) in self { o.append(key, try transform(value)) }
    return o
  }

  /// The object with each value transformed, without the keys whose value
  /// becomes nil.
  public func compactMapValues(
    _ transform: (JSONValue) throws -> JSONValue?
  ) rethrows
    -> JSONObject
  {
    var o = JSONObject(minimumCapacity: count)
    for (key, value) in self {
      if let v = try transform(value) { o.append(key, v) }
    }
    return o
  }

  /// The entries for which `isIncluded` is true, in order.
  public func filter(
    _ isIncluded: ((key: String, value: JSONValue)) throws -> Bool
  ) rethrows
    -> JSONObject
  {
    var o = JSONObject(minimumCapacity: count)
    for entry in self where try isIncluded(entry) { o.append(entry.key, entry.value) }
    return o
  }

  /// Sets each of `other`'s keys, in its order, resolving a key both have
  /// with `combine(old, new)`.
  public mutating func merge(
    _ other: JSONObject, uniquingKeysWith combine: (JSONValue, JSONValue) throws -> JSONValue
  ) rethrows {
    for (key, value) in other {
      if let old = self[key] {
        self[key] = try combine(old, value)
      } else {
        self[key] = value
      }
    }
  }

  /// This object with `other`'s keys set, as ``merge(_:uniquingKeysWith:)``.
  public func merging(
    _ other: JSONObject, uniquingKeysWith combine: (JSONValue, JSONValue) throws -> JSONValue
  ) rethrows -> JSONObject {
    var o = self
    try o.merge(other, uniquingKeysWith: combine)
    return o
  }

  /// The object as a dictionary. Of keys that are canonically equivalent
  /// (`"é"` and `"e\u{301}"`), which a `Dictionary` can't tell apart, the
  /// last one wins.
  public var dictionary: [String: JSONValue] {
    var out: [String: JSONValue] = [:]
    out.reserveCapacity(count)
    for (key, value) in self { out[key] = value }
    return out
  }

  /// Appends a key the object doesn't have, from an object in order, so
  /// after any integer-like keys it has.
  private mutating func append(_ key: String, _ value: JSONValue) {
    makeUnique(capacity: count + 1)
    storage.insert(key, value, at: count, isInteger: Self.arrayIndex(key) != nil)
  }

  /// Makes the storage this object's alone, with room for `capacity` keys.
  private mutating func makeUnique(capacity: Int) {
    let unique = isKnownUniquelyReferenced(&storage)
    if unique && storage.header.capacity >= capacity { return }
    let grown = Swift.max(capacity, unique ? storage.header.capacity * 2 : capacity, 2)
    storage = storage.copy(capacity: grown, moving: unique)
  }

  /// The value of `key` if it is an array index (a canonical integer
  /// below 2^32 - 1), which JS orders before other property keys.
  static func arrayIndex(_ key: String) -> UInt32? {
    var utf8 = key.utf8.makeIterator()
    guard let first = utf8.next(), first >= UInt8(ascii: "0"), first <= UInt8(ascii: "9") else {
      return nil
    }
    var n = UInt64(first - UInt8(ascii: "0"))
    var digits = 1
    while let c = utf8.next() {
      guard c >= UInt8(ascii: "0"), c <= UInt8(ascii: "9"), n != 0 else { return nil }
      digits += 1
      if digits > 10 { return nil }
      n = n * 10 + UInt64(c - UInt8(ascii: "0"))
    }
    guard n < 0xFFFF_FFFF else { return nil }
    return UInt32(n)
  }
}

struct JSONObjectHeader {
  var count: Int
  var capacity: Int
  /// How many of the keys, at the front, are integer-like.
  var integerKeyCount: Int
  /// Where each key is, for objects with more than `indexThreshold` keys.
  /// Smaller ones are searched.
  var index: [JSKey: Int]?
}

struct JSONObjectEntry {
  var key: String
  var value: JSONValue
}

/// A `JSONObject`'s entries, in order, after a header.
final class JSONObjectStorage: ManagedBuffer<JSONObjectHeader, JSONObjectEntry> {
  // Never written: an object copies it before changing it, as it isn't uniquely referenced.
  nonisolated(unsafe) static let empty = make(capacity: 0)

  static func make(capacity: Int) -> JSONObjectStorage {
    unsafeDowncast(
      create(minimumCapacity: capacity) { _ in
        JSONObjectHeader(count: 0, capacity: capacity, integerKeyCount: 0, index: nil)
      },
      to: JSONObjectStorage.self)
  }

  /// A copy with room for `capacity` entries, moving the entries out of
  /// this one when `moving`.
  func copy(capacity: Int, moving: Bool) -> JSONObjectStorage {
    let new = Self.make(capacity: capacity)
    withUnsafeMutablePointers { header, entries in
      new.withUnsafeMutablePointers { newHeader, newEntries in
        let count = header.pointee.count
        if moving {
          newEntries.moveInitialize(from: entries, count: count)
          header.pointee.count = 0
        } else {
          newEntries.initialize(from: entries, count: count)
        }
        newHeader.pointee.count = count
        newHeader.pointee.integerKeyCount = header.pointee.integerKeyCount
        newHeader.pointee.index = header.pointee.index
      }
    }
    return new
  }

  func index(of key: String) -> Int? {
    withUnsafeMutablePointers { header, entries in
      if let index = header.pointee.index { return index[JSKey(key)] }
      for i in 0..<header.pointee.count where StringCompare.equal(entries[i].key, key) {
        return i
      }
      return nil
    }
  }

  func key(at i: Int) -> String {
    withUnsafeMutablePointerToElements { $0[i].key }
  }

  func value(at i: Int) -> JSONValue {
    withUnsafeMutablePointerToElements { $0[i].value }
  }

  func entry(at i: Int) -> (key: String, value: JSONValue) {
    withUnsafeMutablePointerToElements { ($0[i].key, $0[i].value) }
  }

  func replaceValue(at i: Int, with value: JSONValue) -> JSONValue {
    withUnsafeMutablePointerToElements { entries in
      let old = entries[i].value
      entries[i].value = value
      return old
    }
  }

  /// Inserts an entry; there must be room.
  func insert(_ key: String, _ value: JSONValue, at i: Int, isInteger: Bool) {
    withUnsafeMutablePointers { header, entries in
      let count = header.pointee.count
      if i < count {
        (entries + i + 1).moveInitialize(from: entries + i, count: count - i)
      }
      (entries + i).initialize(to: JSONObjectEntry(key: key, value: value))
      header.pointee.count = count + 1
      if isInteger { header.pointee.integerKeyCount += 1 }
      if i < count {
        reindex(header, entries)
      } else if header.pointee.index != nil {
        header.pointee.index![JSKey(key)] = i
      } else if count + 1 > JSONObject.indexThreshold {
        reindex(header, entries)
      }
    }
  }

  func remove(at i: Int) -> JSONValue {
    withUnsafeMutablePointers { header, entries in
      let count = header.pointee.count
      let removed = (entries + i).move()
      (entries + i).moveInitialize(from: entries + i + 1, count: count - i - 1)
      header.pointee.count = count - 1
      if i < header.pointee.integerKeyCount { header.pointee.integerKeyCount -= 1 }
      if header.pointee.index != nil {
        if i == count - 1 {
          header.pointee.index![JSKey(removed.key)] = nil
        } else {
          reindex(header, entries)
        }
      }
      return removed.value
    }
  }

  private func reindex(
    _ header: UnsafeMutablePointer<JSONObjectHeader>,
    _ entries: UnsafeMutablePointer<JSONObjectEntry>
  ) {
    let count = header.pointee.count
    guard count > JSONObject.indexThreshold else {
      header.pointee.index = nil
      return
    }
    var index: [JSKey: Int] = [:]
    index.reserveCapacity(count)
    for i in 0..<count { index[JSKey(entries[i].key)] = i }
    header.pointee.index = index
  }

  deinit {
    withUnsafeMutablePointers { header, entries in
      _ = entries.deinitialize(count: header.pointee.count)
    }
  }
}

extension JSONObject: RandomAccessCollection {
  public typealias Element = (key: String, value: JSONValue)

  public var startIndex: Int { 0 }
  public var endIndex: Int { count }

  public subscript(position: Int) -> Element {
    precondition(position >= 0 && position < count, "Index out of range")
    return storage.entry(at: position)
  }
}

extension JSONObject: ExpressibleByDictionaryLiteral {
  /// The object of `elements`, in their order. A key that repeats keeps its
  /// first place and its last value, as in a JS object literal.
  public init(dictionaryLiteral elements: (String, JSONValue)...) {
    self.init(minimumCapacity: elements.count)
    for (key, value) in elements { self[key] = value }
  }
}

extension JSONObject: Hashable {
  /// The same keys, by code unit, with equal values, in any order.
  public static func == (a: JSONObject, b: JSONObject) -> Bool {
    if a.storage === b.storage { return true }
    guard a.count == b.count else { return false }
    for i in 0..<a.count {
      let (key, value) = a[i]
      // Usually the keys are in the same order.
      if StringCompare.equal(key, b.storage.key(at: i)) {
        if value != b.storage.value(at: i) { return false }
      } else {
        guard let j = b.index(forKey: key), value == b.storage.value(at: j) else {
          return false
        }
      }
    }
    return true
  }

  /// Independent of the order, as `==` is.
  public func hash(into hasher: inout Hasher) {
    var acc = 0
    for (key, value) in self {
      var h = Hasher()
      h.combine(JSKey(key))
      h.combine(value)
      acc ^= h.finalize()
    }
    hasher.combine(count)
    hasher.combine(acc)
  }
}

extension JSONObject: CustomStringConvertible, CustomDebugStringConvertible {
  /// The object as JSON text.
  public var description: String { JSONSerializer.stringify(.object(self)) }

  public var debugDescription: String { description }
}

extension JSONObject: Codable {
  struct CodingKeys: CodingKey {
    var stringValue: String
    var intValue: Int? { nil }
    init(stringValue: String) { self.stringValue = stringValue }
    init?(intValue: Int) { nil }
  }

  /// Decodes an object. A decoder gives its keys in no particular order, so
  /// they are put in UTF-16 code unit order.
  public init(from decoder: any Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    self.init(minimumCapacity: container.allKeys.count)
    for key in container.allKeys.sorted(by: {
      StringCompare.utf16Less($0.stringValue, $1.stringValue)
    }) {
      self[key.stringValue] = try container.decode(JSONValue.self, forKey: key)
    }
  }

  public func encode(to encoder: any Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)
    for (key, value) in self { try container.encode(value, forKey: CodingKeys(stringValue: key)) }
  }
}
