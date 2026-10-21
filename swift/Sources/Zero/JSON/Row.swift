/// A row: column name to value. A missing column reads as `nil`, as an
/// `undefined` property does in JS.
///
/// It reads and writes like a `[String: JSONValue]`. Inside, the values sit
/// in one buffer, in the order of a layout the rows of a table share, so code
/// that knows the layout reads a column by index instead of hashing its
/// name. Copies share the buffer until one of them changes.
// Unchecked: a row only writes storage it alone owns.
public struct Row: @unchecked Sendable {
  @usableFromInline var storage: RowStorage

  @inlinable
  init(storage: RowStorage) {
    self.storage = storage
  }

  /// A row of `slots`, a value or nil for each of `layout`'s columns.
  package init(layout: RowLayout, slots: [JSONValue?]) {
    precondition(slots.count == layout.names.count)
    storage = .make(layout) { values in
      slots.withUnsafeBufferPointer {
        values.initialize(from: $0.baseAddress!, count: $0.count)
      }
    }
  }

  /// An empty row.
  public init() {
    self.init(layout: .empty, slots: [])
  }

  /// The row of `dictionary`'s columns.
  public init(_ dictionary: [String: JSONValue]) {
    let names = dictionary.keys.sorted(by: StringCompare.utf16Less)
    self.init(layout: RowLayout(names), slots: names.map { dictionary[$0] })
  }

  /// The row of `dictionary`'s columns in `layout`, with any others after
  /// them.
  package init(_ dictionary: [String: JSONValue], layout: RowLayout) {
    var others: [String] = []
    let storage = RowStorage.make(layout) { values in
      values.initialize(repeating: nil, count: layout.names.count)
      for (name, value) in dictionary {
        if let i = layout.index(of: name) {
          values[i] = value
        } else {
          others.append(name)
        }
      }
    }
    if others.isEmpty {
      self.init(storage: storage)
    } else {
      others.sort(by: StringCompare.utf16Less)
      let row = Row(storage: storage)
      self.init(
        layout: RowLayout(layout.names + others),
        slots: (0..<row.slotCount).map { row[slot: $0] } + others.map { dictionary[$0] })
    }
  }

  /// This row in `layout`, with any other columns after its own. The row
  /// itself when it's in `layout` already.
  package func conformed(to layout: RowLayout) -> Row {
    if self.layout === layout { return self }
    var others: [(String, JSONValue)] = []
    let storage = RowStorage.make(layout) { values in
      values.initialize(repeating: nil, count: layout.names.count)
      for (name, value) in self {
        if let i = layout.index(of: name) {
          values[i] = value
        } else {
          others.append((name, value))
        }
      }
    }
    let row = Row(storage: storage)
    if others.isEmpty { return row }
    return Row(
      layout: RowLayout(layout.names + others.map(\.0)),
      slots: (0..<row.slotCount).map { row[slot: $0] } + others.map(\.1))
  }

  // `layout` and `slotCount` read the header through its pointer: reading
  // `header`, a stored class property, checks exclusivity each time.

  /// The layout of the row's columns.
  @inlinable
  package var layout: RowLayout { storage.withUnsafeMutablePointerToHeader { $0.pointee.layout } }

  /// How many columns the layout has.
  @inlinable
  package var slotCount: Int { storage.withUnsafeMutablePointerToHeader { $0.pointee.count } }

  /// The value of the layout's column `i`.
  @inlinable
  package subscript(slot i: Int) -> JSONValue? {
    get {
      precondition(i >= 0 && i < slotCount)
      return storage.withUnsafeMutablePointerToElements { $0[i] }
    }
    set {
      precondition(i >= 0 && i < slotCount)
      if !isKnownUniquelyReferenced(&storage) { storage = storage.copy() }
      storage.withUnsafeMutablePointerToElements { $0[i] = newValue }
    }
  }

  /// The value of the column `name`.
  @inlinable
  public subscript(name: String) -> JSONValue? {
    get {
      guard let i = layout.index(of: name) else { return nil }
      return self[slot: i]
    }
    set {
      if let i = layout.index(of: name) {
        self[slot: i] = newValue
      } else if let newValue {
        // A new layout: others' rows keep theirs.
        storage = storage.appending(name, newValue)
      }
    }
  }

  /// The value of the column `name`, or `defaultValue`.
  @inlinable
  public subscript(name: String, default defaultValue: @autoclosure () -> JSONValue) -> JSONValue {
    self[name] ?? defaultValue()
  }

  /// The value of `column`: by index when this row is in its layout.
  @inlinable
  package subscript(column column: ColumnRef) -> JSONValue? {
    if layout === column.layout {
      guard let i = column.index else { return nil }
      return self[slot: i]
    }
    return self[column.name]
  }

  /// Removes the column `name`, returning its value.
  @discardableResult
  public mutating func removeValue(forKey name: String) -> JSONValue? {
    guard let i = layout.index(of: name) else { return nil }
    defer { self[slot: i] = nil }
    return self[slot: i]
  }

  /// How many columns have a value.
  public var count: Int {
    var n = 0
    for i in 0..<slotCount where self[slot: i] != nil { n += 1 }
    return n
  }

  public var isEmpty: Bool { count == 0 }

  /// The names of the columns with a value, in the layout's order.
  public var keys: [String] { map(\.key) }

  /// The row as a dictionary.
  public var dictionary: [String: JSONValue] {
    var out: [String: JSONValue] = [:]
    out.reserveCapacity(slotCount)
    for (name, value) in self { out[name] = value }
    return out
  }
}

extension Row: Sequence {
  public struct Iterator: IteratorProtocol {
    @usableFromInline let row: Row
    @usableFromInline var i = 0

    @inlinable
    init(_ row: Row) { self.row = row }

    @inlinable
    public mutating func next() -> (key: String, value: JSONValue)? {
      while i < row.slotCount {
        defer { i += 1 }
        if let value = row[slot: i] { return (row.layout.names[i], value) }
      }
      return nil
    }
  }

  @inlinable
  public func makeIterator() -> Iterator { Iterator(self) }

  @inlinable
  public var underestimatedCount: Int { 0 }
}

extension Row: ExpressibleByDictionaryLiteral {
  public init(dictionaryLiteral elements: (String, JSONValue)...) {
    var names: [String] = []
    var slots: [JSONValue?] = []
    var indices: [String: Int] = [:]
    for (name, value) in elements {
      if let i = indices[name] {
        slots[i] = value
      } else {
        indices[name] = names.count
        names.append(name)
        slots.append(value)
      }
    }
    self.init(layout: RowLayout(names, indices: indices), slots: slots)
  }
}

extension Row: Hashable {
  /// The same columns with equal values, whatever the layouts.
  public static func == (a: Row, b: Row) -> Bool {
    if a.storage === b.storage { return true }
    if a.layout === b.layout {
      for i in 0..<a.slotCount where a[slot: i] != b[slot: i] { return false }
      return true
    }
    var n = 0
    for (name, value) in a {
      guard let other = b[name], other == value else { return false }
      n += 1
    }
    return n == b.count
  }

  /// Independent of the layout, as `==` is.
  public func hash(into hasher: inout Hasher) {
    var acc = 0
    var n = 0
    for (name, value) in self {
      var h = Hasher()
      h.combine(name)
      h.combine(value)
      acc ^= h.finalize()
      n += 1
    }
    hasher.combine(n)
    hasher.combine(acc)
  }
}

extension Row: CustomStringConvertible, CustomDebugStringConvertible {
  public var description: String {
    "[" + map { "\(String(reflecting: $0.key)): \($0.value)" }.joined(separator: ", ") + "]"
  }

  public var debugDescription: String { description }
}

extension JSONValue {
  /// The row as a JSON object.
  public init(_ row: Row) {
    self = .object(row.dictionary)
  }
}

/// The columns of rows, in order. The rows of a table share one, so a row
/// stores only its values, and a column's index is the same in all of them.
///
/// Immutable, so reading it needs no exclusivity checks: a row that adds a
/// column gets a new layout.
@usableFromInline
package final class RowLayout: Sendable {
  @usableFromInline let names: [String]
  @usableFromInline let indices: [String: Int]

  package init(_ names: [String]) {
    var indices: [String: Int] = [:]
    indices.reserveCapacity(names.count)
    for (i, name) in names.enumerated() where indices[name] == nil { indices[name] = i }
    self.names = names
    self.indices = indices
  }

  init(_ names: [String], indices: [String: Int]) {
    self.names = names
    self.indices = indices
  }

  package static let empty = RowLayout([])

  /// The layout of `table`'s columns, in declaration order.
  package convenience init(_ table: TableSchema) {
    self.init(table.columnNames)
  }

  @inlinable
  package func index(of name: String) -> Int? { indices[name] }

  /// A column of rows in this layout.
  package func column(_ name: String) -> ColumnRef {
    ColumnRef(layout: self, index: index(of: name), name: name)
  }

  /// This layout with `name` after its columns.
  func appending(_ name: String) -> RowLayout {
    var indices = indices
    indices[name] = names.count
    return RowLayout(names + [name], indices: indices)
  }
}

/// A column, resolved against a layout so rows in it read it by index.
/// Rows in other layouts read it by name.
@usableFromInline
package struct ColumnRef: Sendable {
  @usableFromInline let layout: RowLayout
  /// Nil when the layout has no such column.
  @usableFromInline let index: Int?
  @usableFromInline package let name: String

  @usableFromInline
  init(layout: RowLayout, index: Int?, name: String) {
    self.layout = layout
    self.index = index
    self.name = name
  }
}

@usableFromInline
struct RowHeader {
  @usableFromInline let layout: RowLayout
  /// How many values follow: one for each of the layout's columns.
  @usableFromInline let count: Int

  @inlinable
  init(layout: RowLayout, count: Int) {
    self.layout = layout
    self.count = count
  }
}

/// A row's layout and its values, in one allocation. Rows share it until
/// one changes, which copies it first unless it's the only owner.
@usableFromInline
final class RowStorage: ManagedBuffer<RowHeader, JSONValue?> {
  /// Storage for a value for each of `layout`'s columns, which
  /// `initialize` must initialize.
  @inlinable
  static func make(
    _ layout: RowLayout, _ initialize: (UnsafeMutablePointer<JSONValue?>) throws -> Void
  ) rethrows -> RowStorage {
    let count = layout.names.count
    let storage = unsafeDowncast(
      create(minimumCapacity: count) { _ in RowHeader(layout: layout, count: count) },
      to: RowStorage.self)
    try storage.withUnsafeMutablePointerToElements { try initialize($0) }
    return storage
  }

  @inlinable
  func copy() -> RowStorage {
    withUnsafeMutablePointers { header, values in
      RowStorage.make(header.pointee.layout) {
        $0.initialize(from: values, count: header.pointee.count)
      }
    }
  }

  /// A copy with the column `name` after the others.
  @usableFromInline
  func appending(_ name: String, _ value: JSONValue) -> RowStorage {
    withUnsafeMutablePointers { header, values in
      let count = header.pointee.count
      return RowStorage.make(header.pointee.layout.appending(name)) {
        $0.initialize(from: values, count: count)
        ($0 + count).initialize(to: value)
      }
    }
  }

  deinit {
    withUnsafeMutablePointers { header, values in
      _ = values.deinitialize(count: header.pointee.count)
    }
  }
}
