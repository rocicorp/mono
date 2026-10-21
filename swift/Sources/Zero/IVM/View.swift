/// The shape of a query result: whether it is a single row, and the
/// formats of its relationships (`packages/zero-types/src/format.ts`).
package struct Format: Sendable, Hashable {
  package var singular: Bool
  package var relationships: [String: Format]

  package init(singular: Bool = false, relationships: [String: Format] = [:]) {
    self.singular = singular
    self.relationships = relationships
  }
}

/// A row in a materialized view, with the relationships the query asked
/// for (`Entry` in `view.ts`).
public struct ViewEntry: Sendable {
  public var row: Row
  public var relationships: [String: ViewValue]
  /// How many edges of the relationship reach this row.
  var refCount: Int

  init(row: Row, refCount: Int = 1) {
    self.row = row
    self.relationships = [:]
    self.refCount = refCount
  }

  /// The row with its relationships, as JS views present it.
  public var json: JSONValue {
    var o = row.dictionary
    for (name, value) in relationships {
      switch value {
      case .many(let entries): o[name] = .array(entries.map(\.json))
      case .one(let entry?): o[name] = entry.json
      case .one(nil): o[name] = nil
      }
    }
    return .object(o)
  }
}

extension ViewEntry: Equatable {
  /// Equal rows with equal relationships. How many edges reach the row
  /// doesn't matter.
  public static func == (a: ViewEntry, b: ViewEntry) -> Bool {
    a.row == b.row && a.relationships == b.relationships
  }
}

/// A relationship in a view: several rows, or at most one.
public enum ViewValue: Sendable, Equatable {
  case many([ViewEntry])
  case one(ViewEntry?)

  /// The value as JS views present it; `nil` for a missing singular row.
  public var json: JSONValue? {
    switch self {
    case .many(let entries): .array(entries.map(\.json))
    case .one(let entry): entry?.json
    }
  }

  public var entries: [ViewEntry] {
    switch self {
    case .many(let e): e
    case .one(let e): e.map { [$0] } ?? []
    }
  }
}

/// Applies changes to a view tree (`view-apply-change.ts`). Swift's value
/// semantics give the JS copy-on-write behavior: unchanged subtrees keep
/// sharing storage with earlier snapshots.
enum ViewApply {
  /// Applies `change` to `parent`'s `relationship`. Returns whether the
  /// tree changed (JS returns the same entry when it did not).
  @discardableResult
  static func apply(
    _ parent: inout ViewEntry, _ change: Change, _ schema: SourceSchema, _ relationship: String,
    _ format: Format
  ) -> Bool {
    if schema.isHidden {
      switch change {
      case .add(let node), .remove(let node):
        var changed = false
        let isAdd = change.type == .add
        for (name, stream) in node.relationships {
          guard let childSchema = schema.relationship(name) else {
            preconditionFailure("missing relationship schema \(name)")
          }
          for child in stream() {
            changed =
              apply(&parent, isAdd ? .add(child) : .remove(child), childSchema, name, format)
              || changed
          }
        }
        return changed
      case .edit:
        // A hidden row changed; a change to its relationships would have
        // been split into a remove and an add.
        return false
      case .child(_, let child):
        guard let childSchema = schema.relationship(child.relationshipName) else {
          preconditionFailure("missing relationship schema \(child.relationshipName)")
        }
        return apply(&parent, child.change, childSchema, relationship, format)
      }
    }

    switch change {
    case .add(let node):
      if format.singular {
        if case .one(var old?)? = parent.relationships[relationship] {
          precondition(
            schema.compareRows(old.row, node.row) == 0,
            "Singular relationship '\(relationship)' should not have multiple rows. You may need to declare this relationship with the `many` helper instead of the `one` helper in your schema."
          )
          old.refCount += 1
          parent.relationships[relationship] = .one(old)
        } else {
          var entry = ViewEntry(row: node.row)
          initializeRelationships(&entry, node, schema, format.relationships)
          parent.relationships[relationship] = .one(entry)
        }
      } else {
        var list = takeList(&parent, relationship)
        let pos = search(list, node.row, schema.compareRows)
        if pos >= 0 {
          list[pos].refCount += 1
        } else {
          var entry = ViewEntry(row: node.row)
          initializeRelationships(&entry, node, schema, format.relationships)
          list.insert(entry, at: ~pos)
        }
        parent.relationships[relationship] = .many(list)
      }
      return true

    case .remove(let node):
      if format.singular {
        guard case .one(var old?)? = parent.relationships[relationship] else {
          preconditionFailure("node does not exist")
        }
        if old.refCount == 1 {
          parent.relationships[relationship] = .one(nil)
        } else {
          old.refCount -= 1
          parent.relationships[relationship] = .one(old)
        }
      } else {
        var list = takeList(&parent, relationship)
        let pos = search(list, node.row, schema.compareRows)
        precondition(pos >= 0, "node does not exist")
        if list[pos].refCount == 1 {
          list.remove(at: pos)
        } else {
          list[pos].refCount -= 1
        }
        parent.relationships[relationship] = .many(list)
      }
      return true

    case .child(let node, let child):
      guard let childSchema = schema.relationship(child.relationshipName) else {
        preconditionFailure("missing relationship schema \(child.relationshipName)")
      }
      guard let childFormat = format.relationships[child.relationshipName] else {
        return false  // not in the view
      }
      if format.singular {
        guard case .one(var existing?)? = parent.relationships.removeValue(forKey: relationship)
        else { preconditionFailure("node does not exist") }
        let changed = apply(
          &existing, child.change, childSchema, child.relationshipName, childFormat)
        parent.relationships[relationship] = .one(existing)
        return changed
      }
      var list = takeList(&parent, relationship)
      let pos = search(list, node.row, schema.compareRows)
      precondition(pos >= 0, "node does not exist")
      let changed = apply(
        &list[pos], child.change, childSchema, child.relationshipName, childFormat)
      parent.relationships[relationship] = .many(list)
      return changed

    case .edit(let node, let oldNode):
      if format.singular {
        guard case .one(var existing?)? = parent.relationships[relationship] else {
          preconditionFailure("node does not exist")
        }
        applyEdit(&existing, node.row)
        parent.relationships[relationship] = .one(existing)
        return true
      }
      var list = takeList(&parent, relationship)
      defer { parent.relationships[relationship] = .many(list) }
      if schema.compareRows(oldNode.row, node.row) != 0 {
        // The sort key changed, so the row may move.
        let oldPos = search(list, oldNode.row, schema.compareRows)
        precondition(oldPos >= 0, "old node does not exist")
        let rawPos = search(list, node.row, schema.compareRows)
        let found = rawPos >= 0
        let pos = found ? rawPos : ~rawPos
        // With one reference, a row landing where it was is only edited.
        if list[oldPos].refCount == 1 && (pos == oldPos || pos - 1 == oldPos) {
          applyEdit(&list[oldPos], node.row)
          return true
        }
        // Move the row. With several references an edit arrives for each:
        // a copy stays behind until the last one moves.
        let oldEntry = list[oldPos]
        let newRefCount = oldEntry.refCount - 1
        var adjustedPos = pos
        if newRefCount == 0 {
          list.remove(at: oldPos)
          adjustedPos = oldPos < pos ? pos - 1 : pos
        } else {
          list[oldPos].refCount = newRefCount
        }
        if found {
          let rc = list[adjustedPos].refCount
          applyEdit(&list[adjustedPos], node.row)
          list[adjustedPos].refCount = rc + 1
        } else {
          var moved = oldEntry
          applyEdit(&moved, node.row)
          moved.refCount = 1
          list.insert(moved, at: adjustedPos)
        }
        return true
      }
      let pos = search(list, oldNode.row, schema.compareRows)
      precondition(pos >= 0, "node does not exist")
      applyEdit(&list[pos], node.row)
      return true
    }
  }

  /// Applies `.add` for each node, taking the list out of `parent` once
  /// rather than for each node.
  static func addAll(
    _ parent: inout ViewEntry, _ nodes: some Sequence<Node>, _ schema: SourceSchema,
    _ relationship: String, _ format: Format
  ) {
    guard !schema.isHidden, !format.singular else {
      for node in nodes { apply(&parent, .add(node), schema, relationship, format) }
      return
    }
    var list = takeList(&parent, relationship)
    for node in nodes {
      let pos = search(list, node.row, schema.compareRows)
      if pos >= 0 {
        list[pos].refCount += 1
      } else {
        var entry = ViewEntry(row: node.row)
        initializeRelationships(&entry, node, schema, format.relationships)
        list.insertOrAppend(entry, at: ~pos)
      }
    }
    parent.relationships[relationship] = .many(list)
  }

  /// `{...existing, ...row}`.
  static func applyEdit(_ entry: inout ViewEntry, _ row: Row) {
    if entry.row.layout === row.layout {
      for i in 0..<row.slotCount {
        if let value = row[slot: i] { entry.row[slot: i] = value }
      }
    } else {
      for (name, value) in row { entry.row[name] = value }
    }
  }

  /// Removes and returns the list for `relationship`, so that it is mutated
  /// in place rather than copied.
  static func takeList(_ parent: inout ViewEntry, _ relationship: String) -> [ViewEntry] {
    guard case .many(let list)? = parent.relationships.removeValue(forKey: relationship) else {
      preconditionFailure("expected relationship array")
    }
    return list
  }

  /// Fills in a new entry's relationships from its node.
  static func initializeRelationships(
    _ entry: inout ViewEntry, _ node: Node, _ schema: SourceSchema, _ childFormats: [String: Format]
  ) {
    for (name, stream) in node.relationships {
      guard let childSchema = schema.relationship(name) else {
        preconditionFailure("missing relationship schema \(name)")
      }
      guard let childFormat = childFormats[name] else { continue }
      if childSchema.isHidden || childFormat.singular {
        entry.relationships[name] = childFormat.singular ? .one(nil) : .many([])
        for child in stream() {
          apply(&entry, .add(child), childSchema, name, childFormat)
        }
      } else {
        var list: [ViewEntry] = []
        for child in stream() {
          let pos = search(list, child.row, childSchema.compareRows)
          if pos >= 0 {
            list[pos].refCount += 1
          } else {
            var e = ViewEntry(row: child.row)
            initializeRelationships(&e, child, childSchema, childFormat.relationships)
            list.insertOrAppend(e, at: ~pos)
          }
        }
        entry.relationships[name] = .many(list)
      }
    }
  }

  /// The index of `target`, or `~insertionPoint`. Probes the end first:
  /// hydration appends in order.
  static func search(_ view: [ViewEntry], _ target: Row, _ comparator: Comparator) -> Int {
    var high = view.count - 1
    if high < 0 { return ~0 }
    let last = comparator(view[high].row, target)
    if last < 0 { return ~(high + 1) }
    if last == 0 { return high }
    var low = 0
    high -= 1
    while low <= high {
      let mid = (low + high) >> 1
      let c = comparator(view[mid].row, target)
      if c < 0 {
        low = mid + 1
      } else if c > 0 {
        high = mid - 1
      } else {
        return mid
      }
    }
    return ~low
  }
}

/// The result type of a view (`ResultType` in `typed-view.ts`).
public enum ResultType: String, Sendable, Hashable {
  case unknown, cached, complete, error
}

/// A materialized view of a pipeline's output (`array-view.ts`).
///
/// Listeners are notified on `flush`, which the engine calls when a
/// transaction commits.
package final class ArrayView: Output {
  package typealias Listener = (ViewValue, ResultType) -> Void

  let input: any Input
  let format: Format
  private var schema: SourceSchema?
  private var root: ViewEntry
  private var listeners: [(id: Int, listener: Listener)] = []
  private var nextListenerID = 0
  private var dirty = false
  private var committedRoot: ViewEntry?
  private var held = false
  private var heldResultType: ResultType = .unknown
  private var heldError: ErroredQuery?
  package private(set) var resultType: ResultType = .unknown
  /// Why the query failed, when `resultType` is `.error`.
  package private(set) var error: ErroredQuery?
  package var onDestroy: (() -> Void)?

  /// Hydrates from `input`. `complete` marks the result complete from the
  /// start.
  package convenience init(_ input: any Input, format: Format, complete: Bool = false) {
    self.init(input, format: format, error: nil, complete: complete)
  }

  /// Hydrates from `input`, starting as failed when `error` is set.
  package init(_ input: any Input, format: Format, error: ErroredQuery?, complete: Bool) {
    self.input = input
    self.format = format
    root = ViewEntry(row: [:])
    root.relationships[""] = format.singular ? .one(nil) : .many([])
    if complete {
      resultType = .complete
    } else if let error {
      resultType = .error
      self.error = error
    }
    input.setOutput(self)
    hydrate()
  }

  /// The current rows.
  package var data: ViewValue {
    (committedRoot ?? root).relationships[""]!
  }

  private func getSchema() -> SourceSchema {
    if let schema { return schema }
    let s = input.getSchema()
    schema = s
    return s
  }

  /// Adds a listener, calls it with the current data, and returns a
  /// function that removes it.
  @discardableResult
  package func addListener(_ listener: @escaping Listener) -> () -> Void {
    let id = nextListenerID
    nextListenerID += 1
    listeners.append((id, listener))
    let (data, type) = listenerArgs()
    listener(data, type)
    return { [weak self] in self?.listeners.removeAll { $0.id == id } }
  }

  private func listenerArgs() -> (ViewValue, ResultType) {
    if committedRoot != nil { return (data, heldResultType) }
    return (data, resultType)
  }

  private func fireListeners() {
    let (data, type) = listenerArgs()
    for (_, listener) in listeners { listener(data, type) }
  }

  package func destroy() { onDestroy?() }

  private func hydrate() {
    dirty = true
    ViewApply.addAll(&root, input.fetch(FetchRequest()), getSchema(), "", format)
    flush()
  }

  package func push(_ change: Change, pusher: any InputBase) {
    if ViewApply.apply(&root, change, getSchema(), "", format) {
      dirty = true
    }
  }

  /// Keeps exposing the current snapshot until `releaseData` or the next
  /// `flush`.
  package func holdData() {
    committedRoot = root
    held = true
    heldResultType = resultType
    heldError = error
  }

  /// The result type and error listeners are told about now.
  package var listenerResultType: (ResultType, ErroredQuery?) {
    committedRoot != nil ? (heldResultType, heldError) : (resultType, error)
  }

  package func releaseData() { committedRoot = nil }

  /// Notifies listeners if the view changed since the last flush.
  package func flush() {
    committedRoot = nil
    held = false
    if !dirty { return }
    dirty = false
    fireListeners()
  }

  package func setResultType(_ type: ResultType, error: ErroredQuery? = nil) {
    resultType = type
    self.error = error
    if held {
      dirty = true
    } else if !dirty {
      fireListeners()
    }
  }

  /// The server-confirmed result of an earlier sync is in the store.
  package func markCached() {
    if resultType == .unknown { setResultType(.cached) }
  }

  package func unmarkCached() {
    if resultType == .cached { setResultType(.unknown) }
  }
}

extension Format: JSONCodable {
  package init(json: JSONValue) throws(DecodingError) {
    let o = try JSONObjectReader(json)
    let relationships = try o.optional("relationships") { (v) throws(DecodingError) in
      try JSONDecode.record(v) { (f) throws(DecodingError) in try Format(json: f) }
    }
    self.init(
      singular: try o.optionalBool("singular") ?? false, relationships: relationships ?? [:])
  }

  package var json: JSONValue {
    var rels: [String: JSONValue] = [:]
    for (k, v) in relationships { rels[k] = v.json }
    return ["singular": .bool(singular), "relationships": .object(rels)]
  }
}

extension Array<ViewEntry> {
  /// `insert(_:at:)`, appending at the end, where hydration adds rows. Unlike
  /// `insert`, `append` moves the entry in rather than copying it.
  mutating func insertOrAppend(_ entry: ViewEntry, at i: Int) {
    if i == count { append(entry) } else { insert(entry, at: i) }
  }
}
