/// Helpers shared by `Join` and `FlippedJoin` (`join-utils.ts`).

/// `generateWithOverlay`: applies the in-flight child change `overlay` to a
/// sorted child stream, so a parent that has yet to receive the change sees
/// the children as they were.
func generateWithOverlay(
  _ stream: NodeStream, _ overlay: Change, _ schema: SourceSchema
) -> NodeStream {
  NodeStream {
    var it = stream.makeIterator()
    var applied = false
    var editOldApplied = false
    var editNewApplied = false
    var queue: [Node] = []
    var finished = false
    return AnyIterator {
      while true {
        if !queue.isEmpty { return queue.removeFirst() }
        if finished { return nil }
        guard let node = it.next() else {
          finished = true
          if !applied {
            if case .remove(let n) = overlay {
              applied = true
              return n
            } else if case .edit(_, let oldNode) = overlay {
              precondition(editNewApplied, "edit overlay: new node must be applied before old node")
              editOldApplied = true
              applied = true
              return oldNode
            }
          }
          precondition(applied, "overlayGenerator: overlay was never applied to any fetched node")
          return nil
        }
        var yieldNode = true
        if !applied {
          switch overlay {
          case .add(let n):
            if schema.compareRows(n.row, node.row) == 0 {
              applied = true
              yieldNode = false
            }
          case .remove(let n):
            if schema.compareRows(n.row, node.row) < 0 {
              applied = true
              queue.append(n)
            }
          case .edit(let n, let oldNode):
            if !editOldApplied && schema.compareRows(oldNode.row, node.row) < 0 {
              editOldApplied = true
              if editNewApplied { applied = true }
              queue.append(oldNode)
            }
            if !editNewApplied && schema.compareRows(n.row, node.row) == 0 {
              editNewApplied = true
              if editOldApplied { applied = true }
              yieldNode = false
            }
          case .child(let n, let child):
            if schema.compareRows(n.row, node.row) == 0 {
              applied = true
              queue.append(overlayChild(node, child, schema))
              yieldNode = false
            }
          }
        }
        if yieldNode { queue.append(node) }
      }
    }
  }
}

/// `node` with the child change applied to the relationship it is on.
private func overlayChild(_ node: Node, _ child: ChildChange, _ schema: SourceSchema) -> Node {
  let name = child.relationshipName
  let original = node.relationships[name]!
  let childSchema = schema.relationship(name)!
  return node.with(name) { generateWithOverlay(original(), child.change, childSchema) }
}

/// `generateWithOverlayUnordered`: the overlay for an unordered child
/// stream. Removed rows go first; added rows are suppressed by primary key.
func generateWithOverlayUnordered(
  _ stream: NodeStream, _ overlay: Change, _ schema: SourceSchema
) -> NodeStream {
  NodeStream {
    var it = stream.makeIterator()
    var injected = false
    var suppressed = false
    return AnyIterator {
      if !injected {
        injected = true
        if case .remove(let n) = overlay { return n }
        if case .edit(_, let oldNode) = overlay { return oldNode }
      }
      while let node = it.next() {
        if !suppressed {
          switch overlay {
          case .add(let n), .edit(let n, _):
            if rowEqualsForCompoundKey(n.row, node.row, schema.primaryKey) {
              suppressed = true
              continue
            }
          case .child(let n, let child):
            if rowEqualsForCompoundKey(n.row, node.row, schema.primaryKey) {
              suppressed = true
              return overlayChild(node, child, schema)
            }
          case .remove:
            break
          }
        }
        return node
      }
      precondition(
        suppressed || overlay.type == .remove,
        "overlayGenerator: overlay was never applied to any fetched node")
      return nil
    }
  }
}

func rowEqualsForCompoundKey(_ a: Row, _ b: Row, _ key: [String]) -> Bool {
  for k in key where ValueCompare.compare(a[k], b[k]) != 0 { return false }
  return true
}

func isJoinMatch(_ parent: Row, _ parentKey: [String], _ child: Row, _ childKey: [String]) -> Bool {
  for i in 0..<parentKey.count where !ValueCompare.equal(parent[parentKey[i]], child[childKey[i]]) {
    return false
  }
  return true
}

/// `buildJoinConstraint`: `targetKey` mapped to `sourceRow`'s `sourceKey`
/// values, or `nil` if one is null (a null key matches nothing).
func buildJoinConstraint(
  _ sourceRow: Row, _ sourceKey: [String], _ targetKey: [String]
)
  -> Constraint?
{
  var c = Constraint()
  for i in 0..<targetKey.count {
    let value = sourceRow[sourceKey[i]]
    if value == .null { return nil }
    c.set(targetKey[i], value)
  }
  return c
}

/// `canonicalKey`: a string key over `keys`, tagged by type so `1` and `"1"`
/// differ.
func canonicalKey(_ row: Row, _ keys: [String]) -> JSKey {
  if keys.count == 1 { return JSKey(canonicalValue(row[keys[0]])) }
  var s = ""
  for (i, k) in keys.enumerated() {
    if i > 0 { s += "\u{0}" }
    s += canonicalValue(row[k])
  }
  return JSKey(s)
}

func canonicalKey(_ constraint: Constraint, _ keys: [String]) -> JSKey {
  var row: Row = [:]
  for e in constraint.entries { row[e.key] = e.value }
  return canonicalKey(row, keys)
}

private func canonicalValue(_ v: Value?) -> String {
  switch v {
  case nil, .null?: "n"
  case .string(let s)?: "s" + s
  case .number(let n)?: "d" + JSNumber.format(n)
  case .bool(let b)?: b ? "t" : "f"
  case let v?: "j" + JSONSerializer.stringify(v)
  }
}

/// The partitions of indexed parents that share a join key, as fetch
/// constraints; `[nil]` for an unpartitioned index (`join-index.ts`).
typealias ParentPartitions = [Constraint?]

/// An index from join key to the parent rows that have it. `Join` and
/// `FlippedJoin` skip the parent fetch of a child change when no indexed
/// parent can see it.
final class JoinIndex {
  struct Partition {
    var constraint: Constraint
    var pks: Set<JSKey>
  }

  let parentKey: [String]
  let primaryKey: [String]
  let joinKeyIsPK: Bool
  let partitionKey: [String]?
  var unpartitioned: [JSKey: Set<JSKey>] = [:]
  /// Join key, then partition key. Partitions keep insertion order.
  var partitioned: [JSKey: (order: [JSKey], partitions: [JSKey: Partition])] = [:]
  private(set) var size = 0

  init(_ parentKey: [String], _ primaryKey: [String], _ partitionKey: [String]? = nil) {
    self.parentKey = parentKey
    self.primaryKey = primaryKey
    joinKeyIsPK = parentKey == primaryKey
    self.partitionKey = partitionKey
  }

  func add(_ row: Row) {
    guard let joinKey = joinKeyOf(row, parentKey) else { return }
    let pk = joinKeyIsPK ? joinKey : canonicalKey(row, primaryKey)
    guard let partitionKey else {
      if unpartitioned[joinKey, default: []].insert(pk).inserted { size += 1 }
      return
    }
    let partKey = canonicalKey(row, partitionKey)
    var entry = partitioned[joinKey] ?? ([], [:])
    if entry.partitions[partKey] == nil {
      entry.order.append(partKey)
      entry.partitions[partKey] = Partition(constraint: Constraint.from(row, partitionKey), pks: [])
    }
    if entry.partitions[partKey]!.pks.insert(pk).inserted { size += 1 }
    partitioned[joinKey] = entry
  }

  func remove(_ row: Row) {
    guard let joinKey = joinKeyOf(row, parentKey) else { return }
    let pk = joinKeyIsPK ? joinKey : canonicalKey(row, primaryKey)
    guard let partitionKey else {
      guard var pks = unpartitioned[joinKey] else { return }
      if pks.remove(pk) != nil {
        size -= 1
        unpartitioned[joinKey] = pks.isEmpty ? nil : pks
      }
      return
    }
    guard var entry = partitioned[joinKey] else { return }
    let partKey = canonicalKey(row, partitionKey)
    guard var partition = entry.partitions[partKey] else { return }
    if partition.pks.remove(pk) != nil {
      size -= 1
      if partition.pks.isEmpty {
        entry.partitions[partKey] = nil
        entry.order.removeAll { $0 == partKey }
        partitioned[joinKey] = entry.partitions.isEmpty ? nil : entry
      } else {
        entry.partitions[partKey] = partition
        partitioned[joinKey] = entry
      }
    }
  }

  /// The partitions of the indexed parents whose join key matches
  /// `childRow` over `childKey`, or `nil` if there are none.
  func lookup(_ childRow: Row, _ childKey: [String]) -> ParentPartitions? {
    guard let joinKey = joinKeyOf(childRow, childKey) else { return nil }
    guard partitionKey != nil else {
      return unpartitioned[joinKey] != nil ? [nil] : nil
    }
    guard let entry = partitioned[joinKey] else { return nil }
    return entry.order.map { entry.partitions[$0]!.constraint }
  }

  /// The canonical join key of `row`, or `nil` if part of it is null.
  private func joinKeyOf(_ row: Row, _ key: [String]) -> JSKey? {
    for k in key where row[k] == .null { return nil }
    return canonicalKey(row, key)
  }
}
