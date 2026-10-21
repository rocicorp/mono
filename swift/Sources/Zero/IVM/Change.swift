/// A row flowing through a pipeline, plus its relationships
/// (`packages/zql/src/ivm/data.ts`). Relationships are generated lazily as
/// they are read.
package struct Node {
  package var row: Row
  package var relationships: Relationships

  @inlinable
  package init(row: Row, relationships: Relationships = Relationships()) {
    self.row = row
    self.relationships = relationships
  }

  /// This node with `stream` as its relationship `name`.
  @inlinable
  func with(_ name: String, _ stream: @escaping () -> NodeStream) -> Node {
    var copy = self
    copy.relationships[name] = stream
    return copy
  }
}

/// A node's relationships, in insertion order like a JS object's keys. Nodes
/// carry a handful at most, so lookups are linear.
package struct Relationships: Sequence {
  package typealias Stream = () -> NodeStream
  @usableFromInline var entries: [(name: String, stream: Stream)]

  @inlinable
  package init() { entries = [] }

  @inlinable
  package var isEmpty: Bool { entries.isEmpty }

  @inlinable
  package var names: [String] { entries.map(\.name) }

  @inlinable
  package subscript(name: String) -> Stream? {
    get {
      for entry in entries where entry.name == name { return entry.stream }
      return nil
    }
    set {
      if let i = entries.firstIndex(where: { $0.name == name }) {
        if let newValue {
          entries[i].stream = newValue
        } else {
          entries.remove(at: i)
        }
      } else if let newValue {
        entries.append((name, newValue))
      }
    }
  }

  /// `{...self, ...other}`: `other` wins, keys keep their first position.
  @inlinable
  package func merging(_ other: Relationships) -> Relationships {
    var result = self
    for entry in other.entries { result[entry.name] = entry.stream }
    return result
  }

  @inlinable
  package func makeIterator() -> IndexingIterator<[(name: String, stream: Stream)]> {
    entries.makeIterator()
  }
}

/// The kinds of change (`change-type-enum.ts`).
package enum ChangeType: Int, Sendable, Hashable {
  case add = 0
  case remove = 1
  case edit = 2
  case child = 3
}

/// A change to a child relationship of a node.
package struct ChildChange {
  package var relationshipName: String
  package var change: Change

  @inlinable
  package init(relationshipName: String, change: Change) {
    self.relationshipName = relationshipName
    self.change = change
  }
}

/// A change pushed through a pipeline (`packages/zql/src/ivm/change.ts`).
package indirect enum Change {
  case add(Node)
  case remove(Node)
  case child(Node, ChildChange)
  case edit(Node, oldNode: Node)

  @inlinable
  package var type: ChangeType {
    switch self {
    case .add: .add
    case .remove: .remove
    case .child: .child
    case .edit: .edit
    }
  }

  /// The node, or the new node of an edit.
  @inlinable
  package var node: Node {
    switch self {
    case .add(let n), .remove(let n), .child(let n, _), .edit(let n, _): n
    }
  }
}

/// A change to a source's rows (`packages/zql/src/ivm/source.ts`).
package enum SourceChange: Sendable {
  case add(Row)
  case remove(Row)
  case edit(Row, oldRow: Row)

  /// The change with its rows in `layout`.
  func conformed(to layout: RowLayout) -> SourceChange {
    switch self {
    case .add(let row): .add(row.conformed(to: layout))
    case .remove(let row): .remove(row.conformed(to: layout))
    case .edit(let row, let oldRow):
      .edit(row.conformed(to: layout), oldRow: oldRow.conformed(to: layout))
    }
  }

  @inlinable
  package var type: ChangeType {
    switch self {
    case .add: .add
    case .remove: .remove
    case .edit: .edit
    }
  }

  /// The row, or the new row of an edit.
  @inlinable
  package var row: Row {
    switch self {
    case .add(let r), .remove(let r), .edit(let r, _): r
    }
  }
}
