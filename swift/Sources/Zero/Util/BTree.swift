/// A persistent B+ tree set (`packages/shared/src/btree-set.ts`).
///
/// Nodes are shared between copies and copied on write, so copying a tree
/// is O(1) (`MemorySource.fork`) and an iterator keeps reading the tree as
/// it was when the iterator was created.
@usableFromInline
package struct BTreeSet<Element> {
  @usableFromInline package typealias Compare = (Element, Element) -> Int

  @usableFromInline final class Node {
    // Unchecked: the tree is a value type that copies shared nodes before
    // writing them, so no access to a node overlaps a write to it, and the
    // checks cost more than the reads (`ForwardIterator`).
    /// Leaf: the elements. Internal: the highest element of each child.
    @exclusivity(unchecked) @usableFromInline var keys: ContiguousArray<Element>
    /// Empty for a leaf.
    @exclusivity(unchecked) @usableFromInline var children: ContiguousArray<Node>

    @inlinable init(keys: ContiguousArray<Element>, children: ContiguousArray<Node> = []) {
      self.keys = keys
      self.children = children
    }

    @usableFromInline var isLeaf: Bool { children.isEmpty }

    @inlinable func copy() -> Node { Node(keys: keys, children: children) }
  }

  @usableFromInline static var maxNodeSize: Int { 64 }

  @usableFromInline var root: Node
  @usableFromInline var _count: Int
  @usableFromInline package let compare: Compare

  @usableFromInline package var count: Int { _count }

  @inlinable
  package init(_ compare: @escaping Compare) {
    self.compare = compare
    root = Node(keys: [])
    _count = 0
  }

  /// Builds a tree from elements already sorted by `compare`, in O(n).
  @inlinable
  package init<S: Sequence>(sorted elements: S, _ compare: @escaping Compare)
  where S.Element == Element {
    self.compare = compare
    let max = Self.maxNodeSize
    var level: ContiguousArray<Node> = []
    var keys: ContiguousArray<Element> = []
    keys.reserveCapacity(max)
    var n = 0
    for e in elements {
      keys.append(e)
      n += 1
      if keys.count == max {
        level.append(Node(keys: keys))
        keys.removeAll(keepingCapacity: true)
      }
    }
    if !keys.isEmpty || level.isEmpty { level.append(Node(keys: keys)) }
    while level.count > 1 {
      var next: ContiguousArray<Node> = []
      var i = 0
      while i < level.count {
        let children = ContiguousArray(level[i..<Swift.min(i + max, level.count)])
        next.append(
          Node(
            keys: ContiguousArray(children.lazy.map { $0.keys[$0.keys.count - 1] }),
            children: children))
        i += max
      }
      level = next
    }
    root = level[0]
    _count = n
  }

  @usableFromInline package var isEmpty: Bool { _count == 0 }

  /// The stored element equal to `element`.
  @inlinable
  package func get(_ element: Element) -> Element? {
    // The tree keeps its nodes alive for the walk, which therefore needn't
    // retain each one it passes.
    withExtendedLifetime(root) {
      unowned(unsafe) var node = root
      while !node.isLeaf {
        let i = node.keys.withUnsafeBufferPointer { keys in
          lowerBound(keys) { compare($0, element) }
        }
        if i == node.keys.count { return nil }
        node = node.children.withUnsafeBufferPointer { $0[i] }
      }
      return node.keys.withUnsafeBufferPointer { keys in
        let i = lowerBound(keys) { compare($0, element) }
        return i < keys.count && compare(keys[i], element) == 0 ? keys[i] : nil
      }
    }
  }

  @inlinable
  package func contains(_ element: Element) -> Bool { get(element) != nil }

  /// Adds `element`, replacing an equal one. Returns whether it was new.
  @inlinable @discardableResult
  package mutating func add(_ element: Element) -> Bool {
    if !isKnownUniquelyReferenced(&root) { root = root.copy() }
    let (inserted, split) = Self.insert(element, into: root, compare)
    if let split {
      root = Node(
        keys: [root.keys[root.keys.count - 1], split.keys[split.keys.count - 1]],
        children: [root, split])
    }
    if inserted { _count += 1 }
    return inserted
  }

  /// Removes the element equal to `element`. Returns whether there was one.
  @inlinable @discardableResult
  package mutating func delete(_ element: Element) -> Bool {
    if !isKnownUniquelyReferenced(&root) { root = root.copy() }
    let removed = Self.remove(element, from: root, compare)
    while !root.isLeaf && root.children.count == 1 { root = root.children[0] }
    if !root.isLeaf && root.children.isEmpty { root = Node(keys: []) }
    if removed { _count -= 1 }
    return removed
  }

  /// The elements, ascending.
  @inlinable
  package func values() -> ForwardIterator { values(from: nil) }

  /// The elements not less than `probe`, ascending. `probe(e)` compares `e`
  /// to the bound: negative when `e` is before it.
  @inlinable
  package func values(from probe: ((Element) -> Int)?) -> ForwardIterator {
    ForwardIterator(root: root, probe: probe)
  }

  /// The elements not greater than `probe`, descending.
  @inlinable
  package func valuesReversed(from probe: ((Element) -> Int)?) -> ReverseIterator {
    ReverseIterator(root: root, probe: probe)
  }

  // MARK: Mutation

  /// Inserts into `node`, which the caller made unique. Returns whether the
  /// element was new and the new right sibling if `node` split.
  @inlinable
  static func insert(_ e: Element, into node: Node, _ compare: Compare) -> (Bool, Node?) {
    if node.isLeaf {
      let i = lowerBound(node.keys) { compare($0, e) }
      if i < node.keys.count && compare(node.keys[i], e) == 0 {
        node.keys[i] = e
        return (false, nil)
      }
      node.keys.insert(e, at: i)
      return (true, splitIfNeeded(node))
    }
    var i = lowerBound(node.keys) { compare($0, e) }
    if i == node.keys.count { i -= 1 }
    if !isKnownUniquelyReferenced(&node.children[i]) { node.children[i] = node.children[i].copy() }
    let child = node.children[i]
    let (inserted, split) = insert(e, into: child, compare)
    node.keys[i] = child.keys[child.keys.count - 1]
    if let split {
      node.children.insert(split, at: i + 1)
      node.keys.insert(split.keys[split.keys.count - 1], at: i + 1)
    }
    return (inserted, splitIfNeeded(node))
  }

  @inlinable
  static func splitIfNeeded(_ node: Node) -> Node? {
    if node.keys.count <= maxNodeSize { return nil }
    let half = node.keys.count / 2
    let right = Node(
      keys: ContiguousArray(node.keys[half...]),
      children: node.isLeaf ? [] : ContiguousArray(node.children[half...]))
    node.keys.removeSubrange(half...)
    if !node.isLeaf { node.children.removeSubrange(half...) }
    return right
  }

  /// Removes from `node`, which the caller made unique.
  @inlinable
  static func remove(_ e: Element, from node: Node, _ compare: Compare) -> Bool {
    let i = lowerBound(node.keys) { compare($0, e) }
    if i == node.keys.count { return false }
    if node.isLeaf {
      if compare(node.keys[i], e) != 0 { return false }
      node.keys.remove(at: i)
      return true
    }
    if !isKnownUniquelyReferenced(&node.children[i]) { node.children[i] = node.children[i].copy() }
    let child = node.children[i]
    if !remove(e, from: child, compare) { return false }
    if child.keys.isEmpty {
      node.children.remove(at: i)
      node.keys.remove(at: i)
      return true
    }
    node.keys[i] = child.keys[child.keys.count - 1]
    if child.keys.count < maxNodeSize / 4 {
      // Merge with a neighbor when the two fit in one node.
      let j = i + 1 < node.children.count ? i + 1 : i - 1
      if j >= 0 && node.children[j].keys.count + child.keys.count <= maxNodeSize {
        let (l, r) = j > i ? (i, j) : (j, i)
        if !isKnownUniquelyReferenced(&node.children[l]) {
          node.children[l] = node.children[l].copy()
        }
        let left = node.children[l]
        let right = node.children[r]
        left.keys.append(contentsOf: right.keys)
        left.children.append(contentsOf: right.children)
        node.children.remove(at: r)
        node.keys.remove(at: r)
        node.keys[l] = left.keys[left.keys.count - 1]
      }
    }
    return true
  }

  // MARK: Iteration

  @usableFromInline package struct ForwardIterator: IteratorProtocol, Sequence {
    @usableFromInline var path: [(Node, Int)] = []
    @usableFromInline var leaf: Node?
    /// The leaf's keys, read once per leaf.
    @usableFromInline var keys: ContiguousArray<Element> = []
    @usableFromInline var index = 0

    @inlinable
    init(root: Node, probe: ((Element) -> Int)?) {
      var node = root
      while !node.isLeaf {
        let i = probe.map { p in lowerBound(node.keys) { p($0) } } ?? 0
        if i == node.keys.count { return }
        path.append((node, i))
        node = node.children[i]
      }
      leaf = node
      keys = node.keys
      index = probe.map { p in lowerBound(node.keys) { p($0) } } ?? 0
    }

    @inlinable
    package mutating func next() -> Element? {
      if index < keys.count {
        index += 1
        return keys[index - 1]
      }
      while leaf != nil {
        if index < keys.count {
          index += 1
          return keys[index - 1]
        }
        leaf = nil
        keys = []
        while let (node, ci) = path.popLast() {
          if ci + 1 < node.children.count {
            path.append((node, ci + 1))
            var n = node.children[ci + 1]
            while !n.isLeaf {
              path.append((n, 0))
              n = n.children[0]
            }
            leaf = n
            keys = n.keys
            index = 0
            break
          }
        }
      }
      return nil
    }
  }

  @usableFromInline package struct ReverseIterator: IteratorProtocol, Sequence {
    @usableFromInline var path: [(Node, Int)] = []
    @usableFromInline var leaf: Node?
    @usableFromInline var index = 0

    @inlinable
    init(root: Node, probe: ((Element) -> Int)?) {
      var node = root
      while !node.isLeaf {
        var i = node.keys.count - 1
        if let p = probe {
          i = Swift.min(lowerBound(node.keys) { p($0) }, node.keys.count - 1)
        }
        path.append((node, i))
        node = node.children[i]
      }
      leaf = node
      if let p = probe {
        // The last element not greater than the bound.
        index = upperBound(node.keys) { p($0) } - 1
      } else {
        index = node.keys.count - 1
      }
    }

    @inlinable
    package mutating func next() -> Element? {
      while let l = leaf {
        if index >= 0 {
          index -= 1
          return l.keys[index + 1]
        }
        leaf = nil
        while let (node, ci) = path.popLast() {
          if ci > 0 {
            path.append((node, ci - 1))
            var n = node.children[ci - 1]
            while !n.isLeaf {
              path.append((n, n.children.count - 1))
              n = n.children[n.children.count - 1]
            }
            leaf = n
            index = n.keys.count - 1
            break
          }
        }
      }
      return nil
    }
  }
}

/// The first index whose element compares `>= 0`.
@inlinable
func lowerBound<E>(_ keys: UnsafeBufferPointer<E>, _ cmp: (E) -> Int) -> Int {
  var low = 0
  var high = keys.count
  while low < high {
    let mid = (low + high) >> 1
    if cmp(keys[mid]) < 0 { low = mid + 1 } else { high = mid }
  }
  return low
}

/// The first index whose element compares `>= 0`.
@inlinable
func lowerBound<E>(_ keys: ContiguousArray<E>, _ cmp: (E) -> Int) -> Int {
  var low = 0
  var high = keys.count
  while low < high {
    let mid = (low + high) >> 1
    if cmp(keys[mid]) < 0 { low = mid + 1 } else { high = mid }
  }
  return low
}

/// The first index whose element compares `> 0`.
@inlinable
func upperBound<E>(_ keys: ContiguousArray<E>, _ cmp: (E) -> Int) -> Int {
  var low = 0
  var high = keys.count
  while low < high {
    let mid = (low + high) >> 1
    if cmp(keys[mid]) <= 0 { low = mid + 1 } else { high = mid }
  }
  return low
}
