/// The IVM sources of one state of the replica
/// (`packages/zero-client/src/client/ivm-branch.ts`). The main branch feeds
/// the app's views; mutators run against forks, so a rebase can replay them
/// over a newer server snapshot without touching the views.
final class IVMBranch {
  private var sources: [String: MemorySource?]
  private let schema: ZeroSchema
  /// The layout of each table's rows. Forks share them, so the rows a
  /// mutator writes in a fork go into this branch's sources as they are.
  private let layouts: [String: RowLayout]

  convenience init(schema: ZeroSchema) {
    self.init(schema: schema, sources: [:], layouts: schema.tables.mapValues { RowLayout($0) })
  }

  private init(schema: ZeroSchema, sources: [String: MemorySource?], layouts: [String: RowLayout]) {
    self.schema = schema
    self.sources = sources
    self.layouts = layouts
  }

  func source(_ table: String) -> MemorySource? {
    if let source = sources[table] { return source }
    let source = schema.tables[table].map { MemorySource($0, layout: layouts[table]!) }
    sources[table] = .some(source)
    return source
  }

  /// A copy whose sources share rows with these until written.
  func fork() -> IVMBranch {
    IVMBranch(schema: schema, sources: sources.mapValues { $0?.fork() }, layouts: layouts)
  }

  /// Applies the entity changes of `diff` (`applyDiffs`). A run of adds to
  /// one table is pushed together. A row in `rows`, by key, is the one a
  /// transaction wrote, which is the value the diff has for it.
  func apply(_ diff: KVDiff, rows written: [String: Row] = [:]) {
    let prefix = Keys.entitiesPrefix
    var i = 0
    while i < diff.count {
      let op = diff[i]
      guard op.key.utf8.starts(with: prefix.utf8) else {
        i += 1
        continue
      }
      let name = Self.sourceName(op.key)
      guard let source = source(name) else {
        i += 1
        continue
      }
      switch op {
      case .del(_, let old):
        source.push(.remove(Row(old.objectValue ?? [:], layout: source.layout)))
        i += 1
      case .change(let key, let old, let new):
        source.push(
          .edit(
            written[key] ?? Row(new.objectValue ?? [:], layout: source.layout),
            oldRow: Row(old.objectValue ?? [:], layout: source.layout)))
        i += 1
      case .add:
        let tablePrefix = prefix + name + "/"
        var rows: [Row] = []
        while i < diff.count, case .add(let key, let value) = diff[i],
          key.utf8.starts(with: tablePrefix.utf8)
        {
          rows.append(written[key] ?? Row(value.objectValue ?? [:], layout: source.layout))
          i += 1
        }
        source.pushAdds(rows)
      }
    }
  }

  /// `sourceNameFromKey`: the table of an `e/<table>/<id>` key.
  static func sourceName(_ key: String) -> String {
    let rest = key.utf8.dropFirst(Keys.entitiesPrefix.utf8.count)
    let end = rest.firstIndex(of: UInt8(ascii: "/")) ?? rest.endIndex
    return String(rest[rest.startIndex..<end])!
  }
}
