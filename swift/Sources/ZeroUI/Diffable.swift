#if canImport(UIKit) || canImport(AppKit)
  #if canImport(UIKit)
    import UIKit
  #else
    import AppKit
  #endif

  /// Feeds a view's rows to a diffable data source: one item per row, keyed
  /// by its primary key, in the view's order. Items whose row changed are
  /// reconfigured (reloaded on AppKit).
  ///
  /// ```swift
  /// let dataSource = UITableViewDiffableDataSource<Int, RowID>(tableView: tableView) {
  ///   [unowned self] tableView, indexPath, id in
  ///   let cell = tableView.dequeueReusableCell(withIdentifier: "issue", for: indexPath)
  ///   cell.textLabel?.text = adapter.row(for: id)?.title
  ///   return cell
  /// }
  /// adapter = DiffableAdapter<Issue, Int>(try zero.materialize(IssueQueries.open()),
  ///   section: 0, dataSource: dataSource)
  /// ```
  @MainActor
  public final class DiffableAdapter<Row: ZeroRow, Section: Hashable & Sendable> {
    public typealias Snapshot = NSDiffableDataSourceSnapshot<Section, RowID>

    /// The view the adapter reads; it stays current while the adapter lives.
    public let view: MaterializedView
    public let section: Section
    /// The rows by ID. A row that can't be read as `Row` is missing.
    public private(set) var rows: [RowID: Row] = [:]
    private var removeListener: () -> Void = {}

    /// Calls `apply` with the first snapshot now, not animated, and with a
    /// snapshot for each change after, animated.
    public init(
      _ view: MaterializedView, section: Section,
      apply: @escaping (_ snapshot: Snapshot, _ animatingDifferences: Bool) -> Void
    ) {
      self.view = view
      self.section = section
      var animating = false
      removeListener = view.addSnapshotListener { [weak self] snapshot in
        guard let self else { return }
        self.update(snapshot)
        var s = Snapshot()
        s.appendSections([section])
        s.appendItems(snapshot.ids, toSection: section)
        if !snapshot.updated.isEmpty {
          #if canImport(UIKit)
            s.reconfigureItems(Array(snapshot.updated))
          #else
            // AppKit snapshots can only reload.
            s.reloadItems(Array(snapshot.updated))
          #endif
        }
        apply(s, animating)
        animating = true
      }
    }

    isolated deinit {
      removeListener()
    }

    public func row(for id: RowID) -> Row? { rows[id] }

    /// Reads the rows that are new or changed.
    private func update(_ snapshot: ViewSnapshot) {
      var changed = snapshot.updated
      for change in snapshot.difference {
        switch change {
        case .remove(_, let id, _): rows[id] = nil
        case .insert(_, let id, _): changed.insert(id)
        }
      }
      for (id, entry) in zip(snapshot.ids, snapshot.data.entries)
      where changed.contains(id) || rows[id] == nil {
        rows[id] = try? Row(entry: entry)
      }
    }
  }

  #if canImport(UIKit)
    extension DiffableAdapter {
      public convenience init(
        _ view: MaterializedView, section: Section,
        dataSource: UITableViewDiffableDataSource<Section, RowID>
      ) {
        self.init(view, section: section) { [weak dataSource] snapshot, animating in
          dataSource?.apply(snapshot, animatingDifferences: animating)
        }
      }

      public convenience init(
        _ view: MaterializedView, section: Section,
        dataSource: UICollectionViewDiffableDataSource<Section, RowID>
      ) {
        self.init(view, section: section) { [weak dataSource] snapshot, animating in
          dataSource?.apply(snapshot, animatingDifferences: animating)
        }
      }
    }
  #else
    extension DiffableAdapter {
      public convenience init(
        _ view: MaterializedView, section: Section,
        dataSource: NSTableViewDiffableDataSource<Section, RowID>
      ) {
        self.init(view, section: section) { [weak dataSource] snapshot, animating in
          dataSource?.apply(snapshot, animatingDifferences: animating)
        }
      }

      public convenience init(
        _ view: MaterializedView, section: Section,
        dataSource: NSCollectionViewDiffableDataSource<Section, RowID>
      ) {
        self.init(view, section: section) { [weak dataSource] snapshot, animating in
          dataSource?.apply(snapshot, animatingDifferences: animating)
        }
      }
    }
  #endif
#endif
