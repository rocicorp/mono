#if canImport(SwiftUI) && (canImport(UIKit) || canImport(AppKit))
  #if canImport(UIKit)
    import UIKit
  #else
    import AppKit
  #endif
  import Zero

  #if canImport(UIKit)
    /// What `VirtualListView` is: a `UIView`.
    public typealias VirtualListViewBase = UIView
  #else
    /// What `VirtualListView` is: an `NSView`.
    public typealias VirtualListViewBase = NSView
  #endif

  /// `VirtualList` for UIKit and AppKit: a scroll view of a
  /// `ZeroVirtualizer`'s rows, which are the app's own views. They page in
  /// as it scrolls, and what's on screen stays put as rows load, go, and
  /// change height.
  ///
  /// ```swift
  /// let list = VirtualListView(messages) { item, reusing in
  ///   let row = reusing as? MessageRowView ?? MessageRowView()
  ///   row.show(item.row)
  ///   return row
  /// }
  /// ```
  ///
  /// `row` makes the view of a row, or shows the row in `reusing`, a view
  /// another row left; it's nil when there's none. A row whose data
  /// changes (rows of an `Equatable` type that no longer compare equal) is
  /// shown again in its own view. A row's height is its view's, laid out
  /// with Auto Layout at the list's width: its constraints must determine
  /// it. A row that changes height on its own, not with its data, says so
  /// with `rowHeightDidChange(_:)`.
  ///
  /// The list lays the rows out in its `scrollView` itself: their frames,
  /// the content's size and the offset are set in one layout pass, so no
  /// frame shows rows moved by a page loading before the offset that keeps
  /// them in place. Set the scroll view's insets and delegate as for any
  /// other, but not its content size or offset; scroll with the
  /// virtualizer (`scrollToItem`). The list follows the virtualizer while
  /// it's in a window.
  ///
  /// A `headerView` and a `footerView` scroll with the rows, above and below
  /// them. With an `onSelect`, a tap selects a row, and on macOS so do
  /// keys; `row` then takes whether its row is selected:
  ///
  /// ```swift
  /// let list = VirtualListView(issues) { item, isSelected, reusing in
  ///   let row = reusing as? IssueRowView ?? IssueRowView()
  ///   row.show(item.row, selected: isSelected)
  ///   return row
  /// }
  /// list.onSelect = { id in open(id) }
  /// ```
  public final class VirtualListView<
    Context: Sendable, Row: ZeroRow, Key: Hashable, StartRow: Equatable & Sendable,
    ListContext: Equatable & Sendable
  >: VirtualListViewBase {
    public typealias Virtualizer = ZeroVirtualizer<Context, Row, Key, StartRow, ListContext>

    private let list: Virtualizer
    private let rows: RowsScrollView
    private let model = VirtualListModel()

    #if canImport(UIKit)
      /// The scroll view the rows are in.
      public var scrollView: UIScrollView { rows }
    #else
      /// The scroll view the rows are in.
      public var scrollView: NSScrollView { rows }
    #endif

    #if canImport(UIKit)
      public convenience init(
        _ list: Virtualizer,
        row: @escaping @MainActor (_ item: VirtualItem<Row, Key>, _ reusing: UIView?) -> UIView
      ) {
        self.init(list, make: { item, _, reusing in row(item, reusing) })
      }

      /// With whether the row is selected, for a list whose rows can be
      /// (`onSelect`).
      public convenience init(
        _ list: Virtualizer,
        row:
          @escaping @MainActor (
            _ item: VirtualItem<Row, Key>, _ isSelected: Bool, _ reusing: UIView?
          ) -> UIView
      ) {
        self.init(list, make: row)
      }
    #else
      public convenience init(
        _ list: Virtualizer,
        row: @escaping @MainActor (_ item: VirtualItem<Row, Key>, _ reusing: NSView?) -> NSView
      ) {
        self.init(list, make: { item, _, reusing in row(item, reusing) })
      }

      /// With whether the row is selected, for a list whose rows can be
      /// (`onSelect`).
      public convenience init(
        _ list: Virtualizer,
        row:
          @escaping @MainActor (
            _ item: VirtualItem<Row, Key>, _ isSelected: Bool, _ reusing: NSView?
          ) -> NSView
      ) {
        self.init(list, make: row)
      }
    #endif

    private init(
      _ list: Virtualizer,
      make row: @escaping @MainActor (VirtualItem<Row, Key>, Bool, PlatformView?) -> PlatformView
    ) {
      self.list = list
      rows = RowsScrollView(
        views: AppRowViews { item, selected, reusing in
          row(item as! VirtualItem<Row, Key>, selected, reusing)
        })
      super.init(frame: .zero)
      #if canImport(UIKit)
        rows.autoresizingMask = [.flexibleWidth, .flexibleHeight]
      #else
        rows.autoresizingMask = [.width, .height]
      #endif
      rows.frame = bounds
      addSubview(rows)
      model.source = { [list] in RowsSource(list.snapshot) }
      model.connect(rows)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError("init(coder:) is not supported") }

    /// The row with `key` changed height on its own: its view is measured
    /// again, and the rows below it move, with what's on screen staying
    /// put.
    public func rowHeightDidChange(_ key: Key) {
      rows.rows.rowHeightDidChange(AnyHashable(key))
    }

    // MARK: Header and footer

    #if canImport(UIKit)
      /// A view above the rows, scrolling with them. Its height is its
      /// Auto Layout height at the list's width, like a row's.
      public var headerView: UIView? {
        didSet { rows.rows.header = headerView.map(Self.accessory) }
      }
      /// A view below the rows, scrolling with them.
      public var footerView: UIView? {
        didSet { rows.rows.footer = footerView.map(Self.accessory) }
      }
    #else
      /// A view above the rows, scrolling with them. Its height is its
      /// Auto Layout height at the list's width, like a row's.
      public var headerView: NSView? {
        didSet { rows.rows.header = headerView.map(Self.accessory) }
      }
      /// A view below the rows, scrolling with them.
      public var footerView: NSView? {
        didSet { rows.rows.footer = footerView.map(Self.accessory) }
      }
    #endif

    /// The header or the footer changed height on its own: it's measured
    /// again, and the rows move with it, what's on screen staying put
    /// unless the header is on screen.
    public func headerOrFooterHeightDidChange() {
      for view in [headerView, footerView] {
        if let view { rows.rows.accessoryHeightDidChange(view) }
      }
    }

    private static func accessory(_ view: PlatformView) -> RowsAccessory {
      RowsAccessory(view: view) { [unowned view] width in
        AppRowViews.fittingHeight(of: view, width: width)
      }
    }

    // MARK: Selection

    /// The selected row's key: the list highlights its row. Setting it
    /// doesn't call `onSelect`.
    public var selection: Key? {
      get { rows.rows.selected?.base as? Key }
      set { rows.rows.selected = newValue.map { AnyHashable($0) } }
    }

    /// Called with a row the user selects: a tap selects a row (even the
    /// selected one), and on macOS, while the list has focus, the arrow
    /// keys and j and k select the next or the previous one and scroll it
    /// into view.
    /// Rows can be selected while it's set.
    ///
    /// The list highlights the selected row with the platform's selection
    /// color, and marks its view selected for accessibility; on macOS, an
    /// `NSTableCellView` row draws its text in the highlight's colors.
    public var onSelect: ((Key) -> Void)? {
      didSet {
        rows.selectable = onSelect != nil
        rows.rows.onSelect = { [weak self] key in
          if let key = key.base as? Key { self?.onSelect?(key) }
        }
      }
    }

    #if canImport(UIKit)
      public override func didMoveToWindow() {
        super.didMoveToWindow()
        follow(window != nil)
      }
    #else
      public override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        follow(window != nil)
      }
    #endif

    private func follow(_ inWindow: Bool) {
      if inWindow { model.attach(list) } else { model.detach(list) }
    }
  }
#endif
