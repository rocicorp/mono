#if canImport(SwiftUI) && (canImport(UIKit) || canImport(AppKit))
  import SwiftUI
  import Zero

  /// A scroll view of a `ZeroVirtualizer`'s rows: they page in as it
  /// scrolls, and what's on screen stays put as rows load, go, and change
  /// height.
  ///
  /// ```swift
  /// VirtualList(messages) { item in
  ///   MessageRow(message: item.row)
  /// }
  /// ```
  ///
  /// Rows can be any height, and change it. The list draws only the rows
  /// near the screen, with space standing in for the rest. A header and a
  /// footer scroll with the rows, above and below them:
  ///
  /// ```swift
  /// VirtualList(comments) { item in
  ///   CommentRow(comment: item.row)
  /// } header: {
  ///   IssueHeader(issue: issue)
  /// } footer: {
  ///   Composer(issue: issue)
  /// }
  /// ```
  ///
  /// With a `selection`, a tap selects a row, and on macOS so do keys (see
  /// `init(_:selection:row:header:footer:)`).
  ///
  /// Underneath is the platform's scroll view (`UIScrollView`,
  /// `NSScrollView`), which lays the rows out itself: their frames, the
  /// header's and the footer's, its content's size and its offset are set
  /// in one layout pass, so no frame
  /// shows rows moved by a page loading before the offset that keeps them
  /// in place. Each row (and the header and the footer) is hosted in its
  /// own hosting view, which inherits
  /// the list's environment: a row's `@ZeroQuery` reads the list's client
  /// (`.zero(_:)`), and its `@Environment` values are the list's.
  public struct VirtualList<
    Context: Sendable, Row: ZeroRow, Key: Hashable, StartRow: Equatable & Sendable,
    ListContext: Equatable & Sendable, RowContent: View
  >: View {
    public typealias Virtualizer = ZeroVirtualizer<Context, Row, Key, StartRow, ListContext>

    private let list: Virtualizer
    private let row: (VirtualItem<Row, Key>, Bool) -> RowContent
    private let selection: Binding<Key?>?
    private let header: AnyView?
    private let footer: AnyView?
    @State private var model = VirtualListModel()

    /// - Parameters:
    ///   - header: Above the rows, scrolling with them.
    ///   - footer: Below the rows, scrolling with them.
    public init<Header: View, Footer: View>(
      _ list: Virtualizer, @ViewBuilder row: @escaping (VirtualItem<Row, Key>) -> RowContent,
      @ViewBuilder header: () -> Header = { EmptyView() },
      @ViewBuilder footer: () -> Footer = { EmptyView() }
    ) {
      self.list = list
      self.row = { item, _ in row(item) }
      selection = nil
      self.header = Self.accessory(header)
      self.footer = Self.accessory(footer)
    }

    /// A list whose rows can be selected, one at a time: a tap selects a
    /// row, and on macOS, while the list has focus, the arrow keys and j
    /// and k select the next or the previous one and scroll it into view.
    ///
    /// A tap sets `selection` even to the row it is, so the app can open
    /// it again. The list highlights the selected row, as a `List` does;
    /// `row` gets whether its row is selected, for anything else it
    /// shows differently.
    ///
    /// - Parameters:
    ///   - selection: The selected row's key.
    ///   - header: Above the rows, scrolling with them.
    ///   - footer: Below the rows, scrolling with them.
    public init<Header: View, Footer: View>(
      _ list: Virtualizer, selection: Binding<Key?>,
      @ViewBuilder row: @escaping (_ item: VirtualItem<Row, Key>, _ isSelected: Bool) -> RowContent,
      @ViewBuilder header: () -> Header = { EmptyView() },
      @ViewBuilder footer: () -> Footer = { EmptyView() }
    ) {
      self.list = list
      self.row = row
      self.selection = selection
      self.header = Self.accessory(header)
      self.footer = Self.accessory(footer)
    }

    private static func accessory<V: View>(_ content: () -> V) -> AnyView? {
      V.self == EmptyView.self ? nil : AnyView(content())
    }

    public var body: some View {
      // Read here, so the list renders each snapshot.
      _ = list.snapshot
      return RowsScrollViewRepresentable(
        source: { [list] in RowsSource(list.snapshot) },
        row: { [row] in AnyView(row($0 as! VirtualItem<Row, Key>, $1)) }, header: header,
        footer: footer,
        selection: selection.map { selection in
          (
            selection.wrappedValue.map { AnyHashable($0) },
            { key in selection.wrappedValue = key.base as? Key }
          )
        }, model: model
      )
      .onAppear { model.attach(list) }
      .onDisappear { model.detach(list) }
    }
  }

  extension RowsSource {
    /// The snapshot's rows, as the scroll view draws them.
    init<Row, Key>(_ snapshot: VirtualSnapshot<Row, Key>) {
      self.init(
        items: snapshot.items.map {
          RowsItem(key: AnyHashable($0.key), index: $0.index, row: $0.row, item: $0)
        },
        spaceBefore: snapshot.spaceBefore, spaceAfter: snapshot.spaceAfter)
    }
  }

  /// Ties the virtualizer to the scroll view: attached while the list is
  /// on screen.
  @MainActor
  final class VirtualListModel {
    weak var view: RowsScrollView?
    var source: () -> RowsSource = { RowsSource() }
    private var attachAs: ((RowsScrollView) -> Void)?
    private var attached = false

    func attach<C, R, K, S, L>(_ list: ZeroVirtualizer<C, R, K, S, L>) {
      attachAs = { [unowned self] view in
        let host = view.rows.host
        host.didScroll = { [list] in list.didScroll() }
        host.viewportDidResize = { [list] in list.viewportDidResize() }
        host.afterUpdate = { [list] in list.afterUpdate() }
        host.isLanding = { [list] in list.isLanding }
        host.estimatedRowHeight = { [list] in list.estimatedRowHeight }
        view.rows.scrollToRow = { [list] key in
          if let key = key.base as? K { list.scrollToRow(key: key, align: .auto) }
        }
        list.anchoring = .manual
        // Each snapshot, in the layout pass or out of it, goes to the view
        // at once: a pass lays out what the core publishes during it.
        list.onSnapshot = { [weak view, unowned self] in view?.rows.update(source()) }
        list.attach(host)
        view.rows.update(source())
      }
      if let view { connect(view) }
    }

    /// Detaches from `list`, unless another list has taken it over since,
    /// as when one list replaces another with the same virtualizer.
    func detach<C, R, K, S, L>(_ list: ZeroVirtualizer<C, R, K, S, L>) {
      attachAs = nil
      attached = false
      guard list.detach(view?.rows.host) else { return }
      list.onSnapshot = nil
    }

    /// The view exists: attach to it if the list is on screen.
    func connect(_ view: RowsScrollView) {
      self.view = view
      guard !attached, let attachAs else { return }
      attached = true
      attachAs(view)
    }
  }
#endif
