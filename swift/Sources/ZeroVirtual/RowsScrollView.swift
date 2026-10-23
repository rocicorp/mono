#if canImport(SwiftUI) && (canImport(UIKit) || canImport(AppKit))
  import SwiftUI

  #if canImport(UIKit)
    import UIKit
  #else
    import AppKit
  #endif

  #if canImport(UIKit)
    typealias PlatformView = UIView
  #else
    typealias PlatformView = NSView
  #endif

  /// A row to draw: its key and index, the row (to tell a changed row from
  /// the same one), and the snapshot's item, for the row's view.
  struct RowsItem {
    var key: AnyHashable
    var index: Int
    var row: Any
    var item: Any
  }

  /// The rows the view draws: the snapshot's.
  struct RowsSource {
    var items: [RowsItem] = []
    var spaceBefore = 0.0
    var spaceAfter = 0.0
  }

  /// How a row is shown: selected, and whether its highlight is
  /// emphasized, as while the list has focus on macOS.
  struct RowState: Equatable {
    var selected = false
    var emphasized = false
  }

  /// A view above or below the rows, laid out with them: a header or a
  /// footer.
  @MainActor
  final class RowsAccessory {
    let view: PlatformView
    /// Its height at a width.
    let measure: (Double) -> Double
    var height: Double?

    init(view: PlatformView, measure: @escaping (Double) -> Double) {
      self.view = view
      self.measure = measure
    }
  }

  /// How a row's view is made and measured: hosted SwiftUI
  /// (`HostedRowViews`), or the app's own (`AppRowViews`). The rest of the
  /// layout is the same for both.
  @MainActor
  protocol RowViews: AnyObject {
    /// Views of rows that leave are kept, hidden, for rows that come.
    var reusesViews: Bool { get }
    /// The view showing `item` in `state`: `view` shown anew (the row's
    /// own when its row or state changed, or, when views are reused, one a
    /// row left), or a new one when it's nil.
    func view(
      for item: RowsItem, _ state: RowState, reusing view: PlatformView?, in layout: RowsLayout
    )
      -> PlatformView
    /// The view's height at `width`.
    func height(of view: PlatformView, width: Double) -> Double
    /// The view's row left and the view isn't reused.
    func discard(_ view: PlatformView)
  }

  /// A row on screen: its view, its measured height, and the row it shows.
  @MainActor
  final class RowCell {
    var view: PlatformView
    var height: Double?
    var row: Any?

    init(view: PlatformView) { self.view = view }
  }

  /// What the platform scroll views share: the rows' cells, and the layout
  /// host that places them.
  @MainActor
  final class RowsLayout {
    let host = LayoutHost()
    let views: any RowViews
    var source = RowsSource()
    private var cells: [AnyHashable: RowCell] = [:]
    /// Views rows left, hidden, for other rows (when `views` reuses them).
    private var pool: [PlatformView] = []
    private var width = 0.0
    /// Changes with each snapshot: rows are compared with their cells' then.
    private var generation = 0
    private var seen: [AnyHashable: Int] = [:]
    /// The list has drawn rows. Until then, it draws none while it opens at
    /// a jump: its first frame is where the jump lands, not where it starts.
    private(set) var shown = false
    var addView: (PlatformView) -> Void = { _ in }

    /// The views above and below the rows.
    var header: RowsAccessory? {
      didSet { replace(oldValue, with: header) }
    }
    var footer: RowsAccessory? {
      didSet { replace(oldValue, with: footer) }
    }

    /// A tap or a key selects a row.
    var selectable = false
    /// The selected row, which the list highlights.
    var selected: AnyHashable? {
      didSet {
        guard selected != oldValue else { return }
        reshow(oldValue)
        reshow(selected)
      }
    }
    /// The highlight is emphasized: the list has focus.
    var emphasized = false {
      didSet {
        guard emphasized != oldValue else { return }
        reshow(selected)
      }
    }
    /// The user selected a row, with a tap or a key: also the selected one.
    var onSelect: (AnyHashable) -> Void = { _ in }
    /// Scrolls a loaded row into view.
    var scrollToRow: (AnyHashable) -> Void = { _ in }

    init(views: any RowViews) {
      self.views = views
      host.rows = { [unowned self] in
        (source.items.map { (key: $0.key, index: $0.index) }, source.spaceBefore, source.spaceAfter)
      }
      host.measure = { [unowned self] key in measure(key) }
      host.header = { [unowned self] in height(of: header) }
      host.footer = { [unowned self] in height(of: footer) }
    }

    /// A new snapshot to draw.
    func update(_ source: RowsSource) {
      self.source = source
      generation += 1
      host.invalidate()
    }

    /// The row with `key` changed height on its own: it's measured again in
    /// the next layout pass.
    func rowHeightDidChange(_ key: AnyHashable) {
      guard let cell = cells[key] else { return }
      cell.height = nil
      host.invalidate()
    }

    /// The header or the footer changed height: it's measured again in the
    /// next layout pass.
    func accessoryHeightDidChange(_ view: PlatformView) {
      for accessory in [header, footer] where accessory?.view === view {
        accessory?.height = nil
        host.invalidate()
      }
    }

    /// The header's or the footer's height as it laid itself out: when it's
    /// another than the one measured, it's measured again.
    func accessory(_ view: PlatformView, laidOutAt height: Double) {
      for accessory in [header, footer] where accessory?.view === view {
        guard let old = accessory?.height, abs(old - height) > 0.5 else { return }
        accessoryHeightDidChange(view)
      }
    }

    /// One layout pass at `width`: the cells to place, the content's height
    /// and the offset; the header's and the footer's frames, and the
    /// selected row's.
    func layout(
      width: Double, offset: Double, viewport: Double
    ) -> (
      frames: [(cell: RowCell, minY: Double, maxY: Double)], contentHeight: Double, offset: Double,
      accessories: [(view: PlatformView, minY: Double, maxY: Double)],
      selected: (minY: Double, maxY: Double)?
    ) {
      if width != self.width {
        self.width = width
        for cell in cells.values { cell.height = nil }
        header?.height = nil
        footer?.height = nil
        host.invalidateHeights()
      }
      let r = host.layout(offset: offset, viewport: viewport)
      if !shown, !r.frames.isEmpty, !host.isLanding() { shown = true }
      let live = Set(r.frames.map(\.key))
      for (key, cell) in cells where !live.contains(key) {
        cells[key] = nil
        seen[key] = nil
        if views.reusesViews {
          cell.view.isHidden = true
          pool.append(cell.view)
        } else {
          cell.view.removeFromSuperview()
          views.discard(cell.view)
        }
      }
      var accessories: [(view: PlatformView, minY: Double, maxY: Double)] = []
      if let header { accessories.append((header.view, 0, host.headerHeight)) }
      if let footer {
        accessories.append((footer.view, r.contentHeight - host.footerHeight, r.contentHeight))
      }
      let selected = selected.flatMap { key in r.frames.first { $0.key == key } }
      return (
        r.frames.compactMap { f in cells[f.key].map { ($0, f.minY, f.maxY) } }, r.contentHeight,
        r.offset, accessories, selected.map { ($0.minY, $0.maxY) }
      )
    }

    /// The header and the footer are hidden while the rows are: until the
    /// list has landed where it opens.
    var accessoriesHidden: Bool { !shown && host.isLanding() }

    // MARK: Selection

    /// The user selected the row with `key`.
    func userSelect(_ key: AnyHashable) {
      selected = key
      onSelect(key)
    }

    /// The row a tap at `y`, in content coordinates, selects.
    func key(at y: Double) -> AnyHashable? {
      host.rowFrames().first { $0.minY <= y && y < $0.maxY }?.key
    }

    /// Selects the row `step` rows from the selected one, or the first on
    /// screen when there's none, and scrolls it into view.
    func moveSelection(by step: Int) {
      let items = source.items
      let next: AnyHashable
      if let selected, let i = items.firstIndex(where: { $0.key == selected }) {
        guard items.indices.contains(i + step) else { return }
        next = items[i + step].key
      } else {
        let top = host.scrollOffset
        guard let first = host.rowFrames().first(where: { $0.maxY > top + 0.5 }) else { return }
        next = first.key
      }
      userSelect(next)
      scrollToRow(next)
    }

    private func state(_ key: AnyHashable) -> RowState {
      let selected = key == selected
      return RowState(selected: selected, emphasized: selected && emphasized)
    }

    /// The row with `key` is shown again, in its state now.
    private func reshow(_ key: AnyHashable?) {
      guard let key, let cell = cells[key], let item = source.items.first(where: { $0.key == key })
      else { return }
      show(item, in: cell)
      host.invalidate()
    }

    private func replace(_ old: RowsAccessory?, with new: RowsAccessory?) {
      guard old !== new else { return }
      if let old {
        old.view.removeFromSuperview()
        views.discard(old.view)
      }
      if let new { addView(new.view) }
      host.invalidate()
    }

    private func height(of accessory: RowsAccessory?) -> Double {
      guard let accessory else { return 0 }
      if let height = accessory.height { return height }
      let h = accessory.measure(width)
      accessory.height = h
      return h
    }

    private func measure(_ key: AnyHashable) -> Double {
      guard let item = source.items.first(where: { $0.key == key }) else { return 0 }
      if let cell = cells[key] {
        if seen[key] != generation {
          seen[key] = generation
          // The same row: its view and height stand.
          if let old = cell.row, !sameRow(old, item.row) {
            show(item, in: cell)
          }
        }
        return measure(cell)
      }
      let reused = views.reusesViews ? pool.popLast() : nil
      let view = views.view(for: item, state(key), reusing: reused, in: self)
      if view !== reused {
        if let reused { pool.append(reused) }
        addView(view)
      }
      let cell = RowCell(view: view)
      cell.row = item.row
      cells[key] = cell
      seen[key] = generation
      return measure(cell)
    }

    private func show(_ item: RowsItem, in cell: RowCell) {
      cell.row = item.row
      cell.height = nil
      let view = views.view(for: item, state(item.key), reusing: cell.view, in: self)
      guard view !== cell.view else { return }
      // A new view in place of the row's own.
      cell.view.removeFromSuperview()
      views.discard(cell.view)
      cell.view = view
      addView(view)
    }

    private func measure(_ cell: RowCell) -> Double {
      if let height = cell.height { return height }
      let h = views.height(of: cell.view, width: width)
      cell.height = h
      return h
    }

    /// The row's height as it laid itself out: when it's another than the
    /// one measured, it's measured again.
    func row(_ key: AnyHashable, laidOutAt height: Double) {
      guard let old = cells[key]?.height, abs(old - height) > 0.5 else { return }
      rowHeightDidChange(key)
    }
  }

  /// Rows of an `Equatable` type that compare equal are the same row.
  private func sameRow(_ a: Any, _ b: Any) -> Bool {
    guard let a = a as? any Equatable else { return false }
    return a.isEqual(to: b)
  }

  extension Equatable {
    fileprivate func isEqual(to other: Any) -> Bool { (other as? Self) == self }
  }

  /// SwiftUI rows, each in its own hosting controller. A row's controller
  /// isn't reused for another row: it would show the row it had until its
  /// next layout.
  @MainActor
  final class HostedRowViews: RowViews {
    #if canImport(UIKit)
      typealias Controller = UIHostingController<AnyView>
    #else
      typealias Controller = NSHostingController<AnyView>
    #endif
    var row: (Any, Bool) -> AnyView
    private var controllers: [ObjectIdentifier: Controller] = [:]

    init(row: @escaping (Any, Bool) -> AnyView) { self.row = row }

    var reusesViews: Bool { false }

    func view(
      for item: RowsItem, _ state: RowState, reusing view: PlatformView?, in layout: RowsLayout
    )
      -> PlatformView
    {
      let controller = view.flatMap { controllers[ObjectIdentifier($0)] } ?? makeController()
      let key = item.key
      // Text in the system's styles takes the highlight's colors, as in a
      // `List`.
      let content = row(item.item, state.selected)
        .environment(\.backgroundProminence, state.emphasized ? .increased : .standard)
      let shown =
        layout.selectable
        ? AnyView(
          content
            .accessibilityAddTraits(state.selected ? [.isButton, .isSelected] : .isButton)
            .accessibilityAction { [weak layout] in layout?.userSelect(key) })
        : AnyView(content)
      controller.rootView = AnyView(
        shown
          .fixedSize(horizontal: false, vertical: true)
          .onGeometryChange(for: Double.self) {
            Double($0.size.height)
          } action: { [weak layout] h in
            // The row changed height on its own, as a view inside it loaded.
            layout?.row(key, laidOutAt: h)
          })
      return controller.view
    }

    /// A header or a footer showing `content`: `current` showing it anew,
    /// or a new one.
    func accessory(
      _ current: RowsAccessory?, showing content: AnyView, in layout: RowsLayout
    ) -> RowsAccessory {
      let controller =
        current.flatMap { controllers[ObjectIdentifier($0.view)] } ?? makeController()
      let view: PlatformView = controller.view
      controller.rootView = AnyView(
        content
          .fixedSize(horizontal: false, vertical: true)
          .onGeometryChange(for: Double.self) {
            Double($0.size.height)
          } action: { [weak layout, weak view] h in
            if let view { layout?.accessory(view, laidOutAt: h) }
          })
      if let current, current.view === view {
        // Measured with its new content in the next pass, before it's
        // drawn: what it shows can be another height.
        current.height = nil
        return current
      }
      return RowsAccessory(view: view) { [unowned self, unowned view] width in
        height(of: view, width: width)
      }
    }

    private func makeController() -> Controller {
      let controller = Controller(rootView: AnyView(EmptyView()))
      #if canImport(UIKit)
        controller.safeAreaRegions = []
        controller.view.backgroundColor = .clear
      #else
        controller.sizingOptions = []
      #endif
      controllers[ObjectIdentifier(controller.view)] = controller
      return controller
    }

    func height(of view: PlatformView, width: Double) -> Double {
      guard let controller = controllers[ObjectIdentifier(view)] else { return 0 }
      return Double(
        controller.sizeThatFits(in: CGSize(width: width, height: .greatestFiniteMagnitude)).height)
    }

    func discard(_ view: PlatformView) {
      controllers[ObjectIdentifier(view)] = nil
    }
  }

  /// The app's own row views, measured with Auto Layout at the list's
  /// width, and reused.
  @MainActor
  final class AppRowViews: RowViews {
    let make: (Any, Bool, PlatformView?) -> PlatformView

    init(make: @escaping (Any, Bool, PlatformView?) -> PlatformView) { self.make = make }

    var reusesViews: Bool { true }

    func view(
      for item: RowsItem, _ state: RowState, reusing view: PlatformView?, in layout: RowsLayout
    )
      -> PlatformView
    {
      let view = make(item.item, state.selected, view)
      #if canImport(UIKit)
        if state.selected {
          view.accessibilityTraits.insert(.selected)
        } else {
          view.accessibilityTraits.remove(.selected)
        }
      #else
        view.setAccessibilitySelected(state.selected)
        // A table cell view draws its text and images in the highlight's
        // colors, as in a table.
        (view as? NSTableCellView)?.backgroundStyle = state.emphasized ? .emphasized : .normal
      #endif
      return view
    }

    func height(of view: PlatformView, width: Double) -> Double {
      Self.fittingHeight(of: view, width: width)
    }

    /// The view's height with Auto Layout at `width`.
    static func fittingHeight(of view: PlatformView, width: Double) -> Double {
      #if canImport(UIKit)
        let size = view.systemLayoutSizeFitting(
          CGSize(width: width, height: UIView.layoutFittingCompressedSize.height),
          withHorizontalFittingPriority: .required, verticalFittingPriority: .fittingSizeLevel)
        return Double(size.height)
      #else
        let constraint = view.widthAnchor.constraint(equalToConstant: width)
        constraint.isActive = true
        defer { constraint.isActive = false }
        // Laid out at the width first: a wrapping label wraps at the width
        // it was laid out at, and its height follows. A new view, 0 high, at
        // its fitting height: resized from 0 high where the list's offset is
        // between pixels, as when it's measured mid-scroll, its autoresizing
        // subviews come out that fraction of a point above it, which a 1x
        // display rounds to a whole one, for the view's life.
        let height = view.frame.height > 0 ? view.frame.height : view.fittingSize.height
        view.setFrameSize(NSSize(width: width, height: height))
        view.layoutSubtreeIfNeeded()
        return Double(view.fittingSize.height)
      #endif
    }

    func discard(_ view: PlatformView) {}
  }

  #if canImport(UIKit)
    /// A scroll view that lays out the rows itself: their frames, the
    /// header's and the footer's, its content size and its offset, all in
    /// `layoutSubviews`.
    final class RowsScrollView: UIScrollView, UIGestureRecognizerDelegate {
      let rows: RowsLayout
      private var laying = false
      /// Behind the selected row.
      let highlight = UIView()
      private lazy var tap = UITapGestureRecognizer(target: self, action: #selector(tapped))

      init(views: any RowViews) {
        rows = RowsLayout(views: views)
        super.init(frame: .zero)
        alwaysBounceVertical = true
        highlight.isUserInteractionEnabled = false
        highlight.isHidden = true
        addSubview(highlight)
        styleHighlight()
        tap.cancelsTouchesInView = false
        tap.delegate = self
        rows.host.setNeedsLayout = { [weak self] in self?.setNeedsLayout() }
        rows.addView = { [weak self] view in self?.addSubview(view) }
      }

      required init?(coder: NSCoder) { fatalError("init(coder:) is not supported") }

      /// A tap selects a row.
      var selectable: Bool {
        get { rows.selectable }
        set {
          rows.selectable = newValue
          if newValue {
            if tap.view == nil { addGestureRecognizer(tap) }
          } else {
            removeGestureRecognizer(tap)
          }
        }
      }

      override func layoutSubviews() {
        super.layoutSubviews()
        // Applying the offset below lays the view out again.
        guard !laying else { return }
        laying = true
        defer { laying = false }
        let inset = adjustedContentInset
        let width = Double(bounds.width - inset.left - inset.right)
        let r = rows.layout(
          width: width, offset: Double(contentOffset.y + inset.top),
          viewport: Double(bounds.height - inset.top - inset.bottom))
        let scale = max(1, window?.screen.scale ?? traitCollection.displayScale)
        func align(_ y: Double) -> CGFloat { (CGFloat(y) * scale).rounded() / scale }
        func frame(_ minY: Double, _ maxY: Double) -> CGRect {
          CGRect(x: 0, y: align(minY), width: width, height: align(maxY) - align(minY))
        }
        for f in r.frames {
          place(f.cell.view, frame(f.minY, f.maxY), hidden: !rows.shown)
        }
        for a in r.accessories {
          place(a.view, frame(a.minY, a.maxY), hidden: rows.accessoriesHidden)
        }
        if let s = r.selected, rows.shown {
          highlight.frame = frame(s.minY, s.maxY)
          highlight.isHidden = false
        } else {
          highlight.isHidden = true
        }
        let size = CGSize(width: width, height: r.contentHeight)
        if contentSize != size { contentSize = size }
        let y = CGFloat(r.offset) - inset.top
        if contentOffset.y != y { contentOffset.y = y }
      }

      private func place(_ view: UIView, _ frame: CGRect, hidden: Bool) {
        view.isHidden = hidden
        // Set only when it moves: a frame set lays the view out again.
        guard view.frame != frame else { return }
        view.frame = frame
        view.layoutIfNeeded()
      }

      // MARK: Selection

      /// A selected list cell's background, as a plain list draws it.
      private func styleHighlight() {
        var state = UICellConfigurationState(traitCollection: traitCollection)
        state.isSelected = true
        let background = UIBackgroundConfiguration.listCell().updated(for: state)
        highlight.backgroundColor = background.resolvedBackgroundColor(for: tintColor)
        highlight.layer.cornerRadius = background.cornerRadius
      }

      override func tintColorDidChange() {
        super.tintColorDidChange()
        styleHighlight()
      }

      @objc private func tapped() { select(at: Double(tap.location(in: self).y)) }

      /// A tap at `y`, in content coordinates: the row there is selected.
      func select(at y: Double) {
        guard let key = rows.key(at: y) else { return }
        rows.userSelect(key)
      }

      // The rows' own controls take their taps too.
      func gestureRecognizer(
        _ gestureRecognizer: UIGestureRecognizer,
        shouldRecognizeSimultaneouslyWith other: UIGestureRecognizer
      ) -> Bool { true }
    }
  #else
    /// A scroll view that lays out the rows itself: their frames, the
    /// header's and the footer's, its content's size and its offset, all in
    /// one pass. The pass runs when the clip view moves or resizes (the
    /// user's scroll, before it's drawn) and in the document view's
    /// `layout` (a new snapshot).
    final class RowsScrollView: NSScrollView {
      let rows: RowsLayout
      let document = RowsDocumentView()
      private var laying = false
      /// Behind the selected row.
      let highlight = RowsHighlightView()
      private lazy var click = NSClickGestureRecognizer(target: self, action: #selector(clicked))

      init(views: any RowViews) {
        rows = RowsLayout(views: views)
        super.init(frame: .zero)
        hasVerticalScroller = true
        drawsBackground = false
        documentView = document
        document.owner = self
        highlight.isHidden = true
        document.addSubview(highlight)
        click.delaysPrimaryMouseButtonEvents = false
        contentView.postsBoundsChangedNotifications = true
        contentView.postsFrameChangedNotifications = true
        for name in [NSView.boundsDidChangeNotification, NSView.frameDidChangeNotification] {
          NotificationCenter.default.addObserver(
            self, selector: #selector(clipChanged), name: name, object: contentView)
        }
        rows.host.setNeedsLayout = { [weak self] in self?.document.needsLayout = true }
        rows.addView = { [weak self] view in self?.document.addSubview(view) }
      }

      required init?(coder: NSCoder) { fatalError("init(coder:) is not supported") }

      /// A click or a key selects a row.
      var selectable: Bool {
        get { rows.selectable }
        set {
          rows.selectable = newValue
          if newValue {
            if click.view == nil { document.addGestureRecognizer(click) }
          } else {
            document.removeGestureRecognizer(click)
          }
        }
      }

      @objc private func clipChanged() { layoutRows() }

      fileprivate func layoutRows() {
        // Applying the size and offset below moves the clip view again.
        guard !laying, window != nil else { return }
        laying = true
        defer { laying = false }
        let clip = contentView
        let inset = clip.contentInsets
        let width = Double(clip.bounds.width)
        let r = rows.layout(
          width: width, offset: Double(clip.bounds.origin.y + inset.top),
          viewport: Double(clip.bounds.height - inset.top - inset.bottom))
        // On whole pixels of the content, not of the window: aligned in the
        // window, a row would snap differently at each offset and move by a
        // pixel against the content as it scrolls.
        let scale = max(1, window?.backingScaleFactor ?? 1)
        func align(_ y: Double) -> CGFloat { (CGFloat(y) * scale).rounded() / scale }
        func frame(_ minY: Double, _ maxY: Double) -> CGRect {
          CGRect(x: 0, y: align(minY), width: width, height: align(maxY) - align(minY))
        }
        for f in r.frames {
          place(f.cell.view, frame(f.minY, f.maxY), hidden: !rows.shown)
        }
        for a in r.accessories {
          place(a.view, frame(a.minY, a.maxY), hidden: rows.accessoriesHidden)
        }
        if let s = r.selected, rows.shown {
          // Inset, with rounded corners, as an inset table draws it.
          highlight.frame = frame(s.minY, s.maxY).insetBy(dx: 8, dy: 0)
          highlight.isHidden = false
        } else {
          highlight.isHidden = true
        }
        let size = NSSize(width: width, height: r.contentHeight)
        if document.frame.size != size { document.setFrameSize(size) }
        let y = CGFloat(r.offset) - inset.top
        if clip.bounds.origin.y != y {
          clip.scroll(to: NSPoint(x: clip.bounds.origin.x, y: y))
          reflectScrolledClipView(clip)
        }
      }

      private func place(_ view: NSView, _ frame: CGRect, hidden: Bool) {
        view.isHidden = hidden
        // Set only when it moves: a frame set lays the view out again.
        guard view.frame != frame else { return }
        view.frame = frame
        view.layoutSubtreeIfNeeded()
      }

      // MARK: Selection

      @objc private func clicked() { select(at: Double(click.location(in: document).y)) }

      /// A click at `y`, in content coordinates: the list takes focus, and
      /// the row there is selected.
      func select(at y: Double) {
        guard let key = rows.key(at: y) else { return }
        window?.makeFirstResponder(document)
        rows.userSelect(key)
      }

      override func viewWillMove(toWindow newWindow: NSWindow?) {
        super.viewWillMove(toWindow: newWindow)
        let center = NotificationCenter.default
        for name in [NSWindow.didBecomeKeyNotification, NSWindow.didResignKeyNotification] {
          center.removeObserver(self, name: name, object: window)
          if let newWindow {
            center.addObserver(
              self, selector: #selector(focusChanged), name: name, object: newWindow)
          }
        }
      }

      /// The highlight is emphasized while the list has focus.
      @objc fileprivate func focusChanged() {
        let focused = window.map { $0.isKeyWindow && $0.firstResponder === document } ?? false
        rows.emphasized = focused
        highlight.emphasized = focused
      }
    }

    final class RowsDocumentView: NSView {
      weak var owner: RowsScrollView?

      override var isFlipped: Bool { true }

      override func layout() {
        super.layout()
        owner?.layoutRows()
      }

      override var acceptsFirstResponder: Bool { owner?.rows.selectable ?? false }

      override func becomeFirstResponder() -> Bool {
        defer { owner?.focusChanged() }
        return super.becomeFirstResponder()
      }

      override func resignFirstResponder() -> Bool {
        defer { owner?.focusChanged() }
        return super.resignFirstResponder()
      }

      /// The arrow keys, and j and k, move the selection.
      override func keyDown(with event: NSEvent) {
        guard let rows = owner?.rows, rows.selectable,
          event.modifierFlags.isDisjoint(with: [.command, .control, .option])
        else { return super.keyDown(with: event) }
        switch (event.keyCode, event.charactersIgnoringModifiers) {
        case (125, _), (_, "j"): rows.moveSelection(by: 1)
        case (126, _), (_, "k"): rows.moveSelection(by: -1)
        default: super.keyDown(with: event)
        }
      }
    }

    /// The selected row's background: the accent color while the list has
    /// focus, gray when it hasn't, as a table draws it.
    final class RowsHighlightView: NSView {
      var emphasized = false {
        didSet { needsDisplay = true }
      }

      override init(frame: NSRect) {
        super.init(frame: frame)
        wantsLayer = true
      }

      required init?(coder: NSCoder) { fatalError("init(coder:) is not supported") }

      override var wantsUpdateLayer: Bool { true }

      override func updateLayer() {
        let color: NSColor =
          emphasized ? .selectedContentBackgroundColor : .unemphasizedSelectedContentBackgroundColor
        layer?.backgroundColor = color.cgColor
        layer?.cornerRadius = 5
      }

      override func hitTest(_ point: NSPoint) -> NSView? { nil }
    }
  #endif

  /// `VirtualList`'s scroll view.
  @MainActor
  struct RowsScrollViewRepresentable {
    let source: () -> RowsSource
    let row: (Any, Bool) -> AnyView
    let header: AnyView?
    let footer: AnyView?
    /// The selected row, and what a tap or a key sets it to; nil when rows
    /// aren't selectable.
    let selection: (key: AnyHashable?, set: (AnyHashable) -> Void)?
    let model: VirtualListModel

    func make() -> RowsScrollView {
      let view = RowsScrollView(views: HostedRowViews(row: row))
      model.source = source
      model.connect(view)
      update(view)
      return view
    }

    func update(_ view: RowsScrollView) {
      model.source = source
      let rows = view.rows
      let views = rows.views as! HostedRowViews
      views.row = row
      rows.header = header.map { views.accessory(rows.header, showing: $0, in: rows) }
      rows.footer = footer.map { views.accessory(rows.footer, showing: $0, in: rows) }
      view.selectable = selection != nil
      rows.onSelect = selection?.set ?? { _ in }
      rows.selected = selection?.key
      rows.update(source())
    }
  }

  #if canImport(UIKit)
    extension RowsScrollViewRepresentable: UIViewRepresentable {
      func makeUIView(context: Context) -> RowsScrollView { make() }
      func updateUIView(_ view: RowsScrollView, context: Context) { update(view) }
    }
  #else
    extension RowsScrollViewRepresentable: NSViewRepresentable {
      func makeNSView(context: Context) -> RowsScrollView { make() }
      func updateNSView(_ view: RowsScrollView, context: Context) { update(view) }
    }
  #endif
#endif
