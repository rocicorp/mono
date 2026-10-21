/// A `VirtualHost` for a scroll view that lays its rows out itself, in one
/// layout pass: the rows' frames, the content's height and the offset are
/// worked out together, and the view applies them before it draws. The core
/// runs inside the pass: what it does about a pass (a scroll that keeps the
/// rows in place, a new snapshot) is in the same pass, so no frame shows the
/// content moved and the offset not yet.
///
/// The view calls `layout(offset:viewport:)` from its layout pass, with the
/// offset it's at (the user's scroll, since the last pass) and its
/// viewport's height. Rows are measured with `measure` and stacked from the
/// snapshot's space before them, after the header: the content is the
/// header, the space before the rows, the rows, the space after them, and
/// the footer.
@MainActor
package final class LayoutHost: VirtualHost {
  package private(set) var scrollOffset = 0.0
  package private(set) var viewportHeight = 0.0
  package var viewportInsets: (top: Double, bottom: Double) { (0, 0) }
  package private(set) var contentHeight = 0.0
  package var scrollsWithLayout: Bool { true }

  /// The rows to lay out, and the space before and after them: the
  /// snapshot's.
  package var rows: () -> (keys: [(key: AnyHashable, index: Int)], before: Double, after: Double) =
    { ([], 0, 0) }
  /// A row's height, at the view's width.
  package var measure: (AnyHashable) -> Double = { _ in 0 }
  /// The heights of the views above and below the rows, at the view's
  /// width: 0 for none.
  package var header: () -> Double = { 0 }
  package var footer: () -> Double = { 0 }
  /// The header's and the footer's heights, as last laid out.
  package private(set) var headerHeight = 0.0
  package private(set) var footerHeight = 0.0
  package var didScroll: () -> Void = {}
  package var viewportDidResize: () -> Void = {}
  package var afterUpdate: () -> Void = {}
  /// A jump or a restore hasn't landed yet.
  package var isLanding: () -> Bool = { false }
  /// Asks the view for a layout pass.
  package var setNeedsLayout: () -> Void = {}

  private var frames: [VirtualRowFrame] = []
  /// A snapshot, or a row's height, not laid out yet.
  private var stale = true
  /// The core scrolled outside a pass: the next pass applies its offset,
  /// not the view's.
  private var scrolled = false
  private var inPass = false

  package init() {}

  package func rowFrames() -> [VirtualRowFrame] { frames }

  /// A new snapshot, or rows (or the header or footer) that changed
  /// height.
  package func invalidate() {
    stale = true
    if !inPass { setNeedsLayout() }
  }

  package func scroll(to target: Double) {
    let offset = max(0, min(target, contentHeight - viewportHeight))
    guard offset != scrollOffset else { return }
    scrollOffset = offset
    if !inPass {
      scrolled = true
      setNeedsLayout()
    }
    didScroll()
  }

  /// One layout pass. `offset` is where the view is scrolled (the
  /// viewport's top in content coordinates), `viewport` its height. Returns
  /// the rows' frames, the content's height and the offset to apply.
  package func layout(
    offset: Double, viewport: Double
  ) -> (
    frames: [VirtualRowFrame], contentHeight: Double, offset: Double
  ) {
    inPass = true
    defer { inPass = false }
    if viewport != viewportHeight {
      viewportHeight = viewport
      viewportDidResize()
    }
    if scrolled {
      scrolled = false
    } else if offset != scrollOffset {
      // The user's scroll, reported with the rows where they were: the core
      // takes its anchor from them before new rows move it.
      scrollOffset = offset
      didScroll()
    }
    // What the core does about the update can be a new snapshot; it's laid
    // out in this pass too.
    for _ in 0..<8 where stale {
      stale = false
      place()
      afterUpdate()
    }
    return (frames, contentHeight, scrollOffset)
  }

  private func place() {
    let r = rows()
    headerHeight = header()
    var y = headerHeight + r.before
    frames = r.keys.map { item in
      let height = measure(item.key)
      defer { y += height }
      return VirtualRowFrame(index: item.index, key: item.key, minY: y, maxY: y + height)
    }
    footerHeight = footer()
    contentHeight = y + r.after + footerHeight
  }
}
