import Dispatch

/// A `VirtualHost` for a scroll view that lays its rows out itself, in one
/// layout pass: the rows' frames, the content's height and the offset are
/// worked out together, and the view applies them before it draws. The core
/// runs inside the pass: what it does about a pass (a scroll that keeps the
/// rows in place, a new snapshot) is in the same pass, so no frame shows the
/// content moved and the offset not yet.
///
/// The view calls `layout(offset:viewport:)` from its layout pass, with the
/// offset it's at (the user's scroll, since the last pass) and its
/// viewport's height. Rows are stacked from the snapshot's space before
/// them, after the header: the content is the header, the space before the
/// rows, the rows, the space after them, and the footer.
///
/// Only the rows within a viewport's height of the viewport are measured
/// (with `measure`, which makes their views): the rest stand at the height
/// they were last measured at, or the estimate, until they come that near.
/// A row measured at another height moves the rows below it, as a row
/// changing height does, and the core keeps what's on screen in place. While
/// a jump or a restore is landing every row is measured: where it lands is
/// worked out from the rows above it.
///
/// Rows on screen are measured in the pass that shows them. Rows off screen
/// not measured before are measured in the frames after, nearest the screen
/// first (the next a scroll shows, either way): at the start of each frame
/// (`nextFrame`), its pass measures them for up to `frameBudget`, so a pass
/// that brings a page doesn't make all its views in one frame. Until then
/// they stand at the estimate.
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
  /// The height that stands in for a row not measured yet.
  package var estimatedRowHeight: () -> Double = { 0 }
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
  /// Runs work at the start of the next frame: the view's display link.
  package var nextFrame: (@escaping @MainActor () -> Void) -> Void = { work in
    DispatchQueue.main.async { MainActor.assumeIsolated(work) }
  }
  /// The time, in seconds.
  package var now: () -> Double = { Double(DispatchTime.now().uptimeNanoseconds) / 1e9 }
  /// How long, from a frame's first pass, its passes may spend measuring
  /// rows off screen not measured before, in seconds.
  package var frameBudget = 0.004

  private var frames: [VirtualRowFrame] = []
  /// The rows' heights as last measured, for when they're out of reach.
  private var heights: [AnyHashable: Double] = [:]
  /// The rows the last placing measured.
  private var measured: Set<AnyHashable> = []
  /// A snapshot, or a row's height, not laid out yet.
  private var stale = true
  /// The core scrolled outside a pass: the next pass applies its offset,
  /// not the view's.
  private var scrolled = false
  private var inPass = false
  /// Until when rows off screen not measured before are measured: the
  /// frame's budget, from its first pass.
  private var deadline = -Double.infinity
  private var frameStarted = false
  /// A pass left rows near the viewport unmeasured, and the next frame will
  /// lay out again.
  private var framePending = false

  package init() {}

  package func rowFrames() -> [VirtualRowFrame] { frames }

  /// A new snapshot, or rows (or the header or footer) that changed
  /// height.
  package func invalidate() {
    stale = true
    if !inPass { setNeedsLayout() }
  }

  /// The rows' heights at the view's width are no longer their measured
  /// ones, as when the width changed: each is measured again once it's near.
  package func invalidateHeights() {
    heights = [:]
    invalidate()
  }

  package func scroll(to target: Double) {
    let offset = max(0, min(target, contentHeight - viewportHeight))
    guard offset != scrollOffset else { return }
    scrollOffset = offset
    // Laid out again where it scrolled to: rows that came near are
    // measured, and the core sees them.
    stale = true
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
    if frameStarted {
      frameStarted = false
      deadline = now() + frameBudget
    }
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
    // What the core does about the update can be a new snapshot, or a
    // scroll to rows not measured yet; they're laid out in this pass too.
    for _ in 0..<8 where stale || !rowsOnScreenMeasured() {
      stale = false
      place()
      afterUpdate()
    }
    if !framePending, !nearRowsMeasured() {
      framePending = true
      nextFrame { [weak self] in
        guard let self else { return }
        framePending = false
        frameStarted = true
        invalidate()
      }
    }
    return (frames, contentHeight, scrollOffset)
  }

  /// Where rows are measured: the viewport and a viewport's height above
  /// and below it, as far as a fling goes in a few frames, so rows are
  /// measured before they're drawn and a row above that comes out another
  /// height than estimated does so off screen. While a jump or a restore is
  /// landing, all of them.
  private var reach: ClosedRange<Double> {
    if isLanding() { return -Double.infinity...Double.infinity }
    return (scrollOffset - viewportHeight)...(scrollOffset + 2 * viewportHeight)
  }

  /// Where rows are measured in the pass that draws them, however many are
  /// new: the viewport, or all of them while a jump or a restore lands.
  private var screen: ClosedRange<Double> {
    if isLanding() { return reach }
    return scrollOffset...(scrollOffset + viewportHeight)
  }

  private func nearRowsMeasured() -> Bool { allMeasured(in: reach) }

  private func rowsOnScreenMeasured() -> Bool { allMeasured(in: screen) }

  private func allMeasured(in range: ClosedRange<Double>) -> Bool {
    frames.allSatisfy {
      $0.minY > range.upperBound || $0.maxY < range.lowerBound || measured.contains($0.key)
    }
  }

  private func place() {
    let r = rows()
    headerHeight = header()
    measured = []
    stack(r)
    // Rows off screen not measured before, while the frame's budget lasts:
    // those nearest the screen first, which a scroll shows next, either way
    // it goes, each stacked where the rows measured before it put it.
    let reach = reach
    let screen = screen
    while now() < deadline {
      let new = frames.filter {
        !measured.contains($0.key) && $0.minY <= reach.upperBound && $0.maxY >= reach.lowerBound
      }
      guard
        let row = new.min(by: {
          max($0.minY - screen.upperBound, screen.lowerBound - $0.maxY)
            < max($1.minY - screen.upperBound, screen.lowerBound - $1.maxY)
        })
      else { break }
      heights[row.key] = measure(row.key)
      stack(r)
    }
    footerHeight = footer()
    contentHeight = (frames.last?.maxY ?? headerHeight + r.before) + r.after + footerHeight
  }

  /// The rows stacked from the top: those within reach measured if they
  /// were before or are on screen, the rest at their last height or the
  /// estimate.
  private func stack(
    _ r: (keys: [(key: AnyHashable, index: Int)], before: Double, after: Double)
  ) {
    let reach = reach
    let screen = screen
    let estimate = estimatedRowHeight()
    var known: [AnyHashable: Double] = [:]
    var y = headerHeight + r.before
    frames = r.keys.map { item in
      var height = heights[item.key] ?? estimate
      if y <= reach.upperBound && y + height >= reach.lowerBound {
        let onScreen = y <= screen.upperBound && y + height >= screen.lowerBound
        if heights[item.key] != nil || onScreen {
          height = measure(item.key)
          measured.insert(item.key)
        }
      }
      if measured.contains(item.key) || heights[item.key] != nil { known[item.key] = height }
      defer { y += height }
      return VirtualRowFrame(index: item.index, key: item.key, minY: y, maxY: y + height)
    }
    heights = known
  }
}
