import Observation
import SwiftUI

#if os(iOS)
  import UIKit
  typealias PlatformView = UIView
  typealias PlatformScrollView = UIScrollView
#else
  import AppKit
  typealias PlatformView = NSView
  typealias PlatformScrollView = NSScrollView
#endif

/// Checks every display frame for a wrong position: with `-probe`, each row
/// carries a marker view, and on every frame the probe reads where the rows
/// are on screen. A frame is wrong when the rows move by anything but the
/// scroll that drove them: a page loading, space changing, or a jump drawn
/// at a position the list never meant to show.
///
/// - Driven by the probe itself (`drive`), the scroll is known exactly: the
///   rows must move by it, to the half point.
/// - Flung by a finger (iOS), the momentum is smooth: two frames whose
///   speed departs from their neighboring pairs' are a jump in it.
/// - At rest, nothing moves.
/// - A jump (Latest, or a key moving the selection) shows the rows as they
///   were, then as they end up, and nothing in between.
/// - The header changing height on screen pushes the rows by as much, its
///   top holding still; off screen, nothing moves.
///
/// Every frame also checks that the rows move together. The summary is the
/// `probe` element's value, for the UI tests.
@MainActor @Observable
final class FrameProbe {
  static let shared = FrameProbe()
  static let enabled = ProcessInfo.processInfo.arguments.contains("-probe")
  static var script: String? { UserDefaults.standard.string(forKey: "probe-script") }

  /// The summary the UI tests read.
  private(set) var summary = "wrong=0"

  @ObservationIgnored private var markers: [String: WeakView] = [:]
  @ObservationIgnored private(set) weak var scrollView: PlatformScrollView?
  @ObservationIgnored private var previous: Sample?
  @ObservationIgnored private var smooth:
    [(dy: Double, dt: Double, regular: Bool, sample: Sample)] =
      []
  @ObservationIgnored private var applied: Double?
  @ObservationIgnored private var drive: (velocity: Double, done: CheckedContinuation<Void, Never>)?
  @ObservationIgnored private var jump: [Sample]?
  @ObservationIgnored private var jumpStill = 0.0
  /// Since the jump began, and whether it has moved yet.
  @ObservationIgnored private var jumpTime = 0.0
  @ObservationIgnored private var jumpMoved = false
  /// The phase the jump began in, which its frames count in; and whether
  /// a new phase began before it landed: then it ends with the next frame.
  @ObservationIgnored private var jumpPhase = ""
  @ObservationIgnored private var jumpCarried = false
  @ObservationIgnored private var flingStart: (t: Double, moved: Double)?
  @ObservationIgnored private(set) var phase = "start"
  @ObservationIgnored private var wrongByPhase: [(String, Int)] = []
  @ObservationIgnored private(set) var wrong = 0
  @ObservationIgnored private var frames = 0
  @ObservationIgnored private var moving = 0
  @ObservationIgnored private var dropped = 0
  /// The last wrong frames' reasons, for a failing test's message.
  @ObservationIgnored private var lastWrong: [String] = []
  private static var frameInterval: Double {
    #if os(iOS)
      1 / Double(UIScreen.main.maximumFramesPerSecond)
    #else
      1 / Double(NSScreen.main?.maximumFramesPerSecond ?? 60)
    #endif
  }
  @ObservationIgnored private var flings: [String] = []
  @ObservationIgnored private var done = false
  @ObservationIgnored private var ticker: AnyObject?

  struct Sample {
    var t: Double
    /// Rows on screen: top and bottom from the viewport's top.
    var rows: [String: (minY: Double, maxY: Double)]
    /// Every laid-out row's top in content coordinates, on screen or not.
    var content: [String: Double] = [:]
    var tracking = false
    var decelerating = false
    var atEdge = false
    /// The viewport's top in content coordinates: below 0 in a bounce.
    var offset = 0.0
    /// How long after the display link's timestamp the frame was sampled.
    var latency = 0.0

    /// Top to bottom.
    var ids: [String] { rows.sorted { $0.value.minY < $1.value.minY }.map(\.key) }
  }

  /// The rows on screen in the last frame, top to bottom.
  var visible: [String] { previous?.ids ?? [] }
  func top(of id: String) -> Double? { previous?.rows[id]?.minY }
  func bottom(of id: String) -> Double? { previous?.rows[id]?.maxY }
  var viewportHeight: Double {
    #if os(iOS)
      guard let s = scrollView else { return 0 }
      return s.bounds.height - s.adjustedContentInset.top - s.adjustedContentInset.bottom
    #else
      return Double(scrollView?.contentView.bounds.height ?? 0)
    #endif
  }

  func register(_ id: String, _ view: PlatformView) {
    markers[id] = WeakView(view: view)
    if scrollView == nil { scrollView = Self.enclosingScrollView(view) }
    if ticker == nil, let scrollView { start(scrollView) }
  }

  func unregister(_ id: String, _ view: PlatformView) {
    if markers[id]?.view === view { markers[id] = nil }
  }

  func setPhase(_ name: String) {
    // A jump that hasn't moved yet, as when a stalled frame (a slow
    // simulator's) held its landing back past the phase's end, takes the
    // next frame, which the new phase can't have moved yet: it's checked as
    // the jump's, in the jump's phase, and the jump ends with it, moved or
    // not (a jump to where the list already is never moves).
    if jump != nil, !jumpMoved, !jumpCarried {
      jumpCarried = true
      log("JUMP still landing as \(name) begins")
    } else {
      finishJump()
    }
    log("PHASE \(name)")
    phase = name
    if !wrongByPhase.contains(where: { $0.0 == name }) { wrongByPhase.append((name, 0)) }
    publish()
  }

  func finish() {
    finishJump()
    done = true
    publish()
    log("DONE \(summary)")
  }

  /// A programmatic jump starts: the frames until it settles are checked
  /// as a jump.
  func beginJump() {
    guard Self.enabled else { return }
    finishJump()
    jump = previous.map { [$0] } ?? []
    jumpStill = 0
    jumpTime = 0
    jumpMoved = false
    jumpPhase = phase
    jumpCarried = false
    log("JUMP begin top=\(previous?.ids.first ?? "-")")
  }

  #if os(macOS)
    /// Presses the down arrow (or the up arrow) in the list, as the user
    /// would: the list takes focus first.
    func pressArrow(down: Bool) {
      guard let scrollView, let window = scrollView.window,
        let event = NSEvent.keyEvent(
          with: .keyDown, location: .zero, modifierFlags: [.numericPad, .function],
          timestamp: ProcessInfo.processInfo.systemUptime, windowNumber: window.windowNumber,
          context: nil,
          characters: String(UnicodeScalar(down ? NSDownArrowFunctionKey : NSUpArrowFunctionKey)!),
          charactersIgnoringModifiers: String(
            UnicodeScalar(down ? NSDownArrowFunctionKey : NSUpArrowFunctionKey)!),
          isARepeat: false, keyCode: down ? 125 : 126)
      else { return }
      if window.firstResponder !== scrollView.documentView {
        window.makeFirstResponder(scrollView.documentView)
      }
      window.sendEvent(event)
    }
  #endif

  /// Scrolls as a fling would: `velocity` points per second toward the
  /// content's end (negative: toward its start), decaying as UIKit's does.
  func drive(velocity: Double) async {
    await withCheckedContinuation { drive = (velocity, $0) }
  }

  // MARK: Frames

  private func start(_ view: PlatformView) {
    #if os(iOS)
      let link = CADisplayLink(target: Tick(self), selector: #selector(Tick.tick))
      link.preferredFrameRateRange = CAFrameRateRange(minimum: 80, maximum: 120, preferred: 120)
      link.add(to: .main, forMode: .common)
      ticker = link
    #else
      let link = view.displayLink(target: Tick(self), selector: #selector(Tick.tick))
      link.add(to: .main, forMode: .common)
      ticker = link
    #endif
  }

  fileprivate func tick(_ t: Double) {
    guard let scrollView else { return }
    if scrollView.window == nil { return }
    let sample = sample(t, scrollView)
    frames += 1
    // A frame interval over 1.5 of the display's: frames dropped while the
    // list moves.
    if let previous, drive != nil || sample.decelerating {
      moving += 1
      if t - previous.t > 1.5 * Self.frameInterval { dropped += 1 }
    }
    if let previous { check(previous, sample) }
    previous = sample
    step(t, scrollView)
  }

  private func sample(_ t: Double, _ scrollView: PlatformScrollView) -> Sample {
    var s = Sample(t: t, rows: [:])
    s.latency = CACurrentMediaTime() - t
    #if os(iOS)
      let port = scrollView.convert(scrollView.bounds, to: nil)
        .inset(by: scrollView.adjustedContentInset)
      let y = scrollView.contentOffset.y + scrollView.adjustedContentInset.top
      s.offset = Double(y)
      for (id, m) in markers {
        guard let v = m.view, v.window != nil, v.bounds.height > 0, !Self.hidden(v) else {
          continue
        }
        let r = v.convert(v.bounds, to: nil)
        s.content[id] = Double(r.minY - port.minY) + s.offset
        guard r.maxY > port.minY + 0.5, r.minY < port.maxY - 0.5 else { continue }
        s.rows[id] = (Double(r.minY - port.minY), Double(r.maxY - port.minY))
      }
      s.tracking = scrollView.isTracking
      s.decelerating = scrollView.isDecelerating
      s.atEdge = y <= 0.5 || y >= scrollView.contentSize.height - viewportHeight - 0.5
    #else
      let clip = scrollView.contentView
      let port = clip.convert(clip.bounds, to: nil)
      let y = clip.bounds.origin.y
      s.offset = Double(y)
      for (id, m) in markers {
        guard let v = m.view, v.window != nil, v.bounds.height > 0, !Self.hidden(v) else {
          continue
        }
        let r = v.convert(v.bounds, to: nil)
        // Window coordinates count up from the bottom.
        let minY = port.maxY - r.maxY
        let maxY = port.maxY - r.minY
        s.content[id] = Double(minY) + s.offset
        guard maxY > 0.5, minY < port.height - 0.5 else { continue }
        s.rows[id] = (Double(minY), Double(maxY))
      }
      let height = scrollView.documentView?.frame.height ?? 0
      s.atEdge = y <= 0.5 || y >= height - clip.bounds.height - 0.5
    #endif
    return s
  }

  private func check(_ a: Sample, _ b: Sample) {
    let driven = applied
    applied = nil
    if jump != nil {
      jump!.append(b)
      jumpTime += b.t - a.t
      if moved(a, b) {
        jumpStill = 0
        jumpMoved = true
      } else {
        jumpStill += b.t - a.t
      }
      // Settled half a second after it last moved. Its rows can take longer
      // than that to load (a simulator's can), so it waits for its first
      // move; a jump to where the list already is never moves.
      if jumpCarried || (jumpMoved && jumpStill >= 0.5) || jumpTime >= 5 { finishJump() }
      return
    }
    // Rows, not the header or the footer, which the checks below place.
    let common = a.rows.keys.filter { b.rows[$0] != nil && !Self.accessories.contains($0) }
    guard !common.isEmpty else {
      if a.rows.keys.contains(where: Self.isRow) && b.rows.keys.contains(where: Self.isRow) {
        noCommonRow(a, b)
      }
      return
    }
    let deltas = common.map { (id: $0, dy: b.rows[$0]!.minY - a.rows[$0]!.minY) }
    let spread = (deltas.map(\.dy).max() ?? 0) - (deltas.map(\.dy).min() ?? 0)
    if spread > 1 { flag(b, "rows moved apart by \(fmt(spread))", a) }
    // The anchor: the top row on screen in both frames.
    let anchor = common.min { a.rows[$0]!.minY < a.rows[$1]!.minY }!
    let dy = b.rows[anchor]!.minY - a.rows[anchor]!.minY
    let real = b.tracking || b.decelerating || a.tracking || a.decelerating
    if let driven {
      if abs(dy + driven) > 0.75 {
        flag(b, "\(anchor) moved \(fmt(dy)), the drive \(fmt(-driven))", a)
      }
    } else if real {
      smoothCheck(dy: dy, a: a, b: b)
    } else if abs(dy) > 0.5, !pushedByHeader(a, b, dy) {
      flag(b, "\(anchor) moved \(fmt(dy)) at rest", a)
    }
    trackFling(a, b, dy)
    // At rest, the list's first row is at its top or above: space below
    // it is a gap the list shows, not a bounce (which only moves).
    // Below the header, when it's on screen.
    if !real, driven == nil, drive == nil, let first = Self.firstID,
      let y = b.rows[first]?.minY, y - (b.rows["header"]?.maxY ?? 0) > 0.5
    {
      let gap = y - (b.rows["header"]?.maxY ?? 0)
      flag(b, "gap of \(fmt(gap)) above \(first) at rest (offset \(fmt(b.offset)))")
    }
  }

  /// No row on screen in both frames: a jump, unless the offset moved by
  /// about as much as the rows did, as when a stalled frame (a slow
  /// simulator's) lets a fling run on, or the scroll view catches up on the
  /// next frame. Rows laid out in both frames, on screen or not, show it: a
  /// scroll leaves their content positions where they were, or moves them
  /// by what the list compensated in the same pass, as when the stalled
  /// frame brought a page whose rows came out taller than estimated (113 pt
  /// for 23 rows, against a 1,188 pt scroll). A jump moves them while the
  /// offset stays. So up to a quarter of the offset's change, or 50 pt, is a
  /// scroll.
  private func noCommonRow(_ a: Sample, _ b: Sample) {
    let laid = a.content.keys.filter { b.content[$0] != nil && Self.isRow($0) }
    let drift = laid.map { abs(b.content[$0]! - a.content[$0]!) }.min()
    let detail =
      "dt \(fmt((b.t - a.t) * 1000)) ms, latency \(fmt(a.latency * 1000))→\(fmt(b.latency * 1000)) ms, offset \(fmt(a.offset))→\(fmt(b.offset))"
    if let drift, drift <= max(50, abs(b.offset - a.offset) / 4) {
      log("SCROLLED no row in both frames, rows laid out in both moved \(fmt(drift)) (\(detail))")
      return
    }
    let moved = drift.map { "rows laid out in both moved \(fmt($0))" } ?? "no row laid out in both"
    flag(b, "no row in both frames, \(moved) (\(detail))", a)
  }

  /// The header and the footer: placed with the rows, but not rows.
  private static let accessories: Set<String> = ["header", "footer"]
  private static func isRow(_ id: String) -> Bool { !accessories.contains(id) }

  /// At rest, the rows moved because the header, on screen, changed
  /// height: its top held still, and the rows moved with its bottom.
  private func pushedByHeader(_ a: Sample, _ b: Sample, _ dy: Double) -> Bool {
    guard let ha = a.rows["header"], let hb = b.rows["header"],
      abs((hb.maxY - hb.minY) - (ha.maxY - ha.minY)) > 0.5
    else { return false }
    return abs(hb.minY - ha.minY) <= 0.5 && abs(dy - (hb.maxY - ha.maxY)) <= 0.75
  }

  /// The list's first row: where its top is the list's.
  private static var firstID: String? {
    ["chat": "m0000", "issues": "i0000"][UserDefaults.standard.string(forKey: "screen") ?? ""]
  }

  /// Momentum is smooth: two frames whose speed departs from what their
  /// neighboring pairs' speeds make it, by more than deceleration does, are
  /// a jump in it. Checked over pairs, as UIKit moves a slow fling every
  /// other frame; and not within two frames of a late frame, nor of one
  /// whose callback came a frame later in its interval than the last (a
  /// simulator's are irregular): after either, momentum catches up, and the
  /// frame drawn may not be the one sampled.
  private func smoothCheck(dy: Double, a: Sample, b: Sample) {
    // A bounce past the top springs back at its own pace: not momentum.
    let bouncing = a.offset < -0.5 || b.offset < -0.5
    let late =
      b.t - a.t > 1.5 * Self.frameInterval
      || abs(b.latency - a.latency) > 0.5 * Self.frameInterval
    smooth.append((dy, b.t - a.t, !(a.tracking || b.tracking || bouncing || late), b))
    if smooth.count > 8 { smooth.removeFirst() }
    guard smooth.count == 8, smooth.allSatisfy(\.regular) else { return }
    let w = smooth.suffix(6).map { $0 }
    func speed(_ i: Int) -> Double { (w[i].dy + w[i + 1].dy) / (w[i].dt + w[i + 1].dt) }
    let (p, m, n) = (speed(0), speed(2), speed(4))
    let expected = (p + n) / 2
    let sample = w[3].sample
    let moved = w[2].dy + w[3].dy
    if abs(m - expected) > max(300, 0.15 * abs(expected)) {
      // Once per jump: the next check starts after it.
      smooth.removeAll()
      flag(
        sample,
        "speed \(fmt(m)) between \(fmt(p)) and \(fmt(n)) (moved \(fmt(moved)))")
    }
  }

  private func trackFling(_ a: Sample, _ b: Sample, _ dy: Double) {
    if b.decelerating && !b.tracking {
      if flingStart == nil { flingStart = (b.t, 0) }
      flingStart!.moved += dy
    } else if let start = flingStart, !b.decelerating {
      flingStart = nil
      let entry = "\(fmt(b.t - start.t))s/\(fmt(abs(start.moved)))pt\(b.atEdge ? "/edge" : "")"
      flings.append(entry)
      log("FLING \(entry)")
      publish()
    }
  }

  private func finishJump() {
    guard let frames = jump, let first = frames.first, let last = frames.last else {
      jump = nil
      return
    }
    jump = nil
    var bad = 0
    // Each state the jump showed, once.
    for (i, f) in frames.enumerated() where i == 0 || !same(f, frames[i - 1]) {
      let top = f.ids.first.map { "\($0)@\(fmt(f.rows[$0]!.minY))" } ?? "-"
      let ms = fmt((f.t - first.t) * 1000)
      log("JUMP frame \(i) +\(ms)ms \(top)…\(f.ids.last ?? "-") rows=\(f.rows.count)")
    }
    for f in frames.dropFirst() where !same(f, first) && !same(f, last) {
      bad += 1
      flag(
        f, "jump frame neither where it was (\(first.ids.first ?? "-")) nor where it lands",
        phase: jumpPhase)
    }
    log(
      "JUMP end frames=\(frames.count) wrong=\(bad) from \(first.ids.first ?? "-")…\(first.ids.last ?? "-") to \(last.ids.first ?? "-")…\(last.ids.last ?? "-")"
    )
  }

  private func moved(_ a: Sample, _ b: Sample) -> Bool { !same(a, b) }

  private func same(_ a: Sample, _ b: Sample) -> Bool {
    guard Set(a.rows.keys) == Set(b.rows.keys) else { return false }
    return a.rows.allSatisfy { abs($0.value.minY - b.rows[$0.key]!.minY) <= 0.5 }
  }

  private func flag(_ s: Sample, _ why: String, _ before: Sample? = nil, phase: String? = nil) {
    let phase = phase ?? self.phase
    wrong += 1
    if let i = wrongByPhase.firstIndex(where: { $0.0 == phase }) { wrongByPhase[i].1 += 1 }
    let rows = s.ids.prefix(2).map { "\($0)@\(fmt(s.rows[$0]!.minY))" }.joined(separator: " ")
    let was = before.map { b in
      " was " + b.ids.prefix(2).map { "\($0)@\(fmt(b.rows[$0]!.minY))" }.joined(separator: " ")
    }
    let line = "WRONG [\(phase)] \(why): \(rows)\(was ?? "")"
    lastWrong = Array((lastWrong + [line]).suffix(4))
    log(line)
    publish()
  }

  private func publish() {
    let phases = wrongByPhase.map { "\($0.0):\($0.1)" }.joined(separator: ",")
    summary =
      "wrong=\(wrong) frames=\(frames) moving=\(moving) dropped=\(dropped) phases=[\(phases)] flings=[\(flings.suffix(8).joined(separator: ","))] last=[\(lastWrong.joined(separator: "; "))]\(done ? " done" : "")"
  }

  private func log(_ s: String) {
    print("PROBE \(String(format: "%.3f", Date().timeIntervalSince1970)) \(s)")
    fflush(stdout)
  }

  private func fmt(_ x: Double) -> String { String(format: "%.1f", x) }

  // MARK: Driving

  /// Moves the scroll view by this frame's share of the drive, after the
  /// frame was checked: the next frame must show the rows moved by it.
  private func step(_ t: Double, _ scrollView: PlatformScrollView) {
    guard let d = drive else { return }
    let dt = 1.0 / 120
    let move = d.velocity * dt
    #if os(iOS)
      let inset = scrollView.adjustedContentInset
      let lo = -inset.top
      let hi = max(lo, scrollView.contentSize.height + inset.bottom - scrollView.bounds.height)
      let from = scrollView.contentOffset.y
      let to = min(hi, max(lo, from + move))
      scrollView.contentOffset.y = to
    #else
      let clip = scrollView.contentView
      let height = scrollView.documentView?.frame.height ?? 0
      let from = clip.bounds.origin.y
      let to = min(max(0, height - clip.bounds.height), max(0, from + move))
      clip.scroll(to: CGPoint(x: clip.bounds.origin.x, y: to))
      scrollView.reflectScrolledClipView(clip)
    #endif
    applied = Double(to - from)
    // UIKit's deceleration: 0.998 a millisecond.
    let v = d.velocity * pow(0.998, dt * 1000)
    if abs(v) < 20 || to == from {
      drive = nil
      d.done.resume()
    } else {
      drive = (v, d.done)
    }
  }

  /// A row's view the list hides: drawn rows only count.
  private static func hidden(_ view: PlatformView) -> Bool {
    #if os(iOS)
      var v: UIView? = view
      while let s = v {
        if s.isHidden { return true }
        v = s.superview
      }
      return false
    #else
      return view.isHiddenOrHasHiddenAncestor
    #endif
  }

  private static func enclosingScrollView(_ view: PlatformView) -> PlatformScrollView? {
    #if os(iOS)
      var v: UIView? = view.superview
      while let s = v {
        if let scroll = s as? UIScrollView { return scroll }
        v = s.superview
      }
      return nil
    #else
      return view.enclosingScrollView
    #endif
  }
}

private struct WeakView {
  weak var view: PlatformView?
}

/// The display link's target: a link retains it, not the probe.
private final class Tick: NSObject {
  weak var probe: FrameProbe?
  init(_ probe: FrameProbe) { self.probe = probe }

  @MainActor @objc func tick(_ link: CADisplayLink) { probe?.tick(link.timestamp) }
}

/// A row's marker: a view the size of the row, which the probe reads.
struct ProbeMarker: View {
  let id: String

  var body: some View {
    if FrameProbe.enabled { MarkerView(id: id) }
  }
}

#if os(iOS)
  private struct MarkerView: UIViewRepresentable {
    let id: String
    func makeUIView(context: Context) -> Marker { Marker(id) }
    func updateUIView(_ view: Marker, context: Context) {}
  }

  /// A marker in a row's own view (`NativeRowView`): its id changes with
  /// the row the view shows.
  final class Marker: UIView {
    var id: String {
      didSet {
        guard id != oldValue, window != nil else { return }
        FrameProbe.shared.unregister(oldValue, self)
        FrameProbe.shared.register(id, self)
      }
    }
    init(_ id: String) {
      self.id = id
      super.init(frame: .zero)
      isUserInteractionEnabled = false
    }
    required init?(coder: NSCoder) { fatalError() }
    override func didMoveToWindow() {
      super.didMoveToWindow()
      if window != nil {
        FrameProbe.shared.register(id, self)
      } else {
        FrameProbe.shared.unregister(id, self)
      }
    }
  }
#else
  private struct MarkerView: NSViewRepresentable {
    let id: String
    func makeNSView(context: Context) -> Marker { Marker(id) }
    func updateNSView(_ view: Marker, context: Context) {}
  }

  final class Marker: NSView {
    var id: String {
      didSet {
        guard id != oldValue, window != nil else { return }
        FrameProbe.shared.unregister(oldValue, self)
        FrameProbe.shared.register(id, self)
      }
    }
    init(_ id: String) {
      self.id = id
      super.init(frame: .zero)
    }
    required init?(coder: NSCoder) { fatalError() }
    override func hitTest(_ point: NSPoint) -> NSView? { nil }
    override func viewDidMoveToWindow() {
      super.viewDidMoveToWindow()
      if window != nil {
        FrameProbe.shared.register(id, self)
      } else {
        FrameProbe.shared.unregister(id, self)
      }
    }
  }
#endif

/// The probe's summary, as an element the UI tests read.
struct ProbeSummary: View {
  @State private var probe = FrameProbe.shared

  var body: some View {
    if FrameProbe.enabled {
      Text(probe.summary.prefix(40))
        .font(.system(size: 8))
        .lineLimit(1)
        .accessibilityIdentifier("probe")
        .accessibilityValue(probe.summary)
    }
  }
}
