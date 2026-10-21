#if canImport(SwiftUI)
  import Foundation
  import SwiftUI
  import Testing
  import ZeroTesting
  import ZeroUI

  @testable import Zero
  @testable import ZeroVirtual

  #if canImport(UIKit)
    import UIKit
  #else
    import AppKit
  #endif

  /// `VirtualList` in a real window, on a `LoopbackServer`.
  @Suite(.serialized) @MainActor struct VirtualListTests {
    struct Page: Codable, Sendable {
      var limit: Int
      var start: String?
      var backward: Bool
    }

    static let page = defineQuery("notes.page") { (a: Page) in
      var q = Notes.table.orderBy(\.id, a.backward ? .desc : .asc)
      if let s = a.start { q = q.start([\.id: s]) }
      return q.limit(a.limit)
    }
    static let byID = defineQuery("notes.byID") { (id: String) in
      Notes.table.where(\.id, id).one()
    }

    typealias List = ZeroVirtualizer<Void, Note, String, String, String>

    static func id(_ i: Int) -> String { String(format: "n%03d", i) }

    func makeList() throws -> (Zero<Void>, List) {
      let server = LoopbackServer()
      server.put(
        "note", (0..<300).map { ["id": .string(Self.id($0)), "text": .string("note \($0)")] })
      var options = ZeroOptions(
        cacheURL: "http://loopback.test", userID: "u", storageKey: randomID(), storage: .memory)
      options.logSink = RecordingLogSink()
      let zero = try Zero(schema: .notes, options: options, socketFactory: server)
      let list = List(
        zero: zero, listContext: "", estimatedRowHeight: 30, rowKey: \.id, toStartRow: \.id,
        pageQuery: { r, _ in
          Self.page(Page(limit: r.limit, start: r.start, backward: r.direction == .backward))
        },
        rowQuery: { r, _ in Self.byID(r.id) }, minPageSize: 20)
      return (zero, list)
    }

    /// A 300 × 400 window showing `view`.
    func show(_ view: some View) -> AnyObject {
      #if canImport(UIKit)
        let window = UIWindow(frame: CGRect(x: 0, y: 0, width: 300, height: 400))
        window.rootViewController = UIHostingController(rootView: view)
        window.makeKeyAndVisible()
        return window
      #else
        let window = NSWindow(
          contentRect: CGRect(x: 0, y: 0, width: 300, height: 400), styleMask: [.titled],
          backing: .buffered, defer: false)
        window.contentView = NSHostingView(rootView: view)
        window.makeKeyAndOrderFront(nil)
        return window
      #endif
    }

    /// Waits until `done`, for at most `seconds`, leaving the main actor
    /// free: the server's deliveries and SwiftUI's layout run on it. (A
    /// nested run loop wouldn't drain the main queue from a test running on
    /// it.)
    func run(_ seconds: Double, until done: () -> Bool) async throws {
      let end = Date().addingTimeInterval(seconds)
      while Date() < end, !done() { try await Task.sleep(for: .milliseconds(20)) }
    }

    /// The scroll view's offset from the content's top and the height it
    /// shows content in, insets aside, as the platform has them.
    func platformGeometry(_ window: AnyObject) -> (offset: Double, viewport: Double)? {
      #if canImport(UIKit)
        func find(_ v: UIView) -> UIScrollView? {
          (v as? UIScrollView) ?? v.subviews.lazy.compactMap(find).first
        }
        return (window as? UIWindow).flatMap(find).map {
          let inset = $0.adjustedContentInset
          return (
            Double($0.contentOffset.y + inset.top),
            Double($0.bounds.height - inset.top - inset.bottom)
          )
        }
      #else
        func find(_ v: NSView) -> NSScrollView? {
          (v as? NSScrollView) ?? v.subviews.lazy.compactMap(find).first
        }
        return (window as? NSWindow)?.contentView.flatMap(find).map {
          let inset = $0.contentInsets
          return (
            Double($0.contentView.bounds.origin.y + inset.top),
            Double($0.contentView.bounds.height - inset.top - inset.bottom)
          )
        }
      #endif
    }

    final class Seen {
      var texts: Set<String> = []
    }

    /// A row that runs its own query.
    struct QueryRow: View {
      @ZeroQuery<Note, Note?, Void> var note: Note?
      let seen: Seen

      init(id: String, seen: Seen) {
        _note = ZeroQuery(VirtualListTests.byID(id))
        self.seen = seen
      }

      var body: some View {
        let _ = note.map { seen.texts.insert($0.text) }
        Text(note?.text ?? "").frame(height: 30)
      }
    }

    // Rows are hosted views of their own, which inherit the list's environment.
    @Test func aRowsQueryReadsTheListsClient() async throws {
      let (zero, list) = try makeList()
      let seen = Seen()
      let window = show(VirtualList(list) { QueryRow(id: $0.row.id, seen: seen) }.zero(zero))
      try await run(30) { seen.texts.contains("note 0") }
      #expect(seen.texts.contains("note 0"))
      withExtendedLifetime((zero, window)) {}
    }

    @Observable final class Selection {
      var key: String?
      /// Each key a tap or a key set it to.
      var sets: [String] = []
      /// Whether each row was last shown selected.
      @ObservationIgnored var shown: [String: Bool] = [:]

      var binding: Binding<String?> {
        Binding {
          self.key
        } set: {
          self.key = $0
          if let key = $0 { self.sets.append(key) }
        }
      }
    }

    @Test func selectsByTapAndByKeys() async throws {
      let (zero, list) = try makeList()
      let selection = Selection()
      let window = show(
        VirtualList(list, selection: selection.binding) { item, isSelected in
          let _ = selection.shown[item.row.id] = isSelected
          Text(item.row.text).frame(height: 30)
        })
      try await run(30) { list.snapshot.complete && !list.snapshot.items.isEmpty }
      let view = try #require(rowsScrollView(window))
      try await RowsSelectionCheck(list: list, view: view, window: window) {
        (selection.key, selection.sets, selection.shown)
      }.run(self)
      withExtendedLifetime((zero, window)) {}
    }

    /// The `RowsScrollView` in `window`.
    func rowsScrollView(_ window: AnyObject) -> RowsScrollView? {
      #if canImport(UIKit)
        func find(_ v: UIView) -> RowsScrollView? {
          (v as? RowsScrollView) ?? v.subviews.lazy.compactMap(find).first
        }
        return (window as? UIWindow).flatMap(find)
      #else
        func find(_ v: NSView) -> RowsScrollView? {
          (v as? RowsScrollView) ?? v.subviews.lazy.compactMap(find).first
        }
        return (window as? NSWindow)?.contentView.flatMap(find)
      #endif
    }

    @Test func scrollsToARowFarBelow() async throws {
      let (zero, list) = try makeList()
      let window = show(VirtualList(list) { Text($0.row.text).frame(height: 30) })
      // Generous: other tests in the process can hold the main actor for seconds.
      try await run(30) { list.snapshot.complete && !list.snapshot.items.isEmpty }
      #expect(list.snapshot.items.first?.key == Self.id(0))
      list.scrollToItem(Self.id(250), align: .end)
      try await run(30) { list.lastVisibleItem()?.key == Self.id(250) }
      try await run(0.5) { false }
      #expect(list.lastVisibleItem()?.key == Self.id(250))
      // The scroll view itself moved there, not just the virtualizer's idea
      // of it: n250's bottom at the viewport's. Its rows are 30 points, after
      // the space that stands in for the rows before the window.
      let g = try #require(platformGeometry(window))
      let s = list.snapshot
      // Its place in the window: under a permalink anchor, `index` counts
      // from the window's anchor, not the list's start.
      let position = try #require(s.items.firstIndex { $0.key == Self.id(250) })
      #expect(abs(g.offset - (s.spaceBefore + Double(position + 1) * 30 - g.viewport)) <= 1)
      withExtendedLifetime((zero, window)) {}
    }
  }

  /// Selecting rows of 30 points, ids `n000`…, in a 400-point list: by a
  /// tap, twice on the same row, then on macOS by the arrow keys and j and
  /// k (sent through the window), past the screen's bottom. iOS has no
  /// keys for it: there, a tap neither takes first responder nor offers
  /// key commands.
  @MainActor struct RowsSelectionCheck {
    let list: VirtualListTests.List
    let view: RowsScrollView
    let window: AnyObject
    /// The selection, every key the user set it to, and whether each row
    /// was last shown selected.
    let state: () -> (key: String?, sets: [String], shown: [String: Bool])

    /// The row selected last, and how many times the user selected one:
    /// three taps on iOS; two taps and thirteen keys on macOS.
    #if canImport(UIKit)
      static let last = VirtualListTests.id(7)
      static let selections = 3
    #else
      static let last = VirtualListTests.id(14)
      static let selections = 15
    #endif

    func run(_ t: VirtualListTests) async throws {
      let id = VirtualListTests.id
      // Laid out: the snapshot can complete before the view's next pass.
      try await t.run(5) { view.rows.host.rowFrames().count > 5 }
      try tap(id(5))
      try await t.run(5) { state().key == id(5) }
      #expect(state().key == id(5))
      #expect(state().shown[id(5)] == true)
      try await t.run(0.2) { false }
      try expectHighlight(on: id(5))

      // Again: the app hears of it, to open the row again.
      try tap(id(5))
      try await t.run(5) { state().sets.count == 2 }
      #expect(state().sets == [id(5), id(5)])

      #if canImport(UIKit)
        #expect(!view.isFirstResponder)
        #expect(!view.canBecomeFirstResponder)
        #expect(view.keyCommands?.isEmpty ?? true)
        // Another row, by a tap.
        try tap(id(7))
        try await t.run(5) { state().key == id(7) }
        #expect(state().shown[id(5)] == false)
        #expect(state().shown[id(7)] == true)
        try await t.run(0.2) { false }
        try expectHighlight(on: id(7))
      #else
        try await keys(t)
      #endif
    }

    #if canImport(AppKit)
      /// The arrow keys and j and k, from the tapped row.
      func keys(_ t: VirtualListTests) async throws {
        let id = VirtualListTests.id
        for _ in 0..<10 { key(down: true, letter: false) }
        try await t.run(5) { state().key == id(15) }
        #expect(state().key == id(15))
        // Below the screen at first: scrolled into view, its bottom at the
        // list's.
        try await t.run(5) { list.lastVisibleItem()?.key == id(15) }
        #expect(list.lastVisibleItem()?.key == id(15))
        key(down: false, letter: true)
        key(down: false, letter: true)
        key(down: true, letter: true)
        try await t.run(5) { state().sets.count == 15 }
        #expect(state().key == id(14))
        #expect(state().shown[id(15)] == false)
        #expect(state().shown[id(5)] == false)
        #expect(state().shown[id(14)] == true)
        try await t.run(0.2) { false }
        try expectHighlight(on: id(14))
      }

      /// The down or up arrow, or j or k.
      func key(down: Bool, letter: Bool) {
        let w = window as! NSWindow
        let (characters, code): (String, UInt16) =
          letter
          ? (down ? ("j", 38) : ("k", 40))
          : (
            String(UnicodeScalar(down ? NSDownArrowFunctionKey : NSUpArrowFunctionKey)!),
            down ? 125 : 126
          )
        w.sendEvent(
          NSEvent.keyEvent(
            with: .keyDown, location: .zero, modifierFlags: letter ? [] : [.numericPad, .function],
            timestamp: ProcessInfo.processInfo.systemUptime, windowNumber: w.windowNumber,
            context: nil, characters: characters, charactersIgnoringModifiers: characters,
            isARepeat: false, keyCode: code)!)
      }
    #endif

    func frame(_ key: String) throws -> VirtualRowFrame {
      try #require(view.rows.host.rowFrames().first { $0.key == AnyHashable(key) })
    }

    /// A tap's own handler, at the row's middle: UIKit has no way to send
    /// a touch, nor AppKit a click to a window that isn't key, as a test's
    /// isn't.
    func tap(_ key: String) throws {
      let f = try frame(key)
      view.select(at: (f.minY + f.maxY) / 2)
    }

    /// The highlight stands behind the row's frame.
    func expectHighlight(on key: String) throws {
      let f = try frame(key)
      let h = view.highlight
      #expect(!h.isHidden)
      #expect(
        abs(Double(h.frame.minY) - f.minY) <= 0.5 && abs(Double(h.frame.maxY) - f.maxY) <= 0.5)
    }
  }
#endif
