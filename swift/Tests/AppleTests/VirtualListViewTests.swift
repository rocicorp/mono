#if canImport(SwiftUI)
  import Foundation
  import Testing
  import ZeroTesting

  @testable import Zero
  @testable import ZeroVirtual

  #if canImport(UIKit)
    import UIKit
    typealias TestView = UIView
  #else
    import AppKit
    typealias TestView = NSView
  #endif

  /// `VirtualListView` in a real window, on a `LoopbackServer`, with rows
  /// that are plain views.
  @Suite(.serialized) @MainActor struct VirtualListViewTests {
    /// A 30-point row showing a note's id.
    final class NoteView: TestView {
      var id = ""
      init() {
        super.init(frame: .zero)
        translatesAutoresizingMaskIntoConstraints = true
        let height = heightAnchor.constraint(equalToConstant: 30)
        height.isActive = true
      }
      required init?(coder: NSCoder) { fatalError() }
    }

    /// A 300 × 400 window showing `view`.
    func show(_ view: TestView) -> AnyObject {
      #if canImport(UIKit)
        let window = UIWindow(frame: CGRect(x: 0, y: 0, width: 300, height: 400))
        let controller = UIViewController()
        controller.view = view
        window.rootViewController = controller
        window.makeKeyAndVisible()
        return window
      #else
        let window = NSWindow(
          contentRect: CGRect(x: 0, y: 0, width: 300, height: 400), styleMask: [.titled],
          backing: .buffered, defer: false)
        window.contentView = view
        window.makeKeyAndOrderFront(nil)
        return window
      #endif
    }

    @Test func scrollsToARowFarBelow() async throws {
      let t = VirtualListTests()
      let (zero, list) = try t.makeList()
      let view = VirtualListView(list) { item, reusing in
        let row = reusing as? NoteView ?? NoteView()
        row.id = item.row.id
        return row
      }
      let window = show(view)
      try await t.run(30) { list.snapshot.complete && !list.snapshot.items.isEmpty }
      #expect(list.snapshot.items.first?.key == VirtualListTests.id(0))
      list.scrollToItem(VirtualListTests.id(250), align: .end)
      try await t.run(30) { list.lastVisibleItem()?.key == VirtualListTests.id(250) }
      try await t.run(0.5) { false }
      #expect(list.lastVisibleItem()?.key == VirtualListTests.id(250))
      // The scroll view moved there: n250's bottom at the viewport's.
      let g = try #require(t.platformGeometry(window))
      let s = list.snapshot
      let position = try #require(s.items.firstIndex { $0.key == VirtualListTests.id(250) })
      #expect(abs(g.offset - (s.spaceBefore + Double(position + 1) * 30 - g.viewport)) <= 1)
      withExtendedLifetime((zero, window)) {}
    }

    @Test func aReusedViewShowsItsNewRow() async throws {
      let t = VirtualListTests()
      let (zero, list) = try t.makeList()
      var reused = 0
      let view = VirtualListView(list) { item, reusing in
        let row = reusing as? NoteView ?? NoteView()
        if reusing != nil { reused += 1 }
        row.id = item.row.id
        return row
      }
      let window = show(view)
      try await t.run(30) { list.snapshot.complete && !list.snapshot.items.isEmpty }
      // Scrolled down a few screens, as a user would: rows leave at the
      // top, and their views come back below.
      for _ in 0..<12 {
        scroll(view.scrollView, by: 100)
        try await t.run(0.05) { false }
      }
      try await t.run(1) { false }
      #expect(reused > 0)
      // Each view on screen shows the row at its place.
      let s = list.snapshot
      let shown = rowViews(view.scrollView)
      #expect(!shown.isEmpty)
      for row in shown {
        let i = Int(((Double(row.frame.minY) - s.spaceBefore) / 30).rounded())
        #expect(s.items.indices.contains(i) && s.items[i].key == row.id, "\(row.id) at \(i)")
      }
      withExtendedLifetime((zero, window)) {}
    }

    // Switching from one list to another on the same virtualizer: the first
    // one leaving after the second came doesn't detach the second.
    @Test func aSecondListTakesTheVirtualizerOver() async throws {
      let t = VirtualListTests()
      let (zero, list) = try t.makeList()
      func make() -> VirtualListView<Void, Note, String, String, String> {
        VirtualListView(list) { item, reusing in
          let row = reusing as? NoteView ?? NoteView()
          row.id = item.row.id
          return row
        }
      }
      let first = make()
      var firstWindow: AnyObject? = show(first)
      try await t.run(30) { list.snapshot.complete && !list.snapshot.items.isEmpty }
      let second = make()
      let window = show(second)
      try await t.run(0.2) { false }
      #if canImport(UIKit)
        (firstWindow as? UIWindow)?.isHidden = true
        first.removeFromSuperview()
      #else
        (firstWindow as? NSWindow)?.orderOut(nil)
        (firstWindow as? NSWindow)?.contentView = nil
      #endif
      firstWindow = nil
      try await t.run(0.2) { false }
      list.scrollToItem(VirtualListTests.id(250), align: .end)
      try await t.run(30) { list.lastVisibleItem()?.key == VirtualListTests.id(250) }
      #expect(list.lastVisibleItem()?.key == VirtualListTests.id(250))
      #expect(rowViews(second.scrollView).contains { $0.id == VirtualListTests.id(250) })
      withExtendedLifetime((zero, window)) {}
    }

    @Test func selectsByTapAndByKeys() async throws {
      let t = VirtualListTests()
      let (zero, list) = try t.makeList()
      var sets: [String] = []
      var shown: [String: Bool] = [:]
      let view = VirtualListView(list) { item, isSelected, reusing in
        let row = reusing as? NoteView ?? NoteView()
        row.id = item.row.id
        shown[item.row.id] = isSelected
        return row
      }
      view.onSelect = { sets.append($0) }
      let window = show(view)
      try await t.run(30) { list.snapshot.complete && !list.snapshot.items.isEmpty }
      let rows = try #require(view.scrollView as? RowsScrollView)
      try await RowsSelectionCheck(list: list, view: rows, window: window) {
        (view.selection, sets, shown)
      }.run(t)
      // The selected row's view, and no other, is selected for
      // accessibility.
      let selected = rowViews(view.scrollView).filter {
        #if canImport(UIKit)
          $0.accessibilityTraits.contains(.selected)
        #else
          $0.isAccessibilitySelected()
        #endif
      }
      #expect(selected.map(\.id) == [RowsSelectionCheck.last])
      // Set by the app: highlighted, and not reported back.
      view.selection = VirtualListTests.id(3)
      try await t.run(0.2) { false }
      #expect(shown[VirtualListTests.id(3)] == true && shown[RowsSelectionCheck.last] == false)
      #expect(sets.count == RowsSelectionCheck.selections)
      withExtendedLifetime((zero, window)) {}
    }

    func scroll(_ scrollView: AnyObject, by dy: Double) {
      #if canImport(UIKit)
        let s = scrollView as! UIScrollView
        s.contentOffset.y += dy
        s.layoutIfNeeded()
      #else
        let s = scrollView as! NSScrollView
        let clip = s.contentView
        clip.scroll(to: NSPoint(x: 0, y: clip.bounds.origin.y + dy))
        s.reflectScrolledClipView(clip)
      #endif
    }

    /// The row views drawn.
    func rowViews(_ scrollView: AnyObject) -> [NoteView] {
      #if canImport(UIKit)
        let views = (scrollView as! UIScrollView).subviews
      #else
        let views = (scrollView as! NSScrollView).documentView?.subviews ?? []
      #endif
      return views.compactMap { $0 as? NoteView }.filter { !$0.isHidden }
    }
  }
#endif
