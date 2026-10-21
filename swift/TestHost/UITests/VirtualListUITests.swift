import XCTest

/// ZeroVirtual's SwiftUI list (`VirtualList`) in the test app: rows checked
/// by where they are on screen.
@MainActor
class VirtualListUITests: XCTestCase {
  var app: XCUIApplication!
  /// The screens' lists as UIKit or AppKit views (`VirtualListView`).
  var native: Bool { false }

  override func setUp() async throws {
    continueAfterFailure = false
  }

  func launch(_ screen: String) {
    app = XCUIApplication()
    app.launchArguments = ["-screen", screen, "-native", native ? "1" : "0"]
    app.launch()
  }

  var list: XCUIElement { app.scrollViews.firstMatch }

  func row(_ id: String) -> XCUIElement { app.descendants(matching: .any)["row-\(id)"].firstMatch }

  /// The rows on screen, top to bottom.
  func visibleRows() -> [(id: String, frame: CGRect)] {
    // One snapshot of the list, not a query per row: each element's
    // identifier and frame is a round trip to the device.
    guard let snapshot = try? list.snapshot() else { return [] }
    let bounds = snapshot.frame
    var rows: [(id: String, frame: CGRect)] = []
    func walk(_ e: any XCUIElementSnapshot) {
      if e.identifier.hasPrefix("row-") {
        rows.append((String(e.identifier.dropFirst(4)), e.frame))
      }
      e.children.forEach(walk)
    }
    walk(snapshot)
    return
      rows
      .filter { $0.frame.maxY > bounds.minY + 1 && $0.frame.minY < bounds.maxY - 1 }
      .sorted { $0.frame.minY < $1.frame.minY }
  }

  func waitUntil(
    _ what: String, timeout: TimeInterval = 10, file: StaticString = #filePath, line: UInt = #line,
    _ done: () -> Bool
  ) {
    let end = Date().addingTimeInterval(timeout)
    while Date() < end {
      if done() { return }
      RunLoop.current.run(until: Date().addingTimeInterval(0.1))
    }
    XCTFail("timed out waiting until \(what)", file: file, line: line)
  }

  /// Lets a fling come to rest and the list page in what it needs.
  func rest(_ seconds: TimeInterval = 1.5) {
    RunLoop.current.run(until: Date().addingTimeInterval(seconds))
  }

  /// Scrolls toward the top of the list.
  func scrollTowardTop() {
    #if os(macOS)
      list.scroll(byDeltaX: 0, deltaY: 600)
    #else
      list.swipeDown()
    #endif
  }

  /// Scrolls toward the bottom of the list.
  func scrollTowardBottom() {
    #if os(macOS)
      list.scroll(byDeltaX: 0, deltaY: -600)
    #else
      list.swipeUp()
    #endif
  }

  /// The row's bottom is at the list's.
  func atBottom(_ id: String) -> Bool {
    let r = row(id)
    return r.exists && abs(r.frame.maxY - list.frame.maxY) <= 1.5
  }

  /// The top row on screen holds still once the list is at rest.
  func assertHoldsStill(file: StaticString = #filePath, line: UInt = #line) {
    guard let top = visibleRows().first else {
      return XCTFail("no rows on screen", file: file, line: line)
    }
    rest(2.5)
    let now = row(top.id).frame
    XCTAssertEqual(
      now.minY, top.frame.minY, accuracy: 1, "row \(top.id) moved", file: file, line: line)
  }

  // MARK: Chat

  func testChatOpensAtTheLatestMessage() {
    launch("chat")
    waitUntil("the latest message is at the bottom") { atBottom("m0199") }
    XCTAssertGreaterThan(visibleRows().count, 1)
  }

  func testChatFollowsNewMessagesAndALastMessageGrowingTaller() {
    launch("chat")
    waitUntil("the latest message is at the bottom") { atBottom("m0199") }
    app.buttons["receive"].tap()
    waitUntil("a new message is at the bottom") { atBottom("m0200") }
    let height = row("m0200").frame.height
    app.buttons["grow"].tap()
    waitUntil("the grown message is taller, at the bottom") {
      row("m0200").frame.height > height + 20 && atBottom("m0200")
    }
  }

  func testChatScrolledUpStaysAndLatestFollowsAgain() {
    launch("chat")
    waitUntil("the latest message is at the bottom") { atBottom("m0199") }
    scrollTowardTop()
    rest()
    guard let top = visibleRows().first else { return XCTFail("no rows on screen") }
    app.buttons["receive"].tap()
    rest()
    XCTAssertFalse(visibleRows().contains { $0.id == "m0200" }, "followed while scrolled up")
    XCTAssertEqual(row(top.id).frame.minY, top.frame.minY, accuracy: 1)

    app.buttons["latest"].tap()
    waitUntil("the latest message is at the bottom") { atBottom("m0200") }
    app.buttons["receive"].tap()
    waitUntil("a new message is at the bottom again") { atBottom("m0201") }
  }

  func testChatHoldsStillAsOlderMessagesOfManyHeightsLoad() {
    launch("chat")
    waitUntil("the latest message is at the bottom") { atBottom("m0199") }
    var reachedStart = false
    // About 4 messages a swipe, 200 messages: stops once m0000 is in place.
    for _ in 0..<70 where !reachedStart {
      scrollTowardTop()
      rest()
      assertHoldsStill()
      reachedStart = row("m0000").exists && row("m0000").frame.minY >= list.frame.minY - 1
    }
    XCTAssertTrue(reachedStart, "never reached the first message")
    XCTAssertEqual(row("m0000").frame.minY, list.frame.minY, accuracy: 1.5)
  }

  // MARK: A long list

  /// The list's start at its top: the header, and the first issue right
  /// below it.
  func issuesAtStart() -> Bool {
    let header = app.descendants(matching: .any)["header"].firstMatch
    return header.exists && row("i0000").exists
      && abs(header.frame.minY - list.frame.minY) <= 1.5
      && abs(row("i0000").frame.minY - header.frame.maxY) <= 1.5
  }

  func testIssuesPageDownAndBackUp() {
    launch("issues")
    waitUntil("the header and the first issue are at the top") { issuesAtStart() }
    // A swipe moves about 14 issue rows; stop at row 400, not a count.
    var deepest = 0
    for _ in 0..<60 where deepest < 400 {
      scrollTowardBottom()
      rest(1)
      deepest = visibleRows().compactMap { Int($0.id.dropFirst()) }.max() ?? deepest
    }
    XCTAssertGreaterThanOrEqual(deepest, 400, "didn't page far enough down")
    assertHoldsStill()

    var top = false
    for _ in 0..<60 where !top {
      scrollTowardTop()
      rest(1)
      top = issuesAtStart()
    }
    XCTAssertTrue(top, "never got back to the first issue")
  }

  /// The header scrolls with the rows: on screen it pushes them as it
  /// grows; above the screen, they hold still. The footer follows the last
  /// row.
  func testIssuesHeaderAndFooterScrollWithTheRows() {
    launch("issues")
    waitUntil("the header and the first issue are at the top") { issuesAtStart() }
    let header = app.descendants(matching: .any)["header"].firstMatch
    let height = header.frame.height
    app.buttons["grow-header"].tap()
    waitUntil("the header is taller, the first issue below it") {
      header.frame.height > height + 10 && issuesAtStart()
    }

    for _ in 0..<3 {
      scrollTowardBottom()
      rest(1)
    }
    guard let top = visibleRows().first else { return XCTFail("no rows on screen") }
    XCTAssertFalse(header.isHittable, "the header is still on screen")
    app.buttons["grow-header"].tap()
    rest()
    XCTAssertEqual(row(top.id).frame.minY, top.frame.minY, accuracy: 1, "\(top.id) moved")

    app.buttons["end"].tap()
    waitUntil("the last issue is at the bottom") { atBottom("i1999") }
    scrollTowardBottom()
    rest()
    let footer = app.descendants(matching: .any)["footer"].firstMatch
    XCTAssertTrue(footer.exists)
    XCTAssertEqual(footer.frame.minY, row("i1999").frame.maxY, accuracy: 1.5)
    XCTAssertEqual(footer.frame.maxY, list.frame.maxY, accuracy: 1.5)
  }

  /// A tap selects an issue; on macOS, the arrow keys and j and k move the
  /// selection, scrolling it into view.
  func testIssuesSelectByTapAndByKeys() {
    launch("issues")
    waitUntil("the header and the first issue are at the top") { issuesAtStart() }
    let selection = app.staticTexts["selection"]
    // A text's string is its label on iOS, its value on macOS.
    func selected() -> String {
      selection.label.isEmpty ? selection.value as? String ?? "" : selection.label
    }
    #if os(macOS)
      row("i0002").click()
    #else
      row("i0002").tap()
    #endif
    waitUntil("i0002 is selected") { selected() == "i0002" }
    XCTAssertTrue(row("i0002").isSelected)
    #if os(macOS)
      for _ in 0..<3 { app.typeKey(.downArrow, modifierFlags: []) }
      waitUntil("i0005 is selected") { selected() == "i0005" }
      app.typeKey("j", modifierFlags: [])
      waitUntil("i0006 is selected") { selected() == "i0006" }
      app.typeKey("k", modifierFlags: [])
      waitUntil("i0005 is selected again") { selected() == "i0005" }
      // Past the screen's bottom: the list follows the selection.
      for _ in 0..<25 { app.typeKey(.downArrow, modifierFlags: []) }
      waitUntil("i0030 is selected") { selected() == "i0030" }
      rest(0.5)
      XCTAssertTrue(row("i0030").isSelected)
      XCTAssertLessThanOrEqual(row("i0030").frame.maxY, list.frame.maxY + 1)
      XCTAssertGreaterThanOrEqual(row("i0030").frame.minY, list.frame.minY - 1)
      XCTAssertFalse(row("i0002").exists && row("i0002").isSelected)
    #endif
  }
}

/// The same, with the screens' lists as UIKit or AppKit views.
final class NativeVirtualListUITests: VirtualListUITests {
  override var native: Bool { true }
}
