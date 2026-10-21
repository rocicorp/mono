import XCTest

/// Drives the app against zbugs' backend (`server/server.ts`) and keeps a
/// screenshot of each screen, which CI uploads. `ZBUGS_TOKEN` is a token for
/// the seeded crew user; xcodebuild passes `TEST_RUNNER_ZBUGS_TOKEN` as it,
/// and likewise `ZBUGS_CACHE_URL` and `ZBUGS_API_URL` when they're set.
///
/// A step whose screen doesn't appear records a failure, with the app's
/// accessibility tree, and the test goes on, so one run shows every screen
/// it can reach.
final class ScreenshotTests: XCTestCase {
  @MainActor
  func testScreens() throws {
    let environment = ProcessInfo.processInfo.environment
    let token = try XCTUnwrap(environment["ZBUGS_TOKEN"], "Set TEST_RUNNER_ZBUGS_TOKEN")
    #if os(iOS)
      if UIDevice.current.userInterfaceIdiom == .pad {
        XCUIDevice.shared.orientation = .landscapeLeft
      }
    #endif
    let app = XCUIApplication()
    app.launchEnvironment["ZBUGS_TOKEN"] = token
    // On a device, the backend's network address.
    for key in ["ZBUGS_CACHE_URL", "ZBUGS_API_URL"] {
      app.launchEnvironment[key] = environment[key]
    }
    app.launch()
    #if os(iOS) && !targetEnvironment(simulator)
      // A device asks for local network access on the first launch.
      let allow = XCUIApplication(bundleIdentifier: "com.apple.springboard").alerts.buttons["Allow"]
      if allow.waitForExistence(timeout: 10) { allow.tap() }
    #endif

    // The issue list, once zero-cache has synced it.
    let firstIssue = app.descendants(matching: .any).matching(identifier: "IssueRow").firstMatch
    guard expect(app, firstIssue, "an issue in the list", timeout: 120) else { return }
    settle()
    snapshot(app, "1 Issues")

    // An issue with its comments.
    press(firstIssue)
    if expect(app, app.buttons["Comment"], "the issue's comment button") {
      settle()
      snapshot(app, "2 Issue")
    }

    // A new issue.
    if isPhone {
      back(app)
    }
    let newIssue = app.buttons["New Issue"]
    if expect(app, newIssue, "the New Issue button") {
      press(newIssue)
      if expect(app, app.buttons["Create"], "the new issue sheet") {
        settle()
        snapshot(app, "3 New Issue")
        press(app.buttons["Cancel"])
      }
    }

    // The filters, then the UIKit or AppKit list.
    if isPhone {
      back(app)
    }
    #if os(macOS)
      // The pop-up button has no label; its title is a text beside it.
      let listPicker = app.popUpButtons.matching(NSPredicate(format: "value == 'SwiftUI'"))
        .firstMatch
      let native = app.menuItems["AppKit"]
    #else
      // A menu picker's label is its title and its value.
      let listPicker = app.buttons.matching(NSPredicate(format: "label BEGINSWITH 'List,'"))
        .firstMatch
      let native = app.buttons["UIKit"]
    #endif
    if expect(app, listPicker, "the filters' List picker") {
      settle()
      snapshot(app, "4 Filters")
      press(listPicker)
      if expect(app, native, "the native list option") {
        press(native)
      }
      if isPhone {
        let issues = app.buttons["Issues"]
        if expect(app, issues, "the Issues button") { press(issues) }
      }
      let cell = app.descendants(matching: .any).matching(identifier: "IssueCell").firstMatch
      if expect(app, cell, "an issue in the native list") {
        settle()
        snapshot(app, "5 Issues (native)")
      }
    }
  }

  /// All the issues, more than the 200 the list's first page had before it
  /// paged: scrolled down past 200 of them, they keep coming.
  @MainActor
  func testListPagesPastTheFirst200() throws {
    let environment = ProcessInfo.processInfo.environment
    let token = try XCTUnwrap(environment["ZBUGS_TOKEN"], "Set TEST_RUNNER_ZBUGS_TOKEN")
    let app = XCUIApplication()
    app.launchEnvironment["ZBUGS_TOKEN"] = token
    for key in ["ZBUGS_CACHE_URL", "ZBUGS_API_URL"] {
      app.launchEnvironment[key] = environment[key]
    }
    app.launch()
    let rows = app.descendants(matching: .any).matching(identifier: "IssueRow")
    guard expect(app, rows.firstMatch, "an issue in the list", timeout: 120) else { return }

    // Status: All.
    if isPhone {
      back(app)
    }
    #if os(macOS)
      let status = app.popUpButtons.matching(NSPredicate(format: "value == 'Open'")).firstMatch
      let all = app.menuItems["All"]
    #else
      let status = app.buttons.matching(NSPredicate(format: "label BEGINSWITH 'Status,'"))
        .firstMatch
      let all = app.buttons["All"]
    #endif
    guard expect(app, status, "the Status picker") else { return }
    press(status)
    guard expect(app, all, "the All status") else { return }
    press(all)
    if isPhone {
      let issues = app.buttons["Issues"]
      if expect(app, issues, "the Issues button") { press(issues) }
    }
    guard expect(app, rows.firstMatch, "an issue in the list of all issues") else { return }
    settle()

    let list = app.scrollViews.containing(NSPredicate(format: "identifier == 'IssueRow'"))
      .firstMatch
    var seen: Set<String> = []
    for _ in 0..<120 where seen.count <= 200 {
      if let snapshot = try? list.snapshot() {
        func walk(_ e: any XCUIElementSnapshot) {
          if e.identifier == "IssueRow" { seen.insert(e.label) }
          e.children.forEach(walk)
        }
        walk(snapshot)
      }
      #if os(macOS)
        list.scroll(byDeltaX: 0, deltaY: -800)
      #else
        list.swipeUp(velocity: .fast)
      #endif
      Thread.sleep(forTimeInterval: 0.5)
    }
    snapshot(app, "6 Issues past 200")
    XCTAssertGreaterThan(seen.count, 200, "the list stopped at \(seen.count) issues")
  }

  @MainActor
  private var isPhone: Bool {
    #if os(iOS)
      UIDevice.current.userInterfaceIdiom == .phone
    #else
      false
    #endif
  }

  /// Waits for `element`. When it doesn't appear, records a failure with a
  /// screenshot and the accessibility tree.
  @MainActor
  private func expect(
    _ app: XCUIApplication, _ element: XCUIElement, _ what: String, timeout: Double = 20
  ) -> Bool {
    if element.waitForExistence(timeout: timeout) { return true }
    snapshot(app, "Missing \(what)")
    let tree = XCTAttachment(string: app.debugDescription)
    tree.name = "Missing \(what).txt"
    tree.lifetime = .keepAlways
    add(tree)
    XCTFail("No \(what)")
    return false
  }

  @MainActor
  private func press(_ element: XCUIElement) {
    #if os(macOS)
      element.click()
    #else
      element.tap()
    #endif
  }

  /// The navigation bar's back button.
  @MainActor
  private func back(_ app: XCUIApplication) {
    let button = app.navigationBars.buttons.element(boundBy: 0)
    if button.waitForExistence(timeout: 5) { press(button) }
  }

  /// Lets animations and the rows' relationships finish.
  private func settle() {
    Thread.sleep(forTimeInterval: 2)
  }

  /// The screen on iOS, the window and its sheets on macOS.
  @MainActor
  private func snapshot(_ app: XCUIApplication, _ name: String) {
    // On an iPad in landscape, a PNG of the portrait screen's pixels with an
    // EXIF orientation that most viewers apply.
    #if os(macOS)
      let screenshot = app.windows.firstMatch.screenshot()
    #else
      let screenshot = XCUIScreen.main.screenshot()
    #endif
    let attachment = XCTAttachment(screenshot: screenshot)
    attachment.name = name
    attachment.lifetime = .keepAlways
    add(attachment)
  }
}
