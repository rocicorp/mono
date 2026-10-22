import Foundation
import Testing
import ZeroTesting

@testable import Zero
@testable import ZeroVirtual

/// `ZeroVirtualizer` over a client: its queries go to a server the test
/// plays, which answers every query it's asked for from 300 issues.
@MainActor
@Suite struct ZeroVirtualizerTests {
  struct PageArgs: Codable, Sendable {
    var limit: Int
    var start: String?
    var backward: Bool
    var closed: Bool?
  }

  static let page = defineQuery("issuePage") { (args: PageArgs) in
    var q = testZQL.issue.orderBy(\.id, args.backward ? .desc : .asc)
    if let closed = args.closed { q = q.where(\.closed, closed) }
    if let start = args.start { q = q.start([\.id: start]) }
    return q.limit(args.limit)
  }
  static let byID = defineQuery("issueByID") { (id: String) in
    testZQL.issue.where(\.id, id).one()
  }

  static func id(_ i: Int) -> String { "i" + String(format: "%03d", i) }

  let scheduler = ManualScheduler(now: 1_700_000_000_000)
  let sockets = MockSocketFactory()
  private let server = Server()

  typealias List = ZeroVirtualizer<Void, TestIssue, String, String, Bool?>

  func makeList(closed: Bool? = nil, permalinkID: String? = nil) throws -> (Zero<Void>, List) {
    var options = ZeroOptions(
      cacheURL: "http://zero.test", userID: "u", storageKey: randomID(), storage: .memory)
    options.logSink = RecordingLogSink()
    let zero = try Zero(
      schema: .test, options: options, scheduler: scheduler, socketFactory: sockets)
    let list = List(
      zero: zero, listContext: closed, estimatedRowHeight: 20, rowKey: \.id, toStartRow: \.id,
      pageQuery: { r, closed in
        Self.page(
          PageArgs(
            limit: r.limit, start: r.start, backward: r.direction == .backward, closed: closed))
      },
      rowQuery: { r, _ in Self.byID(r.id) }, permalinkID: permalinkID, minPageSize: 100,
      scheduler: scheduler)
    return (zero, list)
  }

  /// One render and a server round: what the list asked for is answered.
  func settle(_ list: List, _ view: ListView) {
    for _ in 0..<30 {
      server.serve(sockets, scheduler)
      view.render(list)
      let before = list.core.queryInputs
      list.afterUpdate()
      // A new space before the rows renders again, as a view would.
      for _ in 0..<3 where list.snapshot.spaceBefore != view.spaceBefore {
        view.render(list)
        list.afterUpdate()
      }
      server.serve(sockets, scheduler)
      if list.core.queryInputs == before && list.snapshot.complete { return }
    }
    Issue.record("did not settle")
  }

  @Test func pagesThroughTheList() throws {
    let (zero, list) = try makeList()
    let view = ListView()
    list.attach(view)
    settle(list, view)
    #expect(list.snapshot.items.map(\.key) == (0..<100).map(Self.id))
    #expect(list.snapshot.items.first?.index == 0)

    for offset in stride(from: 1500.0, through: 5000, by: 700) {
      view.scroll(list, to: offset)
      settle(list, view)
    }
    let s = list.snapshot
    // The rows are where they are in the list, with the ones on screen loaded.
    for item in s.items { #expect(item.key == Self.id(item.index)) }
    #expect(view.visibleIndexes.allSatisfy { i in s.items.contains { $0.index == i } })
    // Near the end: the window reaches the last row.
    #expect(s.items.last?.key == Self.id(299))
    #expect(s.items.first!.index > 150)
    #expect(s.total == 300)
    withExtendedLifetime(zero) {}
  }

  @Test func opensAtAPermalinkWithAPageEitherSide() throws {
    let (zero, list) = try makeList(permalinkID: Self.id(150))
    let view = ListView()
    list.attach(view)
    settle(list, view)
    let keys = list.snapshot.items.map(\.key)
    #expect(keys.contains(Self.id(150)))
    // Half a page before it, and as much after as a page holds.
    #expect(keys.first == Self.id(100))
    #expect(keys.last == Self.id(199))
    #expect(view.top(of: Self.id(150)) == 0)
    withExtendedLifetime(zero) {}
  }

  @Test func aJumpToAnIDWithNoRowDoesNothing() throws {
    let (zero, list) = try makeList()
    let view = ListView()
    list.attach(view)
    settle(list, view)
    let before = list.snapshot.items.map(\.key)
    list.scrollToItem("nope")
    settle(list, view)
    #expect(list.snapshot.items.map(\.key) == before)

    list.scrollToItem(Self.id(250), align: .start)
    settle(list, view)
    #expect(view.top(of: Self.id(250)) == 0)
    withExtendedLifetime(zero) {}
  }

  @Test func aNewListContextStartsOver() throws {
    let (zero, list) = try makeList()
    let view = ListView()
    list.attach(view)
    settle(list, view)
    view.scroll(list, to: 1500)
    settle(list, view)
    list.listContext = true
    settle(list, view)
    // The closed issues are every third one.
    #expect(list.snapshot.items.first?.key == Self.id(0))
    #expect(list.snapshot.items.map(\.key).prefix(3) == [Self.id(0), Self.id(3), Self.id(6)])
    #expect(view.offset == 0)
    #expect(list.snapshot.total == 100)
    withExtendedLifetime(zero) {}
  }

  /// Polls until `done`, on real time.
  func eventually(
    _ done: () -> Bool, sourceLocation: SourceLocation = #_sourceLocation
  ) async throws {
    for _ in 0..<500 {
      if done() { return }
      try await Task.sleep(for: .milliseconds(10))
    }
    Issue.record("timed out", sourceLocation: sourceLocation)
  }

  @Test func pagesOverALoopbackServerOnRealTime() async throws {
    let server = LoopbackServer()
    func issue(_ i: Int) -> JSONObject {
      [
        "id": .string(Self.id(i)), "title": "", "description": "", "closed": .bool(false),
        "owner_id": .null, "createdAt": 1,
      ]
    }
    server.put("issues", (0..<150).map(issue))
    var options = ZeroOptions(
      cacheURL: "http://zero.test", userID: "u", storageKey: randomID(), storage: .memory)
    options.logSink = RecordingLogSink()
    let zero = try Zero(schema: .test, options: options, socketFactory: server)
    let list = List(
      zero: zero, listContext: nil, estimatedRowHeight: 20, rowKey: \.id, toStartRow: \.id,
      pageQuery: { r, _ in
        Self.page(PageArgs(limit: r.limit, start: r.start, backward: r.direction == .backward))
      },
      rowQuery: { r, _ in Self.byID(r.id) }, minPageSize: 100)
    let view = ListView()
    list.attach(view)
    view.render(list)
    list.afterUpdate()
    try await eventually { list.snapshot.complete && list.snapshot.items.count == 100 }
    #expect(list.snapshot.items.first?.key == Self.id(0))

    // Rows the server adds later sync, and the list pages to them.
    server.put("issues", (150..<160).map(issue))
    view.scroll(list, to: .infinity)
    // Until the last page is there and its query confirmed.
    for _ in 0..<40
    where list.snapshot.items.last?.key != Self.id(159) || !list.snapshot.complete {
      view.render(list)
      list.afterUpdate()
      view.scroll(list, to: .infinity)
      try await Task.sleep(for: .milliseconds(50))
    }
    #expect(list.snapshot.items.last?.key == Self.id(159))
    #expect(list.snapshot.complete)
    #expect(list.error == nil)
    withExtendedLifetime(zero) {}
  }
}

/// A server that sends every row at first, then reports each query the
/// client wants as got.
@MainActor
private final class Server {
  var connected = false
  var got: Set<String> = []
  var cookie = 0

  static func row(_ i: Int) -> String {
    let id = ZeroVirtualizerTests.id(i)
    return
      #"{"op":"put","tableName":"issues","value":{"id":"\#(id)","title":"","description":"","closed":\#(i % 3 == 0),"owner_id":null,"createdAt":1}}"#
  }

  func serve(_ sockets: MockSocketFactory, _ scheduler: ManualScheduler) {
    // Past the client's throttle of query changes.
    scheduler.advance(by: 100)
    guard let socket = sockets.sockets.last else { return }
    var rows = ""
    if !connected {
      connected = true
      socket.receive(text: #"["connected",{"wsid":"w"}]"#)
      scheduler.runDueTimers()
      rows = #","rowsPatch":["# + (0..<300).map(Self.row).joined(separator: ",") + "]"
    }
    var wanted: [String] = []
    var texts = socket.messages
    if let header = socket.secProtocol, let decoded = try? SecProtocols.decode(header) {
      texts.append(JSONSerializer.stringify(decoded))
    }
    for text in texts {
      guard let json = try? JSONParser.parse(Array(text.utf8)) else { continue }
      collect(json, &wanted)
    }
    let new = wanted.filter { !got.contains($0) }
    guard !new.isEmpty || !rows.isEmpty else { return }
    got.formUnion(new)
    let base = cookie == 0 ? "null" : "\"\(String(format: "%04d", cookie))\""
    cookie += 1
    let patch = new.map { #"{"op":"put","hash":"\#($0)"}"# }.joined(separator: ",")
    socket.receive(text: #"["pokeStart",{"pokeID":"p\#(cookie)","baseCookie":\#(base)}]"#)
    socket.receive(
      text: #"["pokePart",{"pokeID":"p\#(cookie)","gotQueriesPatch":[\#(patch)]\#(rows)}]"#)
    socket.receive(
      text: #"["pokeEnd",{"pokeID":"p\#(cookie)","cookie":"\#(String(format: "%04d", cookie))"}]"#)
    scheduler.runDueTimers()
  }

  private func collect(_ json: JSONValue, _ out: inout [String]) {
    if let a = json.arrayValue {
      for v in a { collect(v, &out) }
    } else if let o = json.objectValue {
      if o["op"]?.stringValue == "put", let hash = o["hash"]?.stringValue, o["name"] != nil {
        out.append(hash)
      }
      for v in o.values { collect(v, &out) }
    }
  }
}

/// A scroll view of 20-point rows, 400 points tall.
@MainActor
final class ListView: VirtualHost {
  var offset = 0.0
  var frames: [VirtualRowFrame] = []
  var height = 0.0
  var spaceBefore = 0.0
  let viewportHeight = 400.0
  let viewportInsets: (top: Double, bottom: Double) = (0, 0)

  var scrollOffset: Double { offset }
  var contentHeight: Double { height }
  weak var list: ZeroVirtualizerTests.List?

  func scroll(to target: Double) {
    let next = max(0, min(target, height - viewportHeight))
    guard next != offset else { return }
    offset = next
    list?.didScroll()
  }
  func rowFrames() -> [VirtualRowFrame] { frames }

  func render(_ list: ZeroVirtualizerTests.List) {
    self.list = list
    let s = list.snapshot
    spaceBefore = s.spaceBefore
    var y = s.spaceBefore
    frames = s.items.map { item in
      defer { y += 20 }
      return VirtualRowFrame(index: item.index, key: AnyHashable(item.key), minY: y, maxY: y + 20)
    }
    height = y + s.spaceAfter
    scroll(to: offset)
  }

  func scroll(_ list: ZeroVirtualizerTests.List, to target: Double) {
    self.list = list
    scroll(to: target)
  }

  var visibleIndexes: [Int] {
    frames.filter { $0.maxY > offset && $0.minY < offset + viewportHeight }.map(\.index)
  }

  func top(of key: String) -> Double? {
    frames.first { $0.key == AnyHashable(key) }.map { $0.minY - offset }
  }
}
