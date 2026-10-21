import Foundation
import Observation
import ZeroTesting
import ZeroVirtual

/// A client of a `LoopbackServer` holding a long list of issues and a chat
/// of messages of many lengths.
@MainActor
final class Backend {
  let server = LoopbackServer()
  let zero: Zero<Void>
  private(set) var messageCount: Int

  init(issues: Int = 2000, messages: Int = 200) {
    messageCount = messages
    server.put("issues", (0..<issues).map(Self.issue))
    server.put("comments", (0..<messages).map { Self.message($0) })
    let options = ZeroOptions(
      cacheURL: "http://loopback.test", userID: "u", storageKey: UUID().uuidString,
      storage: .memory)
    zero = try! Zero(schema: .test, options: options, socketFactory: server)
  }

  static func issueID(_ i: Int) -> String { "i" + String(format: "%04d", i) }
  static func messageID(_ i: Int) -> String { "m" + String(format: "%04d", i) }

  private static func issue(_ i: Int) -> [String: JSONValue] {
    [
      "id": .string(issueID(i)), "title": .string("Issue \(i)"),
      "description": .string(words(i % 5 == 0 ? 30 : 0, seed: i)), "closed": .bool(i % 3 == 0),
      "owner_id": .null, "createdAt": .number(Double(i)),
    ]
  }

  /// Message `i`: 1 to 60 words, so rows are many heights.
  static func message(_ i: Int, words count: Int? = nil) -> [String: JSONValue] {
    [
      "id": .string(messageID(i)), "authorId": .string(["ana", "ben", "you"][i % 3]),
      "issue_id": "chat", "text": .string("\(i): " + words(count ?? (i * 7919 % 60 + 1), seed: i)),
      "createdAt": .number(Double(1_000 + i)),
    ]
  }

  private static func words(_ n: Int, seed: Int) -> String {
    let lorem = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor"
      .split(separator: " ")
    return (0..<n).map { String(lorem[($0 + seed) % lorem.count]) }.joined(separator: " ")
  }

  /// Another user's message arrives.
  func receive() {
    server.put("comments", Self.message(messageCount))
    messageCount += 1
  }

  /// The last message is edited to be much longer.
  func growLast() {
    server.put("comments", Self.message(messageCount - 1, words: 120))
  }
}

/// Where a chat page starts: the columns it orders by.
struct ChatStart: Equatable, Sendable, Codable {
  var createdAt: Double
  var id: String
}

enum ChatQueries {
  struct Page: Codable, Sendable {
    var limit: Int
    var start: ChatStart?
    var backward: Bool
  }

  static let page = defineQuery("chatPage") { (a: Page) in
    let dir: AST.Direction = a.backward ? .desc : .asc
    var q = testZQL.comment.where(\.issueId, "chat").orderBy(\.createdAt, dir).orderBy(\.id, dir)
    if let s = a.start { q = q.start([\.createdAt: s.createdAt, \.id: s.id]) }
    return q.limit(a.limit)
  }
  static let message = defineQuery("chatMessage") { (id: String) in
    testZQL.comment.where(\.issueId, "chat").where(\.id, id).one()
  }
  static let latest = defineQuery("chatLatest") {
    testZQL.comment.where(\.issueId, "chat").orderBy(\.createdAt, .desc).orderBy(\.id, .desc).one()
  }
}

enum IssueQueries {
  struct Page: Codable, Sendable {
    var limit: Int
    var start: String?
    var backward: Bool
  }

  static let page = defineQuery("issuePage") { (a: Page) in
    var q = testZQL.issue.orderBy(\.id, a.backward ? .desc : .asc)
    if let s = a.start { q = q.start([\.id: s]) }
    return q.limit(a.limit)
  }
  static let issue = defineQuery("issueByID") { (id: String) in
    testZQL.issue.where(\.id, id).one()
  }
}

typealias ChatList = ZeroVirtualizer<Void, TestComment, String, ChatStart, String>
typealias IssueList = ZeroVirtualizer<Void, TestIssue, String, String, String>

extension Backend {
  func chatList() -> ChatList {
    ChatList(
      zero: zero, listContext: "chat", estimatedRowHeight: 80, rowKey: \.id,
      toStartRow: { ChatStart(createdAt: $0.createdAt, id: $0.id) },
      pageQuery: { r, _ in
        ChatQueries.page(.init(limit: r.limit, start: r.start, backward: r.direction == .backward))
      },
      rowQuery: { r, _ in ChatQueries.message(r.id) }, minPageSize: 20, stickToBottom: true)
  }

  func issueList() -> IssueList {
    IssueList(
      zero: zero, listContext: "issues", estimatedRowHeight: 44, rowKey: \.id,
      toStartRow: \.id,
      pageQuery: { r, _ in
        IssueQueries.page(.init(limit: r.limit, start: r.start, backward: r.direction == .backward))
      },
      rowQuery: { r, _ in IssueQueries.issue(r.id) })
  }
}
