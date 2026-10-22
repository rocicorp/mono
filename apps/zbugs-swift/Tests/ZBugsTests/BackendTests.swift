import Foundation
import Testing
import ZBugs
import Zero

/// Where zbugs' backend (server/server.ts) runs, and a token it accepts.
enum Backend {
  static let env = ProcessInfo.processInfo.environment
  static let cacheURL = env["ZBUGS_E2E_URL"]
  static let apiURL = env["ZBUGS_E2E_API"]
  static let token = env["ZBUGS_E2E_TOKEN"]
  static var enabled: Bool { cacheURL != nil && apiURL != nil && token != nil }
}

/// zbugs' Swift port against zbugs' backend: zero-cache asks the API server
/// for the queries the client names, and the API server runs the TS
/// mutators the client ran optimistically.
@Suite(.enabled(if: Backend.enabled), .timeLimit(.minutes(2)))
@MainActor
struct BackendTests {
  func makeZero() throws -> Zero<AuthData?> {
    let token = Backend.token!
    let auth = AuthData(jwt: token, now: Date().timeIntervalSince1970)
    var options = ZeroOptions(
      cacheURL: Backend.cacheURL, userID: auth?.sub, auth: token, storage: .memory)
    options.mutateURL = Backend.apiURL! + "/api/mutate"
    options.queryURL = Backend.apiURL! + "/api/query"
    return try Zero(
      schema: .zbugs, context: auth, mutators: Mutators.all, options: options,
      socketFactory: TransportSocketFactory(NIOTransport()))
  }

  /// Waits until `condition` holds.
  func eventually(
    _ what: String, timeout: Duration = .seconds(30), _ condition: () -> Bool
  ) async throws {
    let deadline = ContinuousClock.now + timeout
    while !condition() {
      guard ContinuousClock.now < deadline else {
        Issue.record("Timed out waiting for \(what)")
        throw CancellationError()
      }
      try await Task.sleep(for: .milliseconds(20))
    }
  }

  static let openIssues = ListContextParams(
    open: true, assignee: nil, creator: nil, labels: [], textFilter: nil, sortField: .modified,
    sortDirection: .desc)

  @Test func listsCreatesAndComments() async throws {
    let zero = try makeZero()
    defer { zero.close() }
    let list = try zero.liveQuery(
      Queries.issueListV2(
        IssueListV2Args(listContext: Self.openIssues, limit: 20, start: nil, dir: .forward)))
    try await eventually("the issue list") { list.resultType == .complete }
    #expect(list.data.count == 20)
    #expect(list.data.allSatisfy { $0.open && $0.creator != nil })
    #expect(list.data.map(\.modified) == list.data.map(\.modified).sorted(by: >))

    // The server runs the TS mutator too, and its trigger numbers the issue.
    let projects = try zero.liveQuery(Queries.allProjects())
    try await eventually("the projects") { projects.resultType == .complete }
    let project = try #require(projects.data.first { $0.name == zeroProjectName })
    let id = makeID()
    let now = (Date().timeIntervalSince1970 * 1000).rounded()
    let created = zero.mutate(
      Mutators.createIssue(
        CreateIssueArgs(
          id: id, title: "From Swift", description: "Hello", created: now, modified: now,
          projectID: project.id)))
    #expect(try await created.client.value == .success(data: nil))
    #expect(list.data.first?.id == id)
    #expect(try await created.server.value == .success(data: nil))

    let detail = try zero.liveQuery(
      Queries.issueDetail(IssueDetailArgs(idField: .id, id: .string(id))))
    try await eventually("the new issue's number") { detail.data?.shortID != nil }
    let comment = zero.mutate(
      Mutators.addComment(
        AddCommentArgs(id: makeID(), issueID: id, body: "First", created: now + 1)))
    #expect(try await comment.server.value == .success(data: nil))
    try await eventually("the comment") { detail.data?.comments?.map(\.body) == ["First"] }
    // A crew member's issue notifies them.
    #expect(detail.data?.notificationState?.subscribed == true)

    // Another client sees it through the server, by its number.
    let other = try makeZero()
    defer { other.close() }
    let shortID = try #require(detail.data?.shortID)
    let byNumber = try other.liveQuery(
      Queries.issueDetail(IssueDetailArgs(idField: .shortID, id: .number(Double(shortID)))))
    try await eventually("the issue on another client") { byNumber.resultType == .complete }
    #expect(byNumber.data?.id == id)
    #expect(byNumber.data?.title == "From Swift")
    #expect(byNumber.data?.comments?.count == 1)
  }

  @Test func reportsWhatTheServerRejects() async throws {
    let zero = try makeZero()
    defer { zero.close() }
    let list = try zero.liveQuery(
      Queries.issueListV2(
        IssueListV2Args(listContext: Self.openIssues, limit: 1, start: nil, dir: .forward)))
    try await eventually("the issue list") { list.resultType == .complete }
    // A label for an issue that doesn't exist: the client and the server
    // both refuse, with zbugs' error.
    let result = zero.mutate(
      Mutators.createAndAddLabel(
        CreateAndAddLabelArgs(issueID: "missing", labelID: makeID(), labelName: "x")))
    let expected = MutationOutcome.appError(
      message: "Issue not found", details: ["code": "ENTITY_NOT_FOUND", "id": "missing"])
    #expect(try await result.client.value == expected)
    #expect(try await result.server.value == expected)
  }
}
