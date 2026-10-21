import Zero

// The queries of apps/zbugs/shared/queries.ts. The drift test checks that
// each builds the same AST as its TS definition.

public let zeroProjectName = "Zero"

public enum SortField: String, Codable, Sendable, Hashable {
  case modified, created
}

/// The filters and sort of an issue list (`listContextParams`).
public struct ListContextParams: Codable, Sendable, Hashable {
  @Nullable public var open: Bool?
  public var projectName: String?
  @Nullable public var assignee: String?
  @Nullable public var creator: String?
  @Nullable public var labels: [String]?
  @Nullable public var textFilter: String?
  public var sortField: SortField
  public var sortDirection: AST.Direction

  public init(
    open: Bool?, projectName: String? = nil, assignee: String?, creator: String?,
    labels: [String]?, textFilter: String?, sortField: SortField, sortDirection: AST.Direction
  ) {
    self.open = open
    self.projectName = projectName
    self.assignee = assignee
    self.creator = creator
    self.labels = labels
    self.textFilter = textFilter
    self.sortField = sortField
    self.sortDirection = sortDirection
  }
}

/// Where an issue list starts: the issue's sort keys (`issueRowSortSchema`).
public struct IssueRowSort: Codable, Sendable, Hashable {
  public var id: String
  public var created: Double
  public var modified: Double

  public init(id: String, created: Double, modified: Double) {
    self.id = id
    self.created = created
    self.modified = modified
  }
}

public enum PageDirection: String, Codable, Sendable, Hashable {
  case forward, backward
}

public enum IDField: String, Codable, Sendable, Hashable {
  case shortID, id
}

/// An issue's `id` or its `shortID`.
public enum IssueKey: Codable, Sendable, Hashable {
  case string(String)
  case number(Double)

  public init(from decoder: any Decoder) throws {
    let c = try decoder.singleValueContainer()
    if let s = try? c.decode(String.self) {
      self = .string(s)
    } else {
      self = .number(try c.decode(Double.self))
    }
  }

  public func encode(to encoder: any Encoder) throws {
    var c = encoder.singleValueContainer()
    switch self {
    case .string(let s): try c.encode(s)
    case .number(let n): try c.encode(n)
    }
  }

  var json: JSONValue {
    switch self {
    case .string(let s): .string(s)
    case .number(let n): .number(n)
    }
  }

  /// An issue's id, or its short id when it's all digits
  /// (`getIDFromString` in `shared/issue-id.ts`).
  public static func parse(_ s: String) -> (field: IDField, value: IssueKey) {
    if s.contains(where: { !("0"..."9").contains($0) }) { return (.id, .string(s)) }
    return (.shortID, .number(Double(s) ?? .nan))
  }
}

public struct ProjectArgs: Codable, Sendable, Hashable {
  public var projectName: String
  public init(projectName: String) { self.projectName = projectName }
}

public struct LabelsOrderByNameArgs: Codable, Sendable, Hashable {
  public var projectName: String
  public var orderBy: AST.Direction
  public init(projectName: String, orderBy: AST.Direction) {
    self.projectName = projectName
    self.orderBy = orderBy
  }
}

public struct UsersForProjectArgs: Codable, Sendable, Hashable {
  public enum Filter: String, Codable, Sendable, Hashable {
    case crew, creators, assignees
  }

  public var disabled: Bool
  public var login: String?
  public var projectName: String
  public var filter: Filter?

  public init(disabled: Bool, login: String? = nil, projectName: String, filter: Filter? = nil) {
    self.disabled = disabled
    self.login = login
    self.projectName = projectName
    self.filter = filter
  }
}

public struct CommentsPageArgs: Codable, Sendable, Hashable {
  public struct Cursor: Codable, Sendable, Hashable {
    public var id: String
    public var created: Double
    public init(id: String, created: Double) {
      self.id = id
      self.created = created
    }
  }

  public var issueID: String
  public var limit: Int
  @Nullable public var start: Cursor?
  public var dir: PageDirection
  public var inclusive: Bool

  public init(issueID: String, limit: Int, start: Cursor?, dir: PageDirection, inclusive: Bool) {
    self.issueID = issueID
    self.limit = limit
    self.start = start
    self.dir = dir
    self.inclusive = inclusive
  }
}

public struct IssueDetailArgs: Codable, Sendable, Hashable {
  public var idField: IDField
  public var id: IssueKey
  public init(idField: IDField, id: IssueKey) {
    self.idField = idField
    self.id = id
  }
}

public struct IssueListV2Args: Codable, Sendable, Hashable {
  public var listContext: ListContextParams
  @Nullable public var limit: Int?
  @Nullable public var start: IssueRowSort?
  public var dir: PageDirection
  public var inclusive: Bool?

  public init(
    listContext: ListContextParams, limit: Int?, start: IssueRowSort?, dir: PageDirection,
    inclusive: Bool? = nil
  ) {
    self.listContext = listContext
    self.limit = limit
    self.start = start
    self.dir = dir
    self.inclusive = inclusive
  }
}

public struct ListIssueByIDArgs: Codable, Sendable, Hashable {
  public var listContext: ListContextParams
  public var idField: IDField
  public var idValue: IssueKey
  public init(listContext: ListContextParams, idField: IDField, idValue: IssueKey) {
    self.listContext = listContext
    self.idField = idField
    self.idValue = idValue
  }
}

public struct PrevNextArgs: Codable, Sendable, Hashable {
  public enum Direction: String, Codable, Sendable, Hashable {
    case next, prev
  }

  @Nullable public var listContext: ListContextParams?
  @Nullable public var issue: IssueRowSort?
  public var dir: Direction

  public init(listContext: ListContextParams?, issue: IssueRowSort?, dir: Direction) {
    self.listContext = listContext
    self.issue = issue
    self.dir = dir
  }
}

public struct IssueListArgs: Codable, Sendable, Hashable {
  public var listContext: ListContextParams
  public var limit: Int
  public init(listContext: ListContextParams, limit: Int) {
    self.listContext = listContext
    self.limit = limit
  }
}

public struct UserPickerArgs: Codable, Sendable, Hashable {
  public enum Filter: String, Codable, Sendable, Hashable {
    case crew, creators
  }

  public var disabled: Bool
  @Nullable public var login: String?
  @Nullable public var filter: Filter?

  public init(disabled: Bool, login: String?, filter: Filter?) {
    self.disabled = disabled
    self.login = login
    self.filter = filter
  }
}

public enum Queries {
  public static let allLabels = defineQuery("allLabels") { (_: NoArgs, _: AuthData?) in
    zql.label
  }

  public static let allUsers = defineQuery("allUsers") { (_: NoArgs, _: AuthData?) in
    zql.user
  }

  public static let allProjects = defineQuery("allProjects") { (_: NoArgs, _: AuthData?) in
    zql.project
  }

  public static let user = defineQuery("user") { (userID: String, _: AuthData?) in
    zql.user.where(\.id, userID).one()
  }

  public static let crewUser = defineQuery("crewUser") { (userID: String, _: AuthData?) in
    zql.user.where(\.id, userID).where(\.role, .crew).one()
  }

  public static let labels = defineQuery("labels") { (args: ProjectArgs, _: AuthData?) in
    labelsQuery(args.projectName, nil)
  }

  public static let labelsOrderByName = defineQuery("labelsOrderByName") {
    (args: LabelsOrderByNameArgs, _: AuthData?) in
    labelsQuery(args.projectName, args.orderBy)
  }

  public static let issuePreloadV2 = defineQuery("issuePreloadV2") {
    (args: ProjectArgs, auth: AuthData?) in
    applyIssuePermissions(
      zql.issue
        .whereExists(\.project, scalar: true) {
          $0.where(\.lowerCaseName, args.projectName.lowercased())
        }
        .related(\.labels)
        .related(\.viewState) { q in
          truthy(auth?.sub).map { q.where(\.userID, $0) } ?? alwaysFalse(q)
        }
        .related(\.creator)
        .related(\.assignee)
        .related(\.emoji) { $0.related(\.creator) }
        .related(\.comments) { comments in
          comments
            .related(\.creator)
            .related(\.emoji) { $0.related(\.creator) }
            .limit(10)
            .orderBy(\.created, .desc)
        }
        .orderBy(\.modified, .desc)
        .orderBy(\.id, .desc)
        .limit(1000),
      auth?.role)
  }

  public static let userPref = defineQuery("userPref") { (key: String, auth: AuthData?) in
    zql.userPref.where(\.key, key).where(\.userID, .eq, auth?.sub).one()
  }

  public static let usersForProject = defineQuery("usersForProject") {
    (args: UsersForProjectArgs, _: AuthData?) in
    var q = zql.user
    if args.disabled, let login = truthy(args.login) {
      q = q.where(\.login, login)
    } else if let filter = args.filter {
      let inProject = { (issues: Query<Issue, [Issue]>) in
        issues.whereExists(\.project, scalar: true) {
          $0.where(\.lowerCaseName, args.projectName.lowercased())
        }
      }
      switch filter {
      case .crew: q = q.where(crew)
      case .creators: q = q.whereExists(\.createdIssues, inProject)
      case .assignees: q = q.whereExists(\.assignedIssues, inProject)
      }
    }
    return q
  }

  /// A page of an issue's comments, ordered by (created, id); a backward
  /// page flips the order. `start` is the cursor row.
  public static let commentsPage = defineQuery("commentsPage") {
    (args: CommentsPageArgs, _: AuthData?) in
    let dir: AST.Direction = args.dir == .forward ? .asc : .desc
    var q = zql.comment
      .related(\.creator)
      .related(\.emoji) { $0.related(\.creator) }
      .where(\.issueID, args.issueID)
      .orderBy(\.created, dir)
      .orderBy(\.id, dir)
    if let start = args.start {
      q = q.start([\.id: start.id, \.created: start.created], inclusive: args.inclusive)
    }
    return q.limit(args.limit)
  }

  public static let comment = defineQuery("comment") { (id: String, _: AuthData?) in
    zql.comment
      .related(\.creator)
      .related(\.emoji) { $0.related(\.creator) }
      .where(\.id, id)
      .one()
  }

  public static let issueDetail = defineQuery("issueDetail") {
    (args: IssueDetailArgs, auth: AuthData?) in
    applyIssuePermissions(
      zql.issue
        .where(args.idField.rawValue, args.id.json)
        .related(\.project)
        .related(\.emoji) { $0.related(\.creator) }
        .related(\.creator)
        .related(\.assignee)
        .related(\.labels)
        .related(\.notificationState) { q in
          truthy(auth?.sub).map { q.where(\.userID, $0) } ?? alwaysFalse(q)
        }
        .related(\.viewState) { q in
          (truthy(auth?.sub).map { q.where(\.userID, $0) } ?? alwaysFalse(q)).one()
        }
        // The most recent comments, so the comments list renders locally on
        // navigation.
        .related(\.comments) { comments in
          comments
            .related(\.creator)
            .related(\.emoji) { $0.related(\.creator) }
            .limit(50)
            .orderBy(\.created, .desc)
            .orderBy(\.id, .desc)
        }
        .one(),
      auth?.role)
  }

  public static let issueListV2 = defineQuery("issueListV2") {
    (args: IssueListV2Args, auth: AuthData?) in
    buildListQuery(
      ListQueryArgs(
        listContext: args.listContext, userID: auth?.sub, role: auth?.role, limit: args.limit,
        start: args.start, dir: args.dir, inclusive: args.inclusive ?? false))
  }

  /// One issue, for the order to start a list at.
  public static let listIssueByID = defineQuery("listIssueByID") {
    (args: ListIssueByIDArgs, auth: AuthData?) in
    buildListQuery(ListQueryArgs(listContext: args.listContext, role: auth?.role))
      .where(args.idField.rawValue, args.idValue.json)
      .one()
  }

  public static let emojiChange = defineQuery("emojiChange") { (subjectID: String, _: AuthData?) in
    zql.emoji
      .where(\.subjectID, subjectID)
      .related(\.creator) { $0.one() }
  }

  /// Deprecated in zbugs.
  public static let prevNext = defineQuery("prevNext") { (args: PrevNextArgs, auth: AuthData?) in
    buildListQuery(
      ListQueryArgs(
        listContext: args.listContext, role: auth?.role, start: args.issue,
        dir: args.dir == .next ? .forward : .backward)
    ).one()
  }

  /// Deprecated in zbugs.
  public static let issueList = defineQuery("issueList") { (args: IssueListArgs, auth: AuthData?) in
    buildListQuery(
      ListQueryArgs(
        listContext: args.listContext, userID: auth?.sub, role: auth?.role, limit: args.limit))
  }

  /// Deprecated in zbugs.
  public static let userPicker = defineQuery("userPicker") { (args: UserPickerArgs, _: AuthData?) in
    var q = zql.user
    if args.disabled, let login = truthy(args.login) {
      q = q.where(\.login, login)
    } else if let filter = args.filter {
      switch filter {
      case .crew: q = q.where(crew)
      case .creators: q = q.whereExists(\.createdIssues)
      }
    }
    return q
  }

  /// Every query, for the server's registry and the drift test.
  public static let all: [any NamedQuery<AuthData?>] = [
    allLabels, allUsers, allProjects, user, crewUser, labels, labelsOrderByName, issuePreloadV2,
    userPref, usersForProject, commentsPage, comment, issueDetail, issueListV2, listIssueByID,
    emojiChange, prevNext, issueList, userPicker,
  ]
}

/// The crew, without the bots.
private func crew(_ user: RowExpression<User>) -> Condition {
  user.and(user.role == .crew, user.not(user.login.like("rocibot%")))
}

func applyIssuePermissions<Result: QueryResult>(
  _ q: Query<Issue, Result>, _ role: Role?
)
  -> Query<Issue, Result>
where Result.Row == Issue {
  role == .crew ? q : q.where(\.visibility, .public)
}

func labelsQuery(_ projectName: String, _ orderBy: AST.Direction?) -> Query<Label, [Label]> {
  var q = zql.label.whereExists(\.project, scalar: true) {
    $0.where(\.lowerCaseName, projectName.lowercased())
  }
  if let orderBy {
    q = q.orderBy(\.name, orderBy)
  }
  return q
}

/// Matches no row.
func alwaysFalse<R: ZeroRow, S: QueryResult>(_ q: Query<R, S>) -> Query<R, S> where S.Row == R {
  q.where { $0.or() }
}

/// The string when JS would take it as true: neither nil nor empty.
func truthy(_ s: String?) -> String? {
  s.flatMap { $0.isEmpty ? nil : $0 }
}

struct ListQueryArgs {
  var issueQuery = zql.issue
  var listContext: ListContextParams?
  var userID: String?
  var role: Role?
  var limit: Int?
  var start: IssueRowSort?
  var dir = PageDirection.forward
  var inclusive = false
}

func buildListQuery(_ args: ListQueryArgs) -> Query<Issue, [Issue]> {
  var q = args.issueQuery
    .related(\.viewState) { q in
      (truthy(args.userID).map { q.where(\.userID, $0) } ?? alwaysFalse(q)).one()
    }
    .related(\.labels)
    // The issue page cannot render until the creator is present; loading it
    // here lets a click from the list render locally.
    .related(\.creator)

  guard let listContext = args.listContext else {
    return alwaysFalse(q)
  }
  let projectName = (listContext.projectName ?? zeroProjectName).lowercased()
  q = q.whereExists(\.project, scalar: true) { $0.where(\.lowerCaseName, projectName) }

  let dir: AST.Direction =
    args.dir == .forward
    ? listContext.sortDirection : listContext.sortDirection == .asc ? .desc : .asc
  let sortField: KeyPath<Issue, Double> =
    listContext.sortField == .modified ? \.modified : \.created
  q = q.orderBy(sortField, dir).orderBy(\.id, dir)

  if let start = args.start {
    q = q.start(
      [\.id: start.id, \.created: start.created, \.modified: start.modified],
      inclusive: args.inclusive)
  }
  if let limit = args.limit, limit != 0 {
    q = q.limit(limit)
  }

  let text = truthy(listContext.textFilter).map { "%\(escapeLike($0))%" }
  q = q.where { issue in
    issue.and(
      [
        listContext.open.map { issue.open == $0 },
        text.map { text in
          issue.or(
            issue.title.ilike(text), issue.description.ilike(text),
            issue.exists(\.comments) { $0.where(\.body, .ilike, text) })
        },
      ]
        + (listContext.labels ?? []).map { label in
          // `label`'s unique key is (projectID, name); the scalar `project`
          // gate pins projectID before the `name` gate runs.
          issue.exists(\.issueLabels) {
            $0.whereExists(\.label, scalar: true) {
              $0.where(\.name, label).whereExists(\.project, scalar: true) {
                $0.where(\.lowerCaseName, projectName)
              }
            }
          }
        })
  }

  if let creator = truthy(listContext.creator) {
    q = q.whereExists(\.creator, scalar: true) { $0.where(\.login, creator) }
  }
  if let assignee = truthy(listContext.assignee) {
    q = q.whereExists(\.assignee, scalar: true) { $0.where(\.login, assignee) }
  }
  return applyIssuePermissions(q, args.role)
}

extension Query {
  /// `where(column, value)` by the column's name, as JS writes it.
  func `where`(_ column: String, _ value: JSONValue) -> Self {
    Self(any.where(column, value))
  }
}
