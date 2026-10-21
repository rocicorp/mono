import Zero

// The mutators of apps/zbugs/shared/mutators.ts. The drift test checks
// that each changes the same rows as its TS definition, and fails the same
// way.

public struct CreateIssueArgs: Codable, Sendable, Hashable {
  public var id: String
  public var title: String
  public var description: String?
  public var created: Double
  public var modified: Double
  public var projectID: String

  public init(
    id: String, title: String, description: String? = nil, created: Double, modified: Double,
    projectID: String
  ) {
    self.id = id
    self.title = title
    self.description = description
    self.created = created
    self.modified = modified
    self.projectID = projectID
  }
}

/// The fields of an issue to change; nil leaves a field as it is.
public struct UpdateIssueArgs: Codable, Sendable, Hashable {
  public var id: String
  public var title: String?
  public var description: String?
  public var `open`: Bool?
  /// `.some(nil)` unassigns the issue.
  public var assigneeID: String??
  public var visibility: IssueVisibility?
  public var modified: Double

  public init(
    id: String, title: String? = nil, description: String? = nil, open: Bool? = nil,
    assigneeID: String?? = nil, visibility: IssueVisibility? = nil, modified: Double
  ) {
    self.id = id
    self.title = title
    self.description = description
    self.open = open
    self.assigneeID = assigneeID
    self.visibility = visibility
    self.modified = modified
  }

  enum CodingKeys: String, CodingKey {
    case id, title, description, open, assigneeID, visibility, modified
  }

  // By hand, since a left-out `assigneeID` and a null one differ.
  public init(from decoder: any Decoder) throws {
    let c = try decoder.container(keyedBy: CodingKeys.self)
    id = try c.decode(String.self, forKey: .id)
    title = try c.decodeIfPresent(String.self, forKey: .title)
    description = try c.decodeIfPresent(String.self, forKey: .description)
    open = try c.decodeIfPresent(Bool.self, forKey: .open)
    if c.contains(.assigneeID) {
      assigneeID = .some(
        try c.decodeNil(forKey: .assigneeID) ? nil : c.decode(String.self, forKey: .assigneeID))
    }
    visibility = try c.decodeIfPresent(IssueVisibility.self, forKey: .visibility)
    modified = try c.decode(Double.self, forKey: .modified)
  }

  public func encode(to encoder: any Encoder) throws {
    var c = encoder.container(keyedBy: CodingKeys.self)
    try c.encode(id, forKey: .id)
    try c.encodeIfPresent(title, forKey: .title)
    try c.encodeIfPresent(description, forKey: .description)
    try c.encodeIfPresent(open, forKey: .open)
    if let assigneeID { try c.encode(assigneeID, forKey: .assigneeID) }
    try c.encodeIfPresent(visibility, forKey: .visibility)
    try c.encode(modified, forKey: .modified)
  }
}

public struct IssueLabelArgs: Codable, Sendable, Hashable {
  public var issueID: String
  public var labelID: String
  /// Ignored: the issue's project is the label's. Still accepted for the
  /// clients that send it.
  public var projectID: String?

  public init(issueID: String, labelID: String) {
    self.issueID = issueID
    self.labelID = labelID
  }
}

public struct RemoveLabelArgs: Codable, Sendable, Hashable {
  public var issueID: String
  public var labelID: String
  public init(issueID: String, labelID: String) {
    self.issueID = issueID
    self.labelID = labelID
  }
}

public enum NotificationType: String, Codable, Sendable, Hashable {
  case subscribe, unsubscribe
}

public struct NotificationUpdateArgs: Codable, Sendable, Hashable {
  public var issueID: String
  public var subscribed: NotificationType
  public var created: Double
  public init(issueID: String, subscribed: NotificationType, created: Double) {
    self.issueID = issueID
    self.subscribed = subscribed
    self.created = created
  }
}

public struct AddEmojiArgs: Codable, Sendable, Hashable {
  public var id: String
  public var unicode: String
  public var annotation: String
  public var subjectID: String
  public var created: Double
  public init(id: String, unicode: String, annotation: String, subjectID: String, created: Double) {
    self.id = id
    self.unicode = unicode
    self.annotation = annotation
    self.subjectID = subjectID
    self.created = created
  }
}

public struct AddCommentArgs: Codable, Sendable, Hashable {
  public var id: String
  public var issueID: String
  public var body: String
  public var created: Double
  public init(id: String, issueID: String, body: String, created: Double) {
    self.id = id
    self.issueID = issueID
    self.body = body
    self.created = created
  }
}

public struct EditCommentArgs: Codable, Sendable, Hashable {
  public var id: String
  public var body: String
  public init(id: String, body: String) {
    self.id = id
    self.body = body
  }
}

public struct CreateLabelArgs: Codable, Sendable, Hashable {
  public var id: String
  public var name: String
  public var projectID: String
  public init(id: String, name: String, projectID: String) {
    self.id = id
    self.name = name
    self.projectID = projectID
  }
}

public struct CreateAndAddLabelArgs: Codable, Sendable, Hashable {
  public var issueID: String
  public var labelID: String
  public var labelName: String
  /// Ignored, as in `IssueLabelArgs`.
  public var projectID: String?
  public init(issueID: String, labelID: String, labelName: String) {
    self.issueID = issueID
    self.labelID = labelID
    self.labelName = labelName
  }
}

public struct ViewStateArgs: Codable, Sendable, Hashable {
  public var issueID: String
  public var viewed: Double
  public init(issueID: String, viewed: Double) {
    self.issueID = issueID
    self.viewed = viewed
  }
}

public struct UserPrefArgs: Codable, Sendable, Hashable {
  public var key: String
  public var value: String
  public init(key: String, value: String) {
    self.key = key
    self.value = value
  }
}

public enum Mutators {
  public static let createIssue = defineMutator("issue.create") {
    (tx, args: CreateIssueArgs, auth: AuthData?) in
    let auth = try assertIsLoggedIn(auth)
    try tx.mutate.insert(
      Issue(
        id: args.id, title: args.title, open: true, modified: args.modified,
        created: args.created, projectID: args.projectID, creatorID: auth.sub,
        description: args.description ?? "", visibility: .public))
    // The creator follows the issue.
    try updateIssueNotification(
      tx, userID: auth.sub, issueID: args.id, subscribed: .subscribe, created: args.created)
  }

  public static let updateIssue = defineMutator("issue.update") {
    (tx, change: UpdateIssueArgs, auth: AuthData?) in
    // Authorization comes first, so the error does not tell whether a
    // private issue exists.
    try assertIsCreatorOrAdmin(tx, auth, zql.issue, change.id)
    guard let oldIssue = try tx.run(zql.issue.where(\.id, change.id).one()) else {
      // An admin skips the creator check; the same error as for anyone else.
      throw mutationError("Issue not found or not authorized", .notAuthorized, change.id)
    }
    try tx.mutate.issue.update(JSONValueEncoder.encode(change).objectValue ?? [:])

    var previousAssigneeID: String?
    if case .some(let assigneeID) = change.assigneeID, assigneeID != oldIssue.assigneeID {
      previousAssigneeID = oldIssue.assigneeID
    }
    // The assignee follows the issue; the one before stops.
    if let assigneeID = truthy(change.assigneeID ?? nil) {
      try updateIssueNotification(
        tx, userID: assigneeID, issueID: change.id, subscribed: .subscribe,
        created: change.modified)
    }
    if let previous = truthy(previousAssigneeID) {
      try updateIssueNotification(
        tx, userID: previous, issueID: change.id, subscribed: .unsubscribe,
        created: change.modified)
    }
  }

  public static let deleteIssue = defineMutator("issue.delete") {
    (tx, id: String, auth: AuthData?) in
    try assertIsCreatorOrAdmin(tx, auth, zql.issue, id)
    try tx.mutate.issue.delete(["id": .string(id)])
  }

  /// Deletes the issues if the user created every one of them (or is an
  /// admin); otherwise none.
  public static let deleteIssues = defineMutator("issue.deleteMany") {
    (tx, ids: [String], auth: AuthData?) in
    _ = try assertIsLoggedIn(auth)
    for id in ids {
      try assertIsCreatorOrAdmin(tx, auth, zql.issue, id)
    }
    for id in ids {
      try tx.mutate.issue.delete(["id": .string(id)])
    }
  }

  public static let addLabel = defineMutator("issue.addLabel") {
    (tx, args: IssueLabelArgs, auth: AuthData?) in
    try assertIsCreatorOrAdmin(tx, auth, zql.issue, args.issueID)
    try tx.mutate.insert(
      IssueLabel(
        issueID: args.issueID, labelID: args.labelID,
        projectID: issueProjectID(tx, args.issueID)))
  }

  public static let removeLabel = defineMutator("issue.removeLabel") {
    (tx, args: RemoveLabelArgs, auth: AuthData?) in
    try assertIsCreatorOrAdmin(tx, auth, zql.issue, args.issueID)
    try tx.mutate.issueLabel.delete([
      "issueID": .string(args.issueID), "labelID": .string(args.labelID),
    ])
  }

  public static let updateNotification = defineMutator("notification.update") {
    (tx, args: NotificationUpdateArgs, auth: AuthData?) in
    let auth = try assertIsLoggedIn(auth)
    try updateIssueNotification(
      tx, userID: auth.sub, issueID: args.issueID, subscribed: args.subscribed,
      created: args.created, forceUpdate: true)
  }

  public static let addEmojiToIssue = defineMutator("emoji.addToIssue") {
    (tx, args: AddEmojiArgs, auth: AuthData?) in
    try addEmoji(tx, .issue, args, auth)
  }

  public static let addEmojiToComment = defineMutator("emoji.addToComment") {
    (tx, args: AddEmojiArgs, auth: AuthData?) in
    try addEmoji(tx, .comment, args, auth)
  }

  public static let removeEmoji = defineMutator("emoji.remove") {
    (tx, id: String, auth: AuthData?) in
    try assertIsCreatorOrAdmin(tx, auth, zql.emoji, id)
    try tx.mutate.emoji.delete(["id": .string(id)])
  }

  public static let addComment = defineMutator("comment.add") {
    (tx, args: AddCommentArgs, auth: AuthData?) in
    let creatorID = try assertIsLoggedIn(auth).sub
    try assertUserCanSeeIssue(tx, creatorID, args.issueID)
    try tx.mutate.insert(
      Comment(
        id: args.id, issueID: args.issueID, created: args.created, body: args.body,
        creatorID: creatorID))
    try updateIssueNotification(
      tx, userID: creatorID, issueID: args.issueID, subscribed: .subscribe,
      created: args.created)
  }

  public static let editComment = defineMutator("comment.edit") {
    (tx, args: EditCommentArgs, auth: AuthData?) in
    try assertIsCreatorOrAdmin(tx, auth, zql.comment, args.id)
    try tx.mutate.comment.update(["id": .string(args.id), "body": .string(args.body)])
  }

  public static let removeComment = defineMutator("comment.remove") {
    (tx, id: String, auth: AuthData?) in
    try assertIsCreatorOrAdmin(tx, auth, zql.comment, id)
    try tx.mutate.comment.delete(["id": .string(id)])
  }

  public static let createLabel = defineMutator("label.create") {
    (tx, args: CreateLabelArgs, auth: AuthData?) in
    guard try isAdmin(auth) else {
      throw mutationError("Only admins can create labels", .notAuthorized, args.id)
    }
    try tx.mutate.insert(Label(id: args.id, name: args.name, projectID: args.projectID))
  }

  public static let createAndAddLabel = defineMutator("label.createAndAddToIssue") {
    (tx, args: CreateAndAddLabelArgs, auth: AuthData?) in
    guard try isAdmin(auth) else {
      throw mutationError("Only admins can create labels", .notAuthorized, args.labelID)
    }
    let projectID = try issueProjectID(tx, args.issueID)
    try tx.mutate.insert(Label(id: args.labelID, name: args.labelName, projectID: projectID))
    try tx.mutate.insert(
      IssueLabel(issueID: args.issueID, labelID: args.labelID, projectID: projectID))
  }

  public static let setViewState = defineMutator("viewState.set") {
    (tx, args: ViewStateArgs, auth: AuthData?) in
    let userID = try assertIsLoggedIn(auth).sub
    try tx.mutate.upsert(ViewState(issueID: args.issueID, userID: userID, viewed: args.viewed))
  }

  public static let setUserPref = defineMutator("userPref.set") {
    (tx, args: UserPrefArgs, auth: AuthData?) in
    let userID = try assertIsLoggedIn(auth).sub
    try tx.mutate.upsert(UserPref(key: args.key, userID: userID, value: args.value))
  }

  /// Every mutator, for the client and the drift test.
  public static let all: [any Mutator<AuthData?>] = [
    createIssue, updateIssue, deleteIssue, deleteIssues, addLabel, removeLabel,
    updateNotification, addEmojiToIssue, addEmojiToComment, removeEmoji, addComment, editComment,
    removeComment, createLabel, createAndAddLabel, setViewState, setUserPref,
  ]
}

/// The project of an issue: the issue decides it, never the caller.
@MainActor
func issueProjectID(_ tx: Transaction, _ issueID: String) throws -> String {
  guard let issue = try tx.run(zql.issue.where(\.id, issueID).one()) else {
    throw mutationError("Issue not found", .entityNotFound, issueID)
  }
  return issue.projectID
}

enum EmojiSubject {
  case issue, comment
}

@MainActor
func addEmoji(_ tx: Transaction, _ subject: EmojiSubject, _ args: AddEmojiArgs, _ auth: AuthData?)
  throws
{
  let creatorID = try assertIsLoggedIn(auth).sub
  switch subject {
  case .issue: try assertUserCanSeeIssue(tx, creatorID, args.subjectID)
  case .comment: try assertUserCanSeeComment(tx, creatorID, args.subjectID)
  }
  try tx.mutate.insert(
    Emoji(
      id: args.id, value: args.unicode, annotation: args.annotation, subjectID: args.subjectID,
      creatorID: creatorID, created: args.created))
  // An emoji on the issue itself follows the issue.
  if subject == .issue {
    try updateIssueNotification(
      tx, userID: creatorID, issueID: args.subjectID, subscribed: .subscribe,
      created: args.created)
  }
}

/// Subscribes to an issue, unless the user chose before and `forceUpdate`
/// is false, or unsubscribes from it.
@MainActor
func updateIssueNotification(
  _ tx: Transaction, userID: String, issueID: String, subscribed: NotificationType,
  created: Double, forceUpdate: Bool = false
) throws {
  try assertUserCanSeeIssue(tx, userID, issueID)
  let existing = try tx.run(
    zql.issueNotifications.where(\.userID, userID).where(\.issueID, issueID).one())
  if subscribed == .subscribe && (existing == nil || forceUpdate) {
    try tx.mutate.upsert(
      IssueNotifications(userID: userID, issueID: issueID, subscribed: true, created: created))
  } else if subscribed == .unsubscribe {
    try tx.mutate.upsert(
      IssueNotifications(userID: userID, issueID: issueID, subscribed: false, created: created))
  }
}
