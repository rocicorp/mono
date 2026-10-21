import Foundation
import Zero

/// Who the client acts for: the `sub` and `role` of the zbugs JWT
/// (`AuthData` in apps/zbugs/shared/auth.ts). The context of every query
/// and mutator; nil when logged out.
public struct AuthData: Codable, Sendable, Hashable {
  public var sub: String
  public var role: UserRole

  public init(sub: String, role: UserRole) {
    self.sub = sub
    self.role = role
  }
}

public typealias Role = UserRole

/// apps/zbugs/shared/error.ts.
public enum MutationErrorCode: String, Sendable {
  case notLoggedIn = "NOT_LOGGED_IN"
  case notAuthorized = "NOT_AUTHORIZED"
  case notificationFailed = "NOTIFICATION_FAILED"
  case entityNotFound = "ENTITY_NOT_FOUND"
}

/// An `ApplicationError` with zbugs' details: `{code, id}`.
func mutationError(_ message: String, _ code: MutationErrorCode, _ id: String? = nil)
  -> ApplicationError
{
  var details: [String: JSONValue] = ["code": .string(code.rawValue)]
  if let id { details["id"] = .string(id) }
  return ApplicationError(message, details: .object(details))
}

/// `must` in packages/shared: the value, or the error JS throws for a
/// missing one.
func must<T>(_ value: T?) throws -> T {
  guard let value else { throw ApplicationError("Unexpected undefined value") }
  return value
}

func assertIsLoggedIn(_ auth: AuthData?) throws -> AuthData {
  guard let auth else {
    throw mutationError("User must be logged in for this operation", .notLoggedIn)
  }
  return auth
}

func isAdmin(_ auth: AuthData?) throws -> Bool {
  try assertIsLoggedIn(auth).role == .crew
}

/// A row with a creator: an issue, a comment, or an emoji.
protocol Created: ZeroRow {
  var creatorID: String { get }
  var id: String { get }
}

extension Issue: Created {}
extension Comment: Created {}
extension Emoji: Created {}

@MainActor
func assertIsCreatorOrAdmin<R: Created>(
  _ tx: Transaction, _ auth: AuthData?, _ query: Query<R, [R]>, _ id: String
) throws {
  let auth = try assertIsLoggedIn(auth)
  if try isAdmin(auth) { return }
  // By name: a key path through `Created` is not the row's own `\.id`.
  let entity = try tx.run(Query<R, [R]>(query.any.where("id", .string(id))).one())
  // Security: the same message whether or not the entity exists.
  guard let entity, auth.sub == entity.creatorID else {
    throw mutationError("Not authorized to access this resource", .notAuthorized, id)
  }
}

@MainActor
func assertUserCanSeeIssue(_ tx: Transaction, _ userID: String, _ issueID: String) throws {
  let issue = try must(tx.run(zql.issue.where(\.id, issueID).one()))
  let user = try must(tx.run(zql.user.where(\.id, userID).one()))
  if issue.visibility != .public && userID != issue.creatorID && user.role != .crew {
    throw mutationError(
      "User does not have permission to view this issue", .notAuthorized, issueID)
  }
}

@MainActor
func assertUserCanSeeComment(_ tx: Transaction, _ userID: String, _ commentID: String) throws {
  let comment = try must(tx.run(zql.comment.where(\.id, commentID).one()))
  try assertUserCanSeeIssue(tx, userID, comment.issueID)
}

extension AuthData {
  /// The claims of a zbugs JWT, without checking its signature (the API
  /// server checks it): nil when `jwt` is not one, or has expired by `now`
  /// (seconds since 1970).
  public init?(jwt: String, now: Double) {
    let parts = jwt.split(separator: ".", omittingEmptySubsequences: false)
    guard parts.count == 3, let payload = base64URLDecode(parts[1]),
      let claims = try? JSONDecoder().decode(JSONValue.self, from: Data(payload)),
      let sub = claims["sub"]?.stringValue,
      let role = claims["role"]?.stringValue.flatMap(UserRole.init(rawValue:))
    else { return nil }
    if let exp = claims["exp"]?.numberValue, exp < now { return nil }
    self.init(sub: sub, role: role)
  }
}

/// RFC 4648 §5 base64url, padded or not.
private func base64URLDecode(_ s: Substring) -> [UInt8]? {
  var bits: UInt32 = 0
  var count = 0
  var out: [UInt8] = []
  for c in s.utf8 {
    let v: UInt32
    switch c {
    case UInt8(ascii: "A")...UInt8(ascii: "Z"): v = UInt32(c - UInt8(ascii: "A"))
    case UInt8(ascii: "a")...UInt8(ascii: "z"): v = UInt32(c - UInt8(ascii: "a")) + 26
    case UInt8(ascii: "0")...UInt8(ascii: "9"): v = UInt32(c - UInt8(ascii: "0")) + 52
    case UInt8(ascii: "-"), UInt8(ascii: "+"): v = 62
    case UInt8(ascii: "_"), UInt8(ascii: "/"): v = 63
    case UInt8(ascii: "="): continue
    default: return nil
    }
    bits = bits << 6 | v
    count += 6
    if count >= 8 {
      count -= 8
      out.append(UInt8(truncatingIfNeeded: bits >> UInt32(count)))
    }
  }
  return out
}

/// A new row ID, as zbugs makes them (`nanoid`): 21 URL-safe characters.
public func makeID() -> String {
  let alphabet = Array("useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict")
  var rng = SystemRandomNumberGenerator()
  return String((0..<21).map { _ in alphabet[Int(rng.next() % 64)] })
}
