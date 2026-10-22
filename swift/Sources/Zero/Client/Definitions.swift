/// A named query (`defineQuery` in `packages/zql/src/query/query-registry.ts`).
///
/// The name must match the TS registry entry the server runs: the client
/// builds the query locally for optimistic results, and the server builds
/// its own from the name and arguments.
public struct QueryDefinition<
  Args: Encodable & Sendable, Context: Sendable, Row: ZeroRow, Result: QueryResult
>:
  Sendable
where Result.Row == Row {
  public let name: String
  let body: @Sendable (Args, Context) -> Query<Row, Result>

  public func callAsFunction(_ args: Args) -> QueryRequest<Row, Result, Context> {
    let body = self.body
    return QueryRequest(name: name, args: [encodeArgs(args)]) { body(args, $0).any }
  }
}

/// A named query whose arguments can come as JSON: what the drift harness
/// checks against the TS definition of the same name.
public protocol NamedQuery<Context>: Sendable {
  associatedtype Context: Sendable
  var name: String { get }
  /// The query for `args` (none, or one JSON value) read as the
  /// definition's argument type, with the arguments it sends for them.
  func query(json args: [JSONValue], context: Context) throws -> (AnyQuery, args: [JSONValue])
}

extension QueryDefinition: NamedQuery where Args: Decodable {
  public func query(
    json args: [JSONValue], context: Context
  ) throws -> (
    AnyQuery, args: [JSONValue]
  ) {
    let decoded = try JSONValueDecoder.decode(Args.self, from: args.first ?? .null)
    let request = self(decoded)
    return (request.query(context), request.args)
  }
}

extension NoArgsQueryDefinition: NamedQuery {
  public func query(
    json args: [JSONValue], context: Context
  ) throws -> (
    AnyQuery, args: [JSONValue]
  ) {
    let request = self()
    return (request.query(context), request.args)
  }
}

/// A named query without arguments.
public struct NoArgsQueryDefinition<Context: Sendable, Row: ZeroRow, Result: QueryResult>: Sendable
where Result.Row == Row {
  public let name: String
  let body: @Sendable (Context) -> Query<Row, Result>

  public func callAsFunction() -> QueryRequest<Row, Result, Context> {
    let body = self.body
    return QueryRequest(name: name, args: []) { body($0).any }
  }
}

/// A named query applied to its arguments, ready to materialize, run, or
/// preload.
public struct QueryRequest<Row: ZeroRow, Result: QueryResult, Context: Sendable>: Sendable
where Result.Row == Row {
  public let name: String
  /// The arguments as sent: none, or one JSON value.
  public let args: [JSONValue]
  let build: @Sendable (Context) -> AnyQuery

  init(name: String, args: [JSONValue], build: @escaping @Sendable (Context) -> AnyQuery) {
    self.name = name
    self.args = args
    self.build = build
  }

  /// The query, named so the server knows it.
  func query(_ context: Context) -> AnyQuery {
    build(context).nameAndArgs(name, args)
  }
}

public func defineQuery<
  Args: Encodable & Sendable, Context: Sendable, Row: ZeroRow, Result: QueryResult
>(
  _ name: String, _ body: @escaping @Sendable (Args, Context) -> Query<Row, Result>
) -> QueryDefinition<Args, Context, Row, Result> {
  QueryDefinition(name: name, body: body)
}

/// A query of an app without a context.
public func defineQuery<Args: Encodable & Sendable, Row: ZeroRow, Result: QueryResult>(
  _ name: String, _ body: @escaping @Sendable (Args) -> Query<Row, Result>
) -> QueryDefinition<Args, Void, Row, Result> {
  QueryDefinition(name: name) { args, _ in body(args) }
}

/// A query without arguments that reads the context:
/// `defineQuery("issues.mine") { (_: NoArgs, ctx: Auth) in … }`.
public func defineQuery<Context: Sendable, Row: ZeroRow, Result: QueryResult>(
  _ name: String, _ body: @escaping @Sendable (NoArgs, Context) -> Query<Row, Result>
) -> NoArgsQueryDefinition<Context, Row, Result> {
  NoArgsQueryDefinition(name: name) { body(NoArgs(), $0) }
}

/// A query without arguments of an app without a context.
public func defineQuery<Row: ZeroRow, Result: QueryResult>(
  _ name: String, _ body: @escaping @Sendable () -> Query<Row, Result>
) -> NoArgsQueryDefinition<Void, Row, Result> {
  NoArgsQueryDefinition(name: name) { _ in body() }
}

/// Stands for the arguments of a query or mutator that takes none, where
/// the definition also takes the context.
public struct NoArgs: Sendable, Hashable {
  public init() {}
}

func encodeArgs<Args: Encodable>(_ args: Args) -> JSONValue {
  do {
    return try JSONValueEncoder.encode(args)
  } catch {
    preconditionFailure("Arguments must encode to JSON: \(error)")
  }
}

/// A mutator the client can run and replay by name.
public protocol Mutator<Context>: Sendable {
  associatedtype Context: Sendable
  var name: String { get }
  /// Runs the mutator in `tx`. `args` is nil when the caller passed none.
  /// Returns what the caller's `client` result carries.
  @MainActor func run(_ tx: Transaction, args: JSONValue?, context: Context) throws -> JSONValue?
  /// `args` read as the mutator's argument type and encoded again: what
  /// the client would send for them.
  func reencode(args: JSONValue?) throws -> JSONValue?
}

extension Mutator {
  public func reencode(args: JSONValue?) throws -> JSONValue? { args }
}

/// A mutator defined in Swift (`defineMutator` in
/// `packages/zql/src/mutate/mutator.ts`). The name must match the TS
/// registry entry the API server runs.
///
/// The body runs synchronously on the main actor: once optimistically, and
/// again on every rebase over new server data while the mutation is
/// pending, so it must only depend on `tx`, `args`, and `context`.
public struct MutatorDefinition<Args: Codable & Sendable, Context: Sendable>: Mutator {
  public let name: String
  let body: @MainActor @Sendable (Transaction, Args, Context) throws -> JSONValue?

  public func callAsFunction(_ args: Args) -> MutateRequest<Context> {
    MutateRequest(mutator: self, args: encodeArgs(args))
  }

  @MainActor
  public func run(_ tx: Transaction, args: JSONValue?, context: Context) throws -> JSONValue? {
    let decoded = try JSONValueDecoder.decode(Args.self, from: args ?? .null)
    return try body(tx, decoded, context)
  }

  public func reencode(args: JSONValue?) throws -> JSONValue? {
    encodeArgs(try JSONValueDecoder.decode(Args.self, from: args ?? .null))
  }
}

/// A mutator without arguments.
public struct NoArgsMutatorDefinition<Context: Sendable>: Mutator {
  public let name: String
  let body: @MainActor @Sendable (Transaction, Context) throws -> JSONValue?

  public func callAsFunction() -> MutateRequest<Context> {
    MutateRequest(mutator: self, args: nil)
  }

  @MainActor
  public func run(_ tx: Transaction, args: JSONValue?, context: Context) throws -> JSONValue? {
    try body(tx, context)
  }
}

/// A mutator applied to its arguments, ready for `zero.mutate`.
public struct MutateRequest<Context: Sendable>: Sendable {
  public let mutator: any Mutator<Context>
  /// nil when the mutator takes no arguments (JS `undefined`).
  public let args: JSONValue?

  /// A request with arguments already in their JSON form, for example to
  /// replay a mutation by name. Prefer calling the mutator definition.
  public init(mutator: any Mutator<Context>, args: JSONValue?) {
    self.mutator = mutator
    self.args = args
  }

  public var name: String { mutator.name }
}

public func defineMutator<Args: Codable & Sendable, Context: Sendable>(
  _ name: String, _ body: @escaping @MainActor @Sendable (Transaction, Args, Context) throws -> Void
) -> MutatorDefinition<Args, Context> {
  MutatorDefinition(name: name) { tx, args, context in
    try body(tx, args, context)
    return nil
  }
}

/// A mutator that returns a value to the caller of its optimistic run.
public func defineMutator<Args: Codable & Sendable, Context: Sendable, R: Encodable>(
  _ name: String, _ body: @escaping @MainActor @Sendable (Transaction, Args, Context) throws -> R
) -> MutatorDefinition<Args, Context> {
  MutatorDefinition(name: name) { tx, args, context in
    try JSONValueEncoder.encode(body(tx, args, context))
  }
}

/// A mutator of an app without a context.
public func defineMutator<Args: Codable & Sendable>(
  _ name: String, _ body: @escaping @MainActor @Sendable (Transaction, Args) throws -> Void
) -> MutatorDefinition<Args, Void> {
  MutatorDefinition(name: name) { tx, args, _ in
    try body(tx, args)
    return nil
  }
}

/// A mutator of an app without a context that returns a value to the
/// caller of its optimistic run.
public func defineMutator<Args: Codable & Sendable, R: Encodable>(
  _ name: String, _ body: @escaping @MainActor @Sendable (Transaction, Args) throws -> R
) -> MutatorDefinition<Args, Void> {
  MutatorDefinition(name: name) { tx, args, _ in
    try JSONValueEncoder.encode(body(tx, args))
  }
}

/// A mutator without arguments that reads the context:
/// `defineMutator("issue.closeAll") { (tx, _: NoArgs, ctx: Auth) in … }`.
public func defineMutator<Context: Sendable>(
  _ name: String,
  _ body: @escaping @MainActor @Sendable (Transaction, NoArgs, Context) throws -> Void
) -> NoArgsMutatorDefinition<Context> {
  NoArgsMutatorDefinition(name: name) { tx, context in
    try body(tx, NoArgs(), context)
    return nil
  }
}

/// A mutator without arguments of an app without a context.
public func defineMutator(
  _ name: String, _ body: @escaping @MainActor @Sendable (Transaction) throws -> Void
) -> NoArgsMutatorDefinition<Void> {
  NoArgsMutatorDefinition(name: name) { tx, _ in
    try body(tx)
    return nil
  }
}

/// How a mutation ended, on the client or on the server
/// (`MutatorResultDetails` in `packages/zero-client/src/client/custom.ts`).
public enum MutationOutcome: Sendable, Hashable {
  /// `data` is what the mutator returned (client) or the server's result.
  case success(data: JSONValue?)
  /// The mutator or the API server threw an `ApplicationError`.
  case appError(message: String, details: JSONValue?)
  /// Zero couldn't run the mutation or learn its result, such as while
  /// offline, when the connection closed, or when the mutator isn't
  /// registered with the `Zero` (then nothing ran or was sent). Otherwise,
  /// on the server side this doesn't mean the mutation wasn't applied: it
  /// may have been pushed before the connection closed, and the client
  /// sends pending mutations again when it reconnects. Don't retry it as if
  /// it had failed.
  case zeroError(message: String)

  /// The JS result object.
  package var json: JSONValue {
    switch self {
    case .success(let data):
      var o: JSONObject = ["type": "success"]
      if let data { o["data"] = data }
      return .object(o)
    case .appError(let message, let details):
      var e: JSONObject = ["type": "app", "message": .string(message)]
      if let details { e["details"] = details }
      return ["type": "error", "error": .object(e)]
    case .zeroError(let message):
      return ["type": "error", "error": ["type": "zero", "message": .string(message)]]
    }
  }
}

/// What `zero.mutate` returns: the outcome of the optimistic run on the
/// client, and the server's.
@MainActor
public struct MutatorResult {
  public let client: Deferred<MutationOutcome>
  public let server: Deferred<MutationOutcome>
}
