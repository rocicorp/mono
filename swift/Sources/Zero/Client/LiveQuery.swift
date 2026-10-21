import Observation

/// A query's rows as an observable object, typed by the query's result
/// (`useQuery` in `zero-react`). SwiftUI views, and UIKit and AppKit updates,
/// that read `data`, `resultType`, or `error` follow their changes.
///
/// The query stays synced while the object lives. When `Zero` replaces its
/// client, the object moves to the new one and keeps its identity.
@Observable
@MainActor
public final class LiveQuery<Result: QueryResult> {
  /// The rows, or the row of a `one()` query.
  public private(set) var data: Result
  public private(set) var resultType: ResultType
  /// The server could not run the query (a `QueryError`), or the rows could
  /// not be read as `Result`. `data` keeps the last rows read.
  public private(set) var error: (any Error)?
  /// The view the object reads.
  @ObservationIgnored public let view: MaterializedView
  @ObservationIgnored private var removeListener: () -> Void = {}
  /// The entries last read and the rows read from them: an update reads
  /// only the rows whose entries changed.
  @ObservationIgnored private var entries: [ViewEntry] = []
  @ObservationIgnored private var rows: [Result.Row] = []

  init(_ view: MaterializedView) throws {
    self.view = view
    var entries: [ViewEntry] = []
    var rows: [Result.Row] = []
    data = try Self.read(view.data, &entries, &rows)
    self.entries = entries
    self.rows = rows
    resultType = view.resultType
    error = view.error.map { QueryError($0) }
    removeListener = view.addListener { [weak self] data, type, error in
      self?.update(data, type, error)
    }
  }

  isolated deinit {
    removeListener()
  }

  private func update(_ value: ViewValue, _ type: ResultType, _ queryError: ErroredQuery?) {
    resultType = type
    do {
      data = try Self.read(value, &entries, &rows)
      error = queryError.map { QueryError($0) }
    } catch {
      self.error = error
    }
  }

  /// The result of `value`, reusing the rows read from equal `entries`.
  /// Most updates change a few rows in place, or insert or remove one, so
  /// the entries are matched in order, skipping at most one.
  private static func read(
    _ value: ViewValue, _ entries: inout [ViewEntry], _ rows: inout [Result.Row]
  ) throws(ZeroValueError) -> Result {
    let new = value.entries
    var out: [Result.Row] = []
    out.reserveCapacity(new.count)
    var j = 0
    for entry in new {
      if j < entries.count, entries[j] == entry {
        out.append(rows[j])
        j += 1
      } else if j + 1 < entries.count, entries[j + 1] == entry {
        out.append(rows[j + 1])
        j += 2
      } else {
        out.append(try Result.Row(entry: entry))
      }
    }
    entries = new
    rows = out
    return Result(rows: out)
  }

  /// Raises the query's TTL; a lower one is ignored.
  public func updateTTL(_ ttl: TTL) { view.updateTTL(ttl) }
}

extension Zero {
  /// The query's rows as an observable object that stays current.
  public func liveQuery<R: ZeroRow, S: QueryResult>(
    _ request: QueryRequest<R, S, Context>, ttl: TTL = .defaultTTL
  ) throws -> LiveQuery<S> {
    try LiveQuery(materialize(request, ttl: ttl))
  }

  /// The rows of an unnamed query, which only sees local data, as an
  /// observable object.
  public func liveQuery<R: ZeroRow, S: QueryResult>(
    _ query: Query<R, S>, ttl: TTL = .defaultTTL
  ) throws -> LiveQuery<S> {
    try LiveQuery(materialize(query, ttl: ttl))
  }
}
