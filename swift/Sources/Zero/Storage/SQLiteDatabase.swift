import CSQLite

/// A SQLite failure, with SQLite's result code.
struct SQLiteError: Error, CustomStringConvertible {
  let code: Int32
  let message: String

  var description: String { "SQLite error \(code): \(message)" }

  /// The disk or the database file is full.
  var isFull: Bool { code & 0xff == SQLITE_FULL }
  /// Another connection holds a lock past the busy timeout: contention,
  /// not a storage failure.
  var isBusy: Bool {
    let primary = code & 0xff
    return primary == SQLITE_BUSY || primary == SQLITE_LOCKED
  }
  /// The database file can't be opened or isn't a database.
  var isCannotOpen: Bool {
    let primary = code & 0xff
    return primary == SQLITE_CANTOPEN || primary == SQLITE_NOTADB || primary == SQLITE_CORRUPT
      || primary == SQLITE_PERM || primary == SQLITE_READONLY
  }
}

// SQLite copies bound text and blobs before `sqlite3_bind_*` returns.
private let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

/// A connection to one SQLite database. Not thread-safe: use it from one
/// thread or serial queue at a time.
final class SQLiteDatabase {
  private var db: OpaquePointer?
  private var statements: [String: OpaquePointer] = [:]

  init(path: String) throws(SQLiteError) {
    let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX
    let rc = sqlite3_open_v2(path, &db, flags, nil)
    guard rc == SQLITE_OK else {
      let message = db.map { String(cString: sqlite3_errmsg($0)) } ?? "cannot open"
      sqlite3_close_v2(db)
      db = nil
      throw SQLiteError(code: rc, message: message)
    }
    sqlite3_extended_result_codes(db, 1)
    sqlite3_busy_timeout(db, 5000)
  }

  deinit { close() }

  func close() {
    for (_, s) in statements { sqlite3_finalize(s) }
    statements = [:]
    if let db { sqlite3_close_v2(db) }
    db = nil
  }

  private func error(_ rc: Int32) -> SQLiteError {
    SQLiteError(code: rc, message: db.map { String(cString: sqlite3_errmsg($0)) } ?? "closed")
  }

  /// Runs SQL without parameters or results, such as a schema.
  func execute(_ sql: String) throws(SQLiteError) {
    let rc = sqlite3_exec(db, sql, nil, nil, nil)
    guard rc == SQLITE_OK else { throw error(rc) }
  }

  /// A value to bind to a parameter.
  enum Value {
    case text(String)
    case real(Double)
    case integer(Int64)
    case null
  }

  /// One row of a query's result.
  struct Row {
    fileprivate let statement: OpaquePointer

    func text(_ column: Int32) -> String {
      guard let p = sqlite3_column_text(statement, column) else { return "" }
      let count = Int(sqlite3_column_bytes(statement, column))
      return String(decoding: UnsafeBufferPointer(start: p, count: count), as: UTF8.self)
    }

    /// The column's UTF-8 bytes, without decoding them.
    func bytes(_ column: Int32) -> [UInt8] {
      guard let p = sqlite3_column_text(statement, column) else { return [] }
      let count = Int(sqlite3_column_bytes(statement, column))
      return Array(UnsafeBufferPointer(start: p, count: count))
    }

    func real(_ column: Int32) -> Double { sqlite3_column_double(statement, column) }
    func integer(_ column: Int32) -> Int64 { sqlite3_column_int64(statement, column) }
  }

  private func prepared(_ sql: String) throws(SQLiteError) -> OpaquePointer {
    if let s = statements[sql] {
      sqlite3_reset(s)
      sqlite3_clear_bindings(s)
      return s
    }
    var s: OpaquePointer?
    let rc = sqlite3_prepare_v2(db, sql, -1, &s, nil)
    guard rc == SQLITE_OK, let s else { throw error(rc) }
    statements[sql] = s
    return s
  }

  private func bind(_ s: OpaquePointer, _ values: [Value]) throws(SQLiteError) {
    for (i, v) in values.enumerated() {
      let index = Int32(i + 1)
      let rc: Int32
      switch v {
      case .text(let t): rc = sqlite3_bind_text(s, index, t, -1, transient)
      case .real(let d): rc = sqlite3_bind_double(s, index, d)
      case .integer(let n): rc = sqlite3_bind_int64(s, index, n)
      case .null: rc = sqlite3_bind_null(s, index)
      }
      guard rc == SQLITE_OK else { throw error(rc) }
    }
  }

  /// Runs a statement that returns no rows.
  func run(_ sql: String, _ values: [Value] = []) throws(SQLiteError) {
    let s = try prepared(sql)
    defer { sqlite3_reset(s) }
    try bind(s, values)
    let rc = sqlite3_step(s)
    guard rc == SQLITE_DONE || rc == SQLITE_ROW else { throw error(rc) }
  }

  /// Runs a query and calls `body` for each row.
  func query(
    _ sql: String, _ values: [Value] = [], _ body: (Row) throws -> Void
  ) throws {
    let s = try prepared(sql)
    defer { sqlite3_reset(s) }
    try bind(s, values)
    while true {
      let rc = sqlite3_step(s)
      if rc == SQLITE_DONE { return }
      guard rc == SQLITE_ROW else { throw error(rc) }
      try body(Row(statement: s))
    }
  }

  /// Runs `body` in a transaction, rolled back if it throws.
  func transaction(_ body: () throws -> Void) throws {
    try execute("BEGIN IMMEDIATE")
    do {
      try body()
      try execute("COMMIT")
    } catch {
      try? execute("ROLLBACK")
      throw error
    }
  }
}
