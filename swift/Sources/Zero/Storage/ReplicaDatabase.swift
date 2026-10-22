import Dispatch

/// Where a client keeps its replica between launches.
public enum ZeroStorage: Sendable, Hashable {
  /// A SQLite database in `directory`. The default directory is
  /// `Library/Application Support/Zero` in the app's home on Apple platforms,
  /// and `$XDG_DATA_HOME/zero` (or `~/.local/share/zero`) elsewhere.
  case sqlite(directory: String? = nil)
  /// Memory only, kept for the life of the process: a client created later
  /// with the same options finds what an earlier one stored, as with JS
  /// `kvStore: 'mem'`.
  case memory
}

/// What `Zero.delete()` and `dropAllDatabases(_:)` deleted.
public struct DroppedDatabases: Sendable, Hashable {
  /// The names of the databases deleted.
  public var deleted: [String] = []
  /// The databases that couldn't be deleted.
  public var errors: [StorageFailure] = []
}

/// Deletes every Zero database in `storage`, of every user
/// (`dropAllDatabases`). Clients still open on one keep running on what
/// they have in memory, and their later writes are dropped.
@MainActor
@discardableResult
public func dropAllDatabases(_ storage: ZeroStorage = .sqlite()) async -> DroppedDatabases {
  await ReplicaDatabase.drop(storage) { _ in true }
}

/// A failure of the local storage, rather than of its data
/// (`StorageFailureError`): the client keeps running on memory.
public struct StorageFailure: Error, Sendable, Hashable, CustomStringConvertible {
  public enum Kind: String, Sendable {
    /// The device is out of space.
    case full
    /// The database can't be opened.
    case cannotOpen = "cannot-open"
    /// A read or write failed.
    case ioError = "io-error"
  }

  public let kind: Kind
  public let message: String

  public var description: String { "\(kind.rawValue): \(message)" }

  init(_ error: any Error) {
    if let e = error as? SQLiteError {
      kind = e.isFull ? .full : e.isCannotOpen ? .cannotOpen : .ioError
      message = e.description
    } else if let e = error as? StorageFailure {
      self = e
      return
    } else {
      kind = .ioError
      message = String(describing: error)
    }
  }

  init(kind: Kind, message: String) {
    self.kind = kind
    self.message = message
  }
}

/// A client group: the clients that share a replica because they register
/// the same mutators (`ClientGroup` in
/// `packages/replicache/src/persist/client-groups.ts`).
struct ClientGroupRecord: Sendable, Hashable {
  let id: String
  let mutatorNames: [String]
  /// The server lost the group, so no new client joins it.
  var disabled: Bool
}

/// A client that used the database (`ClientV6`).
struct ClientRecord: Sendable, Hashable {
  let id: String
  let clientGroupID: String
  /// When the client last showed it was running, in ms since 1970.
  var heartbeat: Double
}

/// A deleted client the server may not know about yet
/// (`packages/replicache/src/deleted-clients.ts`).
struct ClientIDPair: Sendable, Hashable {
  let clientGroupID: String
  let clientID: String

  /// `normalizeDeletedClients`: sorted by group then client, deduplicated.
  static func normalize(_ pairs: [ClientIDPair]) -> [ClientIDPair] {
    let sorted = pairs.sorted {
      let c = StringCompare.utf16($0.clientGroupID, $1.clientGroupID)
      return c != 0 ? c < 0 : StringCompare.utf16Less($0.clientID, $1.clientID)
    }
    var out: [ClientIDPair] = []
    for p in sorted where out.last != p { out.append(p) }
    return out
  }
}

/// A mutation that ran on a client and the server has not confirmed.
struct StoredMutation: Sendable, Hashable {
  let clientID: String
  let id: Double
  let name: String
  let args: JSONValue
  /// `performance.now()` of the client that made it.
  let timestamp: Double
}

/// What a client group keeps: the last server snapshot and the mutations
/// not yet in it.
struct StoredGroup {
  var entries = KVMap()
  var cookie: JSONValue = .null
  var lastMutationIDs: [String: Double] = [:]
  var mutations: [StoredMutation] = []
}

/// A change to the database, applied in order.
enum StorageOp: Sendable {
  case putGroup(ClientGroupRecord)
  /// Deletes the group with its snapshot and mutations.
  case deleteGroup(String)
  case putClient(ClientRecord)
  case deleteClients([String])
  case setDeletedClients([ClientIDPair])
  case setProfileID(String)
  /// The snapshot after a poke: the keys it changed (nil for deleted),
  /// whether it cleared every key first, and its cookie and last mutation
  /// IDs. Drops the mutations the IDs confirm.
  case updateSnapshot(
    group: String, changes: [(String, JSONValue?)], cleared: Bool, cookie: JSONValue,
    lastMutationIDs: [String: Double])
  case addMutation(group: String, StoredMutation)
}

/// The database a client stores its replica in, shared by the clients of
/// one storage name in the process. It keeps the clients and client groups
/// in memory and writes changes through: right away to memory, and in order
/// on a background queue to SQLite.
@MainActor
final class ReplicaDatabase {
  /// The JS name of the database (`rep:<name>:<format>:<schemaVersion>`).
  let name: String
  private(set) var groups: [ClientGroupRecord] = []
  private(set) var clients: [ClientRecord] = []
  private(set) var deletedClients: [ClientIDPair] = []
  private(set) var profileID = ""

  /// The groups' data, for a memory database.
  private var memoryGroups: [String: StoredGroup]?
  private let sqlite: SQLiteStore?
  private let key: String
  private var users = 0
  private var dropped = false
  /// Set once a write failed; nothing more is written.
  private(set) var failure: StorageFailure?
  /// The clients that hear about the first write failure, by client ID.
  private var writeFailureListeners: [(clientID: String, listener: (StorageFailure) -> Void)] =
    []

  private static var open: [String: ReplicaDatabase] = [:]
  /// The profile ID of memory databases, one per process as in JS.
  private static var memoryProfileID: String?

  private init(name: String, key: String, sqlite: SQLiteStore?) {
    self.name = name
    self.key = key
    self.sqlite = sqlite
    if sqlite == nil { memoryGroups = [:] }
  }

  /// Opens the database named `name` in `storage`, or the one already open
  /// in this process. A database that can't be opened is replaced by the
  /// memory one of its name, and the failure returned.
  static func open(_ storage: ZeroStorage, name: String) -> (ReplicaDatabase, StorageFailure?) {
    switch storage {
    case .memory:
      return (memory(name: name), nil)
    case .sqlite(let directory):
      do {
        let dir = try directory ?? defaultDirectory()
        try makeDirectories(dir)
        let path = dir + "/" + fileName(name)
        if let db = open[path] {
          db.users += 1
          return (db, nil)
        }
        let store = try SQLiteStore(path: path, name: name, directory: dir)
        let db = ReplicaDatabase(name: name, key: path, sqlite: store)
        do {
          try store.sync { try db.load(from: $0) }
        } catch {
          store.close()
          throw error
        }
        db.users = 1
        open[path] = db
        return (db, nil)
      } catch {
        return (memory(name: name), StorageFailure(error))
      }
    }
  }

  /// The memory database named `name`, which lives for the process and is
  /// shared by name: `.memory` and the fallback of a database that fails
  /// (`fallbackStoreProvider`, `MemStore`).
  static func memory(name: String) -> ReplicaDatabase {
    shared(key: "memory:" + name, name: name, sqlite: nil)
  }

  private static func shared(key: String, name: String, sqlite: SQLiteStore?) -> ReplicaDatabase {
    if let db = open[key] {
      db.users += 1
      return db
    }
    let db = ReplicaDatabase(name: name, key: key, sqlite: sqlite)
    if sqlite == nil {
      if memoryProfileID == nil { memoryProfileID = "p" + randomID() }
      db.profileID = memoryProfileID!
    }
    db.users = 1
    open[key] = db
    return db
  }

  /// Gives up a user's hold. The last user of a SQLite database closes it
  /// once its writes are done; memory databases live on.
  func release() {
    users -= 1
    guard users == 0, let sqlite else { return }
    if Self.open[key] === self { Self.open[key] = nil }
    sqlite.close()
  }

  /// Waits for the writes queued so far.
  func flush() { sqlite?.flush() }

  private func load(from db: SQLiteDatabase) throws {
    try db.query("SELECT id, mutator_names, disabled FROM client_groups ORDER BY seq") { row in
      let names = try JSONParser.parse(row.bytes(1)).arrayValue?.compactMap(\.stringValue) ?? []
      groups.append(
        ClientGroupRecord(id: row.text(0), mutatorNames: names, disabled: row.integer(2) != 0))
    }
    try db.query("SELECT id, client_group_id, heartbeat FROM clients ORDER BY seq") { row in
      clients.append(
        ClientRecord(id: row.text(0), clientGroupID: row.text(1), heartbeat: row.real(2)))
    }
    try db.query("SELECT client_group_id, client_id FROM deleted_clients") { row in
      deletedClients.append(ClientIDPair(clientGroupID: row.text(0), clientID: row.text(1)))
    }
    deletedClients = ClientIDPair.normalize(deletedClients)
    profileID = try SQLiteStore.profileID(in: sqlite!.directory)
  }

  /// The snapshot and pending mutations of group `id`.
  private func loadGroup(_ id: String) throws(StorageFailure) -> StoredGroup {
    if let memoryGroups { return memoryGroups[id] ?? StoredGroup() }
    guard let sqlite else { return StoredGroup() }
    do {
      return try sqlite.sync { try SQLiteStore.loadGroup(id, from: $0) }
    } catch {
      let failure = StorageFailure(error)
      fail(failure)
      throw failure
    }
  }

  /// Applies `ops` to the in-memory state now and queues them for SQLite.
  func apply(_ ops: [StorageOp]) {
    guard !dropped else { return }
    for op in ops { applyToMirror(op) }
    if memoryGroups != nil {
      for op in ops { applyToMemory(op) }
    } else if let sqlite, failure == nil {
      sqlite.write(ops) { [weak self] error in self?.fail(StorageFailure(error)) }
    }
  }

  /// Tells client `clientID` about the first write failure, until it is
  /// removed.
  func addWriteFailureListener(
    _ clientID: String, _ listener: @escaping (StorageFailure) -> Void
  ) {
    writeFailureListeners.append((clientID, listener))
  }

  func removeWriteFailureListener(_ clientID: String) {
    writeFailureListeners.removeAll { $0.clientID == clientID }
  }

  private func fail(_ f: StorageFailure) {
    guard failure == nil else { return }
    failure = f
    for (_, listener) in writeFailureListeners { listener(f) }
  }

  private func applyToMirror(_ op: StorageOp) {
    switch op {
    case .putGroup(let g):
      if let i = groups.firstIndex(where: { $0.id == g.id }) {
        groups[i] = g
      } else {
        groups.append(g)
      }
    case .deleteGroup(let id):
      groups.removeAll { $0.id == id }
    case .putClient(let c):
      if let i = clients.firstIndex(where: { $0.id == c.id }) {
        clients[i] = c
      } else {
        clients.append(c)
      }
    case .deleteClients(let ids):
      let set = Set(ids)
      clients.removeAll { set.contains($0.id) }
    case .setDeletedClients(let pairs):
      deletedClients = pairs
    case .setProfileID(let id):
      profileID = id
    case .updateSnapshot, .addMutation:
      break
    }
  }

  private func applyToMemory(_ op: StorageOp) {
    switch op {
    case .putGroup(let g):
      if memoryGroups![g.id] == nil { memoryGroups![g.id] = StoredGroup() }
    case .deleteGroup(let id):
      memoryGroups![id] = nil
    case .updateSnapshot(let group, let changes, let cleared, let cookie, let lmids):
      var g = memoryGroups![group] ?? StoredGroup()
      if cleared { g.entries.removeAll() }
      for (k, v) in changes {
        if let v { g.entries.set(k, v) } else { g.entries.delete(k) }
      }
      g.cookie = cookie
      g.lastMutationIDs = lmids
      g.mutations.removeAll { $0.id <= lmids[$0.clientID] ?? 0 }
      memoryGroups![group] = g
    case .addMutation(let group, let m):
      memoryGroups![group, default: StoredGroup()].mutations.append(m)
    case .putClient, .deleteClients, .setDeletedClients, .setProfileID:
      break
    }
  }

  // MARK: - Clients and client groups

  /// Adds client `clientID` to the newest client group that registers the
  /// same mutators and isn't disabled, or to a new empty one
  /// (`initClientV6` with client group forking off). Returns the group and
  /// what it holds. Throws, adding nothing, when the database failed or
  /// the group can't be read.
  func initClient(
    _ clientID: String, mutatorNames: [String], now: Double
  ) throws(StorageFailure) -> (
    clientGroupID: String, group: StoredGroup
  ) {
    if let failure { throw failure }
    let names = Set(mutatorNames)
    var ops: [StorageOp] = []
    let groupID: String
    var group = StoredGroup()
    if let match = groups.first(where: {
      !$0.disabled && $0.mutatorNames.count == names.count && names.isSuperset(of: $0.mutatorNames)
    }) {
      groupID = match.id
      group = try loadGroup(groupID)
    } else {
      groupID = randomID()
      ops.append(
        .putGroup(ClientGroupRecord(id: groupID, mutatorNames: mutatorNames, disabled: false)))
    }
    ops.append(.putClient(ClientRecord(id: clientID, clientGroupID: groupID, heartbeat: now)))
    apply(ops)
    return (groupID, group)
  }

  /// Records that client `clientID` is running (`writeHeartbeat`). False
  /// when the client is gone, collected by another client.
  func heartbeat(_ clientID: String, now: Double) -> Bool {
    guard var client = clients.first(where: { $0.id == clientID }) else { return false }
    client.heartbeat = now
    apply([.putClient(client)])
    return true
  }

  /// Deletes the clients other than `clientID` that have not run for
  /// `maxInactive` ms (`gcClients`). Returns every deleted client the
  /// server has not confirmed, or nil when none was deleted now.
  func collectClients(
    keeping clientID: String, now: Double, maxInactive: Double
  )
    -> [ClientIDPair]?
  {
    let stale = clients.filter { $0.id != clientID && now - $0.heartbeat > maxInactive }
    guard !stale.isEmpty else { return nil }
    let deleted = ClientIDPair.normalize(
      deletedClients + stale.map { ClientIDPair(clientGroupID: $0.clientGroupID, clientID: $0.id) })
    apply([.deleteClients(stale.map(\.id)), .setDeletedClients(deleted)])
    return deleted
  }

  /// Deletes the client groups no client is in (`gcClientGroups` without
  /// mutation recovery, so their pending mutations go too).
  func collectClientGroups() {
    let used = Set(clients.map(\.clientGroupID))
    let ops = groups.filter { !used.contains($0.id) }.map { StorageOp.deleteGroup($0.id) }
    if !ops.isEmpty { apply(ops) }
  }

  /// The server deleted these clients and client groups
  /// (`confirmDeletedClients`).
  func confirmDeletedClients(clientIDs: [String], clientGroupIDs: [String]) {
    let ids = Set(clientIDs)
    let groupIDs = Set(clientGroupIDs)
    let gone = clients.filter { ids.contains($0.id) || groupIDs.contains($0.clientGroupID) }
    let remaining = deletedClients.filter {
      !ids.contains($0.clientID) && !groupIDs.contains($0.clientGroupID)
    }
    apply([.deleteClients(gone.map(\.id)), .setDeletedClients(remaining)])
  }

  /// Keeps new clients out of group `id` (`disableClientGroup`).
  func disableClientGroup(_ id: String) {
    guard var g = groups.first(where: { $0.id == id }) else { return }
    g.disabled = true
    apply([.putGroup(g)])
  }

  /// Deletes the database (`dropDatabase`). The next client to open it
  /// starts empty; this one's later writes are dropped.
  func drop() {
    dropped = true
    if Self.open[key] === self { Self.open[key] = nil }
    sqlite?.drop()
  }

  /// The Replicache name a database name holds (`replicacheName`):
  /// `zero-<userID>-<hash>`, from `rep:<name>:<format>:<schemaVersion>`.
  nonisolated static func replicacheName(_ name: String) -> String {
    var parts = name.dropFirst("rep:".count).split(separator: ":", omittingEmptySubsequences: false)
    parts.removeLast(min(2, parts.count))
    return parts.joined(separator: ":")
  }

  /// Deletes the databases in `storage` whose Replicache name `matches`
  /// (`dropMatchingDatabases`): those open in this process, whose clients'
  /// later writes are dropped, and the files of the others.
  static func drop(
    _ storage: ZeroStorage, matching matches: @escaping @Sendable (String) -> Bool
  ) async -> DroppedDatabases {
    var result = DroppedDatabases()
    let prefix: String
    switch storage {
    case .memory: prefix = "memory:"
    case .sqlite(let directory):
      do {
        prefix = try (directory ?? defaultDirectory()) + "/"
      } catch {
        result.errors.append(StorageFailure(error))
        return result
      }
    }
    for (key, db) in open where key.hasPrefix(prefix) && matches(replicacheName(db.name)) {
      db.drop()
      result.deleted.append(db.name)
    }
    guard case .sqlite = storage else { return result }
    let directory = String(prefix.dropLast())
    let files = await Task.detached {
      SQLiteStore.dropDatabases(in: directory) { matches(replicacheName($0)) }
    }.value
    result.deleted += files.deleted
    result.errors += files.errors
    return result
  }

  /// Deletes, in the background, the other databases in this database's
  /// directory whose clients all last ran more than `maxAge` ago. A
  /// database open in this process is kept.
  func collectDatabases(now: Double, maxAge: Double) {
    guard let sqlite, failure == nil, !dropped else { return }
    let keep = Set(Self.open.keys)
    sqlite.collectDatabases(keeping: keep, now: now, maxAge: maxAge)
  }

  // MARK: - Names and places

  /// The database name of a client (`zero.ts`, `makeIDBName`).
  nonisolated static func name(
    userID: String?, storageKey: String, mutateURL: String?, queryURL: String?,
    schemaHash: String
  ) -> String {
    // `JSON.stringify` keeps the keys in this order.
    let nameKey =
      "{\"storageKey\":\(JSONSerializer.stringify(.string(storageKey)))"
      + ",\"mutateUrl\":\(JSONSerializer.stringify(.string(mutateURL ?? "")))"
      + ",\"queryUrl\":\(JSONSerializer.stringify(.string(queryURL ?? "")))}"
    let hashedKey = String(XXHash32.h64(nameKey), radix: 36)
    return "rep:zero-\(userID ?? "__anonymous__")-\(hashedKey):\(formatVersion):"
      + "\(storageVersion).\(schemaHash)"
  }

  /// Replicache's `FormatVersion.Latest`.
  nonisolated static let formatVersion = 7
  /// `STORAGE_VERSION` in `packages/zero-client/src/client/storage-version.ts`.
  nonisolated static let storageVersion = 52
  /// The version of the SQLite layout. A new one starts a new file, as a new
  /// JS storage version starts a new database.
  nonisolated static let sqliteLayoutVersion = 1

  /// The file a database is stored in: names can hold any character.
  nonisolated static func fileName(_ name: String) -> String {
    "zero-v\(sqliteLayoutVersion)-" + String(XXHash32.h64(name), radix: 36) + ".sqlite"
  }
}

/// A SQLite database file and the serial queue that owns its connection.
final class SQLiteStore: @unchecked Sendable {
  private let queue = DispatchQueue(label: "zero.storage")
  // Only used on `queue`.
  private var db: SQLiteDatabase?
  /// Set once a write failed: the writes queued after it are dropped.
  private var failed = false
  /// The writes that found the database busy, written with the next.
  private var retrying: [StorageOp] = []
  let path: String
  let directory: String

  init(path: String, name: String, directory: String) throws {
    self.path = path
    self.directory = directory
    let db = try SQLiteDatabase(path: path)
    try db.execute(
      """
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS client_groups (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL UNIQUE,
        mutator_names TEXT NOT NULL,
        disabled INTEGER NOT NULL,
        cookie TEXT NOT NULL DEFAULT 'null',
        last_mutation_ids TEXT NOT NULL DEFAULT '{}'
      );
      CREATE TABLE IF NOT EXISTS clients (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL UNIQUE,
        client_group_id TEXT NOT NULL,
        heartbeat REAL NOT NULL
      );
      CREATE TABLE IF NOT EXISTS deleted_clients (
        client_group_id TEXT NOT NULL,
        client_id TEXT NOT NULL,
        PRIMARY KEY (client_group_id, client_id)
      ) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS entries (
        client_group_id TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        PRIMARY KEY (client_group_id, key)
      ) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS mutations (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        client_group_id TEXT NOT NULL,
        client_id TEXT NOT NULL,
        id REAL NOT NULL,
        name TEXT NOT NULL,
        args TEXT NOT NULL,
        timestamp REAL NOT NULL
      );
      """)
    try db.run(
      "INSERT INTO meta (key, value) VALUES ('name', ?) ON CONFLICT (key) DO NOTHING",
      [.text(name)])
    self.db = db
  }

  /// Runs `body` on the queue and waits for it.
  func sync<T>(_ body: (SQLiteDatabase) throws -> T) throws -> T {
    try queue.sync {
      guard let db else { throw StorageFailure(kind: .ioError, message: "The database is closed") }
      return try body(db)
    }
  }

  func flush() { queue.sync {} }

  /// Applies `ops` in one transaction, after the writes queued before. A
  /// database busy past the busy timeout is not a failure
  /// (`classifySQLiteError`): the ops are kept and written with the next
  /// write, as the next persist writes what a failed one had. After any
  /// other error nothing more is written.
  func write(_ ops: [StorageOp], onError: @escaping @MainActor @Sendable (any Error) -> Void) {
    queue.async { [self] in
      guard let db, !failed else { return }
      let ops = retrying + ops
      do {
        try db.transaction {
          for op in ops { try Self.apply(op, to: db) }
        }
        retrying = []
      } catch let error as SQLiteError where error.isBusy {
        retrying = ops
      } catch {
        failed = true
        let e = error
        Task { @MainActor in onError(e) }
      }
    }
  }

  func close() {
    queue.sync {
      db?.close()
      db = nil
    }
  }

  /// Deletes the file, after the writes queued before.
  func drop() {
    queue.sync {
      db?.close()
      db = nil
      for suffix in ["", "-wal", "-shm"] { _ = unlink(path + suffix) }
    }
  }

  /// The paths of the database files in `directory`.
  static func databaseFiles(in directory: String) -> [String] {
    guard let dir = opendir(directory) else { return [] }
    var files: [String] = []
    while let entry = readdir(dir) {
      let name = withUnsafeBytes(of: entry.pointee.d_name) { raw in
        String(decoding: raw.prefix { $0 != 0 }, as: UTF8.self)
      }
      if name.hasPrefix("zero-"), name.hasSuffix(".sqlite") { files.append(directory + "/" + name) }
    }
    closedir(dir)
    return files
  }

  private static func unlinkDatabase(_ path: String) -> Bool {
    var deleted = true
    for suffix in ["", "-wal", "-shm"] where unlink(path + suffix) != 0 && errno != ENOENT {
      deleted = false
    }
    return deleted
  }

  /// Deletes the database files in `directory` whose name `matches`, for
  /// `ReplicaDatabase.drop`. A file that isn't one of ours is left.
  static func dropDatabases(
    in directory: String, matching matches: (String) -> Bool
  ) -> DroppedDatabases {
    var result = DroppedDatabases()
    for path in databaseFiles(in: directory) {
      var name: String?
      do {
        let db = try SQLiteDatabase(path: path)
        defer { db.close() }
        try db.query("SELECT value FROM meta WHERE key = 'name'") { name = $0.text(0) }
      } catch {
        continue
      }
      guard let name, matches(name) else { continue }
      if unlinkDatabase(path) {
        result.deleted.append(name)
      } else {
        result.errors.append(
          StorageFailure(kind: .ioError, message: "Cannot delete \(path): errno \(errno)"))
      }
    }
    return result
  }

  /// See `ReplicaDatabase.collectDatabases`.
  func collectDatabases(keeping: Set<String>, now: Double, maxAge: Double) {
    let directory = self.directory
    queue.async {
      for path in Self.databaseFiles(in: directory) {
        guard !keeping.contains(path) else { continue }
        var recent = false
        do {
          let db = try SQLiteDatabase(path: path)
          defer { db.close() }
          try db.query("SELECT max(heartbeat) FROM clients") { row in
            recent = now - row.real(0) < maxAge
          }
        } catch {
          // Not a database of ours, or busy: leave it.
          continue
        }
        if !recent { _ = Self.unlinkDatabase(path) }
      }
    }
  }

  private static func json(_ v: JSONValue) -> SQLiteDatabase.Value {
    .text(JSONSerializer.stringify(v))
  }

  private static func apply(_ op: StorageOp, to db: SQLiteDatabase) throws {
    switch op {
    case .putGroup(let g):
      try db.run(
        """
        INSERT INTO client_groups (id, mutator_names, disabled) VALUES (?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET disabled = excluded.disabled
        """,
        [
          .text(g.id), json(.array(g.mutatorNames.map { .string($0) })),
          .integer(g.disabled ? 1 : 0),
        ])
    case .deleteGroup(let id):
      try db.run("DELETE FROM client_groups WHERE id = ?", [.text(id)])
      try db.run("DELETE FROM entries WHERE client_group_id = ?", [.text(id)])
      try db.run("DELETE FROM mutations WHERE client_group_id = ?", [.text(id)])
    case .putClient(let c):
      try db.run(
        """
        INSERT INTO clients (id, client_group_id, heartbeat) VALUES (?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET heartbeat = excluded.heartbeat
        """,
        [.text(c.id), .text(c.clientGroupID), .real(c.heartbeat)])
    case .deleteClients(let ids):
      for id in ids { try db.run("DELETE FROM clients WHERE id = ?", [.text(id)]) }
    case .setDeletedClients(let pairs):
      try db.run("DELETE FROM deleted_clients")
      for p in pairs {
        try db.run(
          "INSERT OR IGNORE INTO deleted_clients (client_group_id, client_id) VALUES (?, ?)",
          [.text(p.clientGroupID), .text(p.clientID)])
      }
    case .setProfileID:
      break
    case .updateSnapshot(let group, let changes, let cleared, let cookie, let lmids):
      if cleared {
        try db.run("DELETE FROM entries WHERE client_group_id = ?", [.text(group)])
      }
      for (k, v) in changes {
        if let v {
          try db.run(
            """
            INSERT INTO entries (client_group_id, key, value) VALUES (?, ?, ?)
            ON CONFLICT (client_group_id, key) DO UPDATE SET value = excluded.value
            """,
            [.text(group), .text(k), json(v)])
        } else {
          try db.run(
            "DELETE FROM entries WHERE client_group_id = ? AND key = ?", [.text(group), .text(k)])
        }
      }
      try db.run(
        "UPDATE client_groups SET cookie = ?, last_mutation_ids = ? WHERE id = ?",
        [json(cookie), json(.object(lmids.mapValues { .number($0) })), .text(group)])
      for (clientID, lmid) in lmids {
        try db.run(
          "DELETE FROM mutations WHERE client_group_id = ? AND client_id = ? AND id <= ?",
          [.text(group), .text(clientID), .real(lmid)])
      }
    case .addMutation(let group, let m):
      try db.run(
        """
        INSERT INTO mutations (client_group_id, client_id, id, name, args, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
          .text(group), .text(m.clientID), .real(m.id), .text(m.name), json(m.args),
          .real(m.timestamp),
        ])
    }
  }

  static func loadGroup(_ id: String, from db: SQLiteDatabase) throws -> StoredGroup {
    var g = StoredGroup()
    try db.query(
      "SELECT cookie, last_mutation_ids FROM client_groups WHERE id = ?", [.text(id)]
    ) { row in
      g.cookie = try JSONParser.parse(row.bytes(0))
      for (k, v) in try JSONParser.parse(row.bytes(1)).objectValue ?? [:] {
        g.lastMutationIDs[k] = v.numberValue ?? 0
      }
    }
    try db.query("SELECT key, value FROM entries WHERE client_group_id = ?", [.text(id)]) { row in
      g.entries.set(row.text(0), try JSONParser.parse(row.bytes(1)))
    }
    try db.query(
      """
      SELECT client_id, id, name, args, timestamp FROM mutations
      WHERE client_group_id = ? ORDER BY seq
      """, [.text(id)]
    ) { row in
      g.mutations.append(
        StoredMutation(
          clientID: row.text(0), id: row.real(1), name: row.text(2),
          args: try JSONParser.parse(row.bytes(3)), timestamp: row.real(4)))
    }
    return g
  }

  /// The profile ID of every database in `directory`, made the first time
  /// (JS keeps one per browser profile).
  static func profileID(in directory: String) throws -> String {
    let path = directory + "/profile-id"
    if let f = fopen(path, "r") {
      defer { fclose(f) }
      var buffer = [CChar](repeating: 0, count: 64)
      if fgets(&buffer, 64, f) != nil {
        let id = String(
          decoding: buffer.prefix { $0 != 0 }.map { UInt8(bitPattern: $0) }, as: UTF8.self)
        if !id.isEmpty { return id }
      }
    }
    let id = "p" + randomID()
    guard let f = fopen(path, "w") else {
      throw StorageFailure(kind: .ioError, message: "Cannot write \(path)")
    }
    fputs(id, f)
    fclose(f)
    return id
  }
}

#if canImport(Darwin)
  import Darwin
#elseif canImport(Glibc)
  import Glibc
#elseif canImport(Musl)
  import Musl
#endif

/// The default database directory.
func defaultDirectory() throws -> String {
  func env(_ name: String) -> String? {
    guard let v = getenv(name) else { return nil }
    let s = String(cString: v)
    return s.isEmpty ? nil : s
  }
  #if canImport(Darwin)
    guard let home = env("HOME") else {
      throw StorageFailure(kind: .cannotOpen, message: "HOME is not set")
    }
    return home + "/Library/Application Support/Zero"
  #else
    if let data = env("XDG_DATA_HOME") { return data + "/zero" }
    guard let home = env("HOME") else {
      throw StorageFailure(kind: .cannotOpen, message: "HOME is not set")
    }
    return home + "/.local/share/zero"
  #endif
}

/// `mkdir -p`.
func makeDirectories(_ path: String) throws {
  var current = path.hasPrefix("/") ? "" : "."
  for part in path.split(separator: "/") {
    current += "/" + part
    if mkdir(current, 0o755) != 0 && errno != EEXIST {
      throw StorageFailure(kind: .cannotOpen, message: "Cannot create \(current)")
    }
  }
}
