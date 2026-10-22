import CSQLite
import Foundation
import Synchronization
import Testing
import ZeroTesting

@testable import Zero

/// Storage edge cases checked against the JS client (`packages/replicache`
/// persist and kv stores, `packages/zero-client` client groups and active
/// clients): storage failures mid-session and at open, two instances on one
/// storage, and relaunches.
@Suite(.serialized) @MainActor struct SweepStorageTests {
  static let create = defineMutator("issue.create") { (tx, id: String) in
    try tx.mutate.issue.insert([
      "id": .string(id), "title": "", "description": "", "closed": false, "ownerId": .null,
      "createdAt": 1,
    ])
  }

  /// Every SQLite file of these tests is in a directory with
  /// `SQLiteFaults.marker` in its name, so faults reach only them.
  let directory = NSTemporaryDirectory() + SQLiteFaults.marker + UUID().uuidString

  final class Failures: @unchecked Sendable { var kinds: [StorageFailure.Kind] = [] }

  init() {
    _ = SQLiteFaults.installed
    SQLiteFaults.reset()
  }

  func makeZero(
    storageKey: String = "k", storage: ZeroStorage? = nil, at now: Double = 0,
    cacheURL: String? = nil, sockets: MockSocketFactory? = nil,
    scheduler: ManualScheduler? = nil,
    onStorageFailure: (@MainActor @Sendable (StorageFailure) -> Void)? = nil
  ) throws -> Zero<Void> {
    var options = ZeroOptions(
      cacheURL: cacheURL, userID: "u", storageKey: storageKey,
      storage: storage ?? .sqlite(directory: directory))
    options.onStorageFailure = onStorageFailure
    options.logSink = RecordingLogSink()
    return try Zero(
      schema: .test, mutators: [Self.create], options: options,
      scheduler: scheduler ?? ManualScheduler(now: now), socketFactory: sockets)
  }

  /// Lets the main actor run the tasks that report write failures.
  func settle(until done: () -> Bool = { false }) async throws {
    for _ in 0..<50 {
      if done() { return }
      try await Task.sleep(for: .milliseconds(10))
    }
  }

  func databasePath(storageKey: String = "k") -> String {
    directory + "/"
      + ReplicaDatabase.fileName(
        ReplicaDatabase.name(
          userID: "u", storageKey: storageKey, mutateURL: nil, queryURL: nil,
          schemaHash: ZeroSchema.test.clientSchema.hash))
  }

  func cleanUp() {
    SQLiteFaults.reset()
    try? FileManager.default.removeItem(atPath: directory)
  }

  // MARK: - Storage failures

  /// JS: every Replicache instance hears about the storage failure of its
  /// own writes (`#handleStorageFailure`, per instance). Here two clients
  /// share one database and its single `onWriteFailure`: the client opened
  /// last owns it, even after it closed, so the client whose write failed
  /// is never told.
  @Test(.timeLimit(.minutes(1)))
  func aWriteFailureIsReportedToTheClientThatWroteAfterAnotherClosed() async throws {
    defer { cleanUp() }
    let a = Failures()
    let b = Failures()
    let first = try makeZero(onStorageFailure: { a.kinds.append($0.kind) })
    let second = try makeZero(onStorageFailure: { b.kinds.append($0.kind) })
    #expect(first.engine.database === second.engine.database)
    second.close()
    first.engine.database.flush()

    SQLiteFaults.failWrites(SQLITE_FULL)
    first.mutate(Self.create("i1"))
    first.engine.database.flush()
    try await settle(until: { !a.kinds.isEmpty || !b.kinds.isEmpty })

    #expect(a.kinds == [.full])
    #expect(b.kinds == [])
    first.close()
  }

  /// JS stops persisting at the first storage failure, so what is on disk
  /// is a state the client had. Here the writes queued before the failure
  /// was seen keep landing: a full disk that frees up between two
  /// mutations leaves the second on disk without the first.
  @Test(.timeLimit(.minutes(1)))
  func writesQueuedBeforeAFailureDoNotLandAfterIt() async throws {
    defer { cleanUp() }
    let failures = Failures()
    let first = try makeZero(onStorageFailure: { failures.kinds.append($0.kind) })
    first.engine.database.flush()

    SQLiteFaults.failWrites(SQLITE_FULL, times: 1)
    first.mutate(Self.create("i1"))
    first.mutate(Self.create("i2"))
    first.engine.database.flush()
    try await settle(until: { !failures.kinds.isEmpty })
    #expect(failures.kinds == [.full])
    first.close()

    let second = try makeZero(at: 1000)
    let persisted = second.engine.store.pending.map(\.id)
    #expect([[], [1], [1, 2]].contains(persisted), "persisted \(persisted)")
    second.close()
  }

  /// JS does not count `SQLITE_BUSY` as a storage failure: "it is
  /// contention, and busy_timeout plus a retry is the right answer"
  /// (`classifySQLiteError`), and the next persist writes everything the
  /// failed one had. Here another connection holding the write lock past
  /// the busy timeout (an app extension on the same file) stops
  /// persistence for the session and loses the mutation.
  @Test(.timeLimit(.minutes(1)))
  func aBusyDatabaseIsRetriedNotAStorageFailure() async throws {
    defer { cleanUp() }
    let failures = Failures()
    let first = try makeZero(onStorageFailure: { failures.kinds.append($0.kind) })
    first.engine.database.flush()

    let other = Box(try SQLiteDatabase(path: databasePath()))
    try other.value.execute("BEGIN IMMEDIATE")
    let release = Task.detached {
      try? await Task.sleep(for: .seconds(5.5))
      try? other.value.execute("COMMIT")
      other.value.close()
    }
    first.mutate(Self.create("i1"))
    first.engine.database.flush()
    await release.value
    try await settle()
    first.mutate(Self.create("i2"))
    first.engine.database.flush()
    try await settle()
    #expect(failures.kinds == [])
    first.close()

    let second = try makeZero(at: 1000)
    #expect(second.engine.store.pending.map(\.id) == [1, 2])
    second.close()
  }

  /// JS reports a storage failure on any store read, the open's included
  /// (an open that fails moves the instance onto memory and calls
  /// `onStorageFailure`). Here a read failure loading the client group's
  /// snapshot and mutations, after the database opened, is recorded before
  /// the engine listens: it is never reported, and the client runs with an
  /// empty replica and nothing persisted for the session.
  @Test(.timeLimit(.minutes(1)))
  func aReadFailureLoadingTheClientGroupIsReported() async throws {
    defer { cleanUp() }
    let first = try makeZero()
    first.mutate(Self.create("i1"))
    first.close()

    // Fail the reads of the mutations table's root page.
    let db = try SQLiteDatabase(path: databasePath())
    var pageSize: Int64 = 0
    var root: Int64 = 0
    try db.query("PRAGMA page_size") { pageSize = $0.integer(0) }
    try db.query("SELECT rootpage FROM sqlite_master WHERE name = 'mutations'") {
      root = $0.integer(0)
    }
    db.close()
    #expect(root > 1)
    SQLiteFaults.failReads(atOffset: (root - 1) * pageSize)

    let failures = Failures()
    let second = try makeZero(at: 1000, onStorageFailure: { failures.kinds.append($0.kind) })
    try await settle(until: { !failures.kinds.isEmpty })
    // The read did fail.
    #expect(SQLiteFaults.readFaultHits.load(ordering: .sequentiallyConsistent) > 0)
    #expect(failures.kinds == [.ioError])
    second.close()
  }

  /// JS falls back to a memory store of the same name, which lives for the
  /// process and is shared by name like `kvStore: 'mem'`
  /// (`fallbackStoreProvider`, `MemStore`). Here each client gets its own
  /// empty memory database, so a client made again after the first closed
  /// starts in a new client group and the pending mutations are gone.
  @Test func clientsOnTheMemoryFallbackShareIt() async throws {
    let storage = ZeroStorage.sqlite(directory: "/dev/null/" + UUID().uuidString)
    let first = try makeZero(storageKey: randomID(), storage: storage)
    first.mutate(Self.create("i1"))
    first.close()

    let second = try makeZero(
      storageKey: first.engine.options.storageKey!, storage: storage, at: 1000)
    #expect(second.clientGroupID == first.clientGroupID)
    #expect(second.engine.store.pending.map(\.clientID) == [first.clientID])
    #expect(try await second.run(zql.issue).map(\.id) == ["i1"])
    second.close()
  }

  // MARK: - Relaunches

  /// Opens the newest socket and acknowledges it.
  func connect(_ sockets: MockSocketFactory, _ scheduler: ManualScheduler) throws -> MockSocket {
    scheduler.runDueTimers()
    let socket = try #require(sockets.sockets.last)
    socket.receive(text: #"["connected",{"wsid":"w"}]"#)
    scheduler.runDueTimers()
    return socket
  }

  /// The pending mutations come back in the order they were made, and are
  /// pushed in it.
  @Test func keepsTheOrderOfManyPendingMutationsAcrossALaunch() throws {
    defer { cleanUp() }
    let first = try makeZero()
    for i in 1...300 { first.mutate(Self.create("i\(i)")) }
    first.close()

    let scheduler = ManualScheduler(now: 1_700_000_000_000)
    let sockets = MockSocketFactory()
    let second = try makeZero(
      cacheURL: "http://zero.test", sockets: sockets, scheduler: scheduler)
    #expect(second.engine.store.pending.map(\.id) == (1...300).map(Double.init))
    let socket = try connect(sockets, scheduler)
    scheduler.runDueTimers()
    let pushed = socket.messages.compactMap { text -> Double? in
      guard text.hasPrefix(#"["push""#) else { return nil }
      return (try? JSONParser.parse(Array(text.utf8)))?[1]?["mutations"]?[0]?["id"]?.numberValue
    }
    #expect(pushed == (1...300).map(Double.init))
    second.close()
  }

  /// A client group the server lost stays disabled on disk: the next
  /// launch starts a new one (`findMatchingClient` skips disabled groups).
  @Test func aDisabledClientGroupIsNotJoinedAfterALaunch() throws {
    defer { cleanUp() }
    let scheduler = ManualScheduler(now: 1_700_000_000_000)
    let sockets = MockSocketFactory()
    var options = ZeroOptions(
      cacheURL: "http://zero.test", userID: "u", storageKey: "k",
      storage: .sqlite(directory: directory))
    options.logSink = RecordingLogSink()
    options.onClientStateNotFound = {}
    let first = try Zero(
      schema: .test, mutators: [Self.create], options: options, scheduler: scheduler,
      socketFactory: sockets)
    let socket = try connect(sockets, scheduler)
    socket.receive(
      text: #"["error",{"kind":"ClientNotFound","message":"gone","origin":"zeroCache"}]"#)
    scheduler.runDueTimers()
    first.close()

    let second = try makeZero(at: 1_700_000_100_000)
    #expect(second.clientGroupID != first.clientGroupID)
    second.close()
  }

  /// A file cut short by a crash: the client runs on memory and says so.
  @Test func runsOnMemoryWhenTheFileIsTruncated() async throws {
    defer { cleanUp() }
    let first = try makeZero()
    for i in 1...50 { first.mutate(Self.create("i\(i)")) }
    first.close()
    let path = databasePath()
    let size = try #require(
      try FileManager.default.attributesOfItem(atPath: path)[.size] as? Int)
    #expect(truncate(path, off_t(size / 2)) == 0)
    _ = unlink(path + "-wal")

    let failures = Failures()
    let second = try makeZero(at: 1000, onStorageFailure: { failures.kinds.append($0.kind) })
    try await settle(until: { !failures.kinds.isEmpty })
    #expect(failures.kinds.count == 1)
    second.mutate(Self.create("x"))
    #expect(try await second.run(zql.issue).map(\.id).contains("x"))
    second.close()
  }

  // MARK: - The replica's map

  /// The sorted map keeps unique keys in order through many puts and
  /// deletes, and its diffs agree with a dictionary
  /// (`putMany` rebalancing, rocicorp/mono#5923).
  @Test func kvMapStaysSortedAndUniqueThroughManyWrites() {
    var rng = SplitMix(seed: 42)
    var map = KVMap()
    var model: [String: JSONValue] = [:]
    for round in 0..<20 {
      let old = map
      let oldModel = model
      for _ in 0..<500 {
        let key = "e/issues/" + String(rng.next() % 2000)
        if rng.next() % 3 == 0 {
          map.delete(key)
          model[key] = nil
        } else {
          let v = JSONValue.number(Double(round))
          map.set(key, v)
          model[key] = v
        }
      }
      var keys: [String] = []
      for e in map.entries { keys.append(e.key) }
      #expect(keys == model.keys.sorted(by: StringCompare.utf8Less))
      let diff = KVDiff.between(old, map)
      var expected = 0
      for key in Set(oldModel.keys).union(model.keys) where oldModel[key] != model[key] {
        expected += 1
      }
      #expect(diff.count == expected)
      #expect(diff.map(\.key) == diff.map(\.key).sorted(by: StringCompare.utf8Less))
    }
  }
}

struct SplitMix {
  var state: UInt64
  init(seed: UInt64) { state = seed }
  mutating func next() -> UInt64 {
    state &+= 0x9E37_79B9_7F4A_7C15
    var z = state
    z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
    z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
    return z ^ (z >> 31)
  }
}

final class Box<T>: @unchecked Sendable {
  let value: T
  init(_ value: T) { self.value = value }
}

// MARK: - Fault injection

/// A SQLite VFS, made the default, that forwards to the system one and
/// fails writes or reads of the files whose path has `marker` on request.
enum SQLiteFaults {
  static let marker = "sweep-storage-fault-"
  /// The result code writes fail with, or 0.
  static let writeFault = Atomic<Int32>(0)
  /// How many more writes fail; negative for all.
  static let writeFaultsLeft = Atomic<Int>(0)
  /// The offset in a main database file whose reads fail, or -1.
  static let readFaultOffset = Atomic<Int64>(-1)
  /// How many reads failed.
  static let readFaultHits = Atomic<Int>(0)

  static func failWrites(_ code: Int32, times: Int = -1) {
    writeFaultsLeft.store(times, ordering: .sequentiallyConsistent)
    writeFault.store(code, ordering: .sequentiallyConsistent)
  }

  static func failReads(atOffset offset: Int64) {
    readFaultOffset.store(offset, ordering: .sequentiallyConsistent)
  }

  static func reset() {
    writeFault.store(0, ordering: .sequentiallyConsistent)
    readFaultOffset.store(-1, ordering: .sequentiallyConsistent)
    readFaultHits.store(0, ordering: .sequentiallyConsistent)
  }

  /// Whether the next write of a marked file fails, and with what.
  static func takeWriteFault() -> Int32 {
    let code = writeFault.load(ordering: .sequentiallyConsistent)
    guard code != 0 else { return 0 }
    let left = writeFaultsLeft.load(ordering: .sequentiallyConsistent)
    if left == 0 { return 0 }
    if left > 0 {
      if writeFaultsLeft.subtract(1, ordering: .sequentiallyConsistent).newValue == 0 {
        writeFault.store(0, ordering: .sequentiallyConsistent)
      }
    }
    return code
  }

  // A wrapped file: our methods, a tag, then the system VFS's file.
  static let header = 16
  static let marked: Int64 = 1
  static let mainDB: Int64 = 2

  nonisolated(unsafe) static var system: UnsafeMutablePointer<sqlite3_vfs>!
  nonisolated(unsafe) static var methods: UnsafeMutablePointer<sqlite3_io_methods>!

  static let installed: Bool = {
    system = sqlite3_vfs_find(nil)
    let vfs = UnsafeMutablePointer<sqlite3_vfs>.allocate(capacity: 1)
    vfs.initialize(to: system.pointee)
    vfs.pointee.pNext = nil
    vfs.pointee.zName = UnsafePointer(strdup("sweep-faults"))
    vfs.pointee.szOsFile = system.pointee.szOsFile + Int32(header)
    vfs.pointee.xOpen = { _, name, file, flags, outFlags in
      let inner = SQLiteFaults.inner(file!)
      let rc = SQLiteFaults.system.pointee.xOpen!(SQLiteFaults.system, name, inner, flags, outFlags)
      var tag: Int64 = 0
      if let name, strstr(name, SQLiteFaults.marker) != nil { tag |= SQLiteFaults.marked }
      if flags & SQLITE_OPEN_MAIN_DB != 0 { tag |= SQLiteFaults.mainDB }
      UnsafeMutableRawPointer(file!).advanced(by: 8).storeBytes(of: tag, as: Int64.self)
      file!.pointee.pMethods =
        inner.pointee.pMethods == nil ? nil : UnsafePointer(SQLiteFaults.methods)
      return rc
    }

    let m = UnsafeMutablePointer<sqlite3_io_methods>.allocate(capacity: 1)
    m.initialize(to: sqlite3_io_methods())
    // Version 2: WAL, without memory-mapped reads, so reads reach xRead.
    m.pointee.iVersion = 2
    m.pointee.xClose = { f in
      let inner = SQLiteFaults.inner(f!)
      return SQLiteFaults.real(inner).xClose!(inner)
    }
    m.pointee.xRead = { f, buf, amt, off in
      let inner = SQLiteFaults.inner(f!)
      if SQLiteFaults.tag(f!) == SQLiteFaults.marked | SQLiteFaults.mainDB,
        off == SQLiteFaults.readFaultOffset.load(ordering: .sequentiallyConsistent)
      {
        SQLiteFaults.readFaultHits.add(1, ordering: .sequentiallyConsistent)
        return SQLITE_IOERR | (1 << 8)
      }
      return SQLiteFaults.real(inner).xRead!(inner, buf, amt, off)
    }
    m.pointee.xWrite = { f, buf, amt, off in
      let inner = SQLiteFaults.inner(f!)
      if SQLiteFaults.tag(f!) & SQLiteFaults.marked != 0 {
        let code = SQLiteFaults.takeWriteFault()
        if code != 0 { return code }
      }
      return SQLiteFaults.real(inner).xWrite!(inner, buf, amt, off)
    }
    m.pointee.xTruncate = { f, size in
      let inner = SQLiteFaults.inner(f!)
      return SQLiteFaults.real(inner).xTruncate!(inner, size)
    }
    m.pointee.xSync = { f, flags in
      let inner = SQLiteFaults.inner(f!)
      return SQLiteFaults.real(inner).xSync!(inner, flags)
    }
    m.pointee.xFileSize = { f, size in
      let inner = SQLiteFaults.inner(f!)
      return SQLiteFaults.real(inner).xFileSize!(inner, size)
    }
    m.pointee.xLock = { f, lock in
      let inner = SQLiteFaults.inner(f!)
      return SQLiteFaults.real(inner).xLock!(inner, lock)
    }
    m.pointee.xUnlock = { f, lock in
      let inner = SQLiteFaults.inner(f!)
      return SQLiteFaults.real(inner).xUnlock!(inner, lock)
    }
    m.pointee.xCheckReservedLock = { f, out in
      let inner = SQLiteFaults.inner(f!)
      return SQLiteFaults.real(inner).xCheckReservedLock!(inner, out)
    }
    m.pointee.xFileControl = { f, op, arg in
      let inner = SQLiteFaults.inner(f!)
      return SQLiteFaults.real(inner).xFileControl!(inner, op, arg)
    }
    m.pointee.xSectorSize = { f in
      let inner = SQLiteFaults.inner(f!)
      return SQLiteFaults.real(inner).xSectorSize!(inner)
    }
    m.pointee.xDeviceCharacteristics = { f in
      let inner = SQLiteFaults.inner(f!)
      return SQLiteFaults.real(inner).xDeviceCharacteristics!(inner)
    }
    m.pointee.xShmMap = { f, region, size, extend, out in
      let inner = SQLiteFaults.inner(f!)
      return SQLiteFaults.real(inner).xShmMap!(inner, region, size, extend, out)
    }
    m.pointee.xShmLock = { f, offset, n, flags in
      let inner = SQLiteFaults.inner(f!)
      return SQLiteFaults.real(inner).xShmLock!(inner, offset, n, flags)
    }
    m.pointee.xShmBarrier = { f in
      let inner = SQLiteFaults.inner(f!)
      SQLiteFaults.real(inner).xShmBarrier!(inner)
    }
    m.pointee.xShmUnmap = { f, delete in
      let inner = SQLiteFaults.inner(f!)
      return SQLiteFaults.real(inner).xShmUnmap!(inner, delete)
    }
    methods = m
    return sqlite3_vfs_register(vfs, 1) == SQLITE_OK
  }()

  static func inner(_ f: UnsafeMutablePointer<sqlite3_file>) -> UnsafeMutablePointer<sqlite3_file> {
    UnsafeMutableRawPointer(f).advanced(by: header).assumingMemoryBound(to: sqlite3_file.self)
  }

  static func tag(_ f: UnsafeMutablePointer<sqlite3_file>) -> Int64 {
    UnsafeMutableRawPointer(f).advanced(by: 8).load(as: Int64.self)
  }

  static func real(_ inner: UnsafeMutablePointer<sqlite3_file>) -> sqlite3_io_methods {
    inner.pointee.pMethods.pointee
  }
}
