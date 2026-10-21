import Foundation
import Zero
import ZeroTesting

#if canImport(Darwin)
  import Darwin
#elseif canImport(Glibc)
  import Glibc
#endif

/// `zero.bench.ts`'s table: ten number columns, keyed by `a`.
struct User: ZeroRow, Hashable {
  var a, b, c, d, e, f, g, h, i, j: Double

  init(_ n: Int) {
    let v = Double(n)
    (a, b, c, d, e, f, g, h, i, j) = (v, v, v, v, v, v, v, v, v, v)
  }

  static let tableName = "user"
  static let columns: KeyPathNames<User> = [
    \User.a: "a", \User.b: "b", \User.c: "c", \User.d: "d", \User.e: "e",
    \User.f: "f", \User.g: "g", \User.h: "h", \User.i: "i", \User.j: "j",
  ]
  static let relationships: KeyPathNames<User> = [:]

  init(entry: ViewEntry) throws(ZeroValueError) {
    a = try entry.column("a", 0)
    b = try entry.column("b", 1)
    c = try entry.column("c", 2)
    d = try entry.column("d", 3)
    e = try entry.column("e", 4)
    f = try entry.column("f", 5)
    g = try entry.column("g", 6)
    h = try entry.column("h", 7)
    i = try entry.column("i", 8)
    j = try entry.column("j", 9)
  }

  var columnValues: Row {
    [
      "a": .number(a), "b": .number(b), "c": .number(c), "d": .number(d), "e": .number(e),
      "f": .number(f), "g": .number(g), "h": .number(h), "i": .number(i), "j": .number(j),
    ]
  }

  /// The row as `rowsPatch` puts it, with `b` through `j` set to `value`.
  static func json(_ n: Int, _ value: Int) -> String {
    #"{"op":"put","tableName":"user","value":{"a":\#(n),"b":\#(value),"c":\#(value),"#
      + #""d":\#(value),"e":\#(value),"f":\#(value),"g":\#(value),"h":\#(value),"#
      + #""i":\#(value),"j":\#(value)}}"#
  }
}

extension ZeroSchema {
  static let bench = ZeroSchema(tables: [
    TableSchema(
      name: "user",
      columns: ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"].map {
        ($0, ColumnSchema(.number))
      },
      primaryKey: ["a"])
  ])
}

let users = Query<User, [User]>(AnyQuery(.bench, "user"))

struct InsertUsers: Codable, Sendable {
  var from: Int
  var count: Int
}

struct UpdateUser: Codable, Sendable {
  var a: Int
  var value: Int
}

enum Mutators {
  static let insertUsers = defineMutator("insertUsers") { (tx, args: InsertUsers) in
    for n in args.from..<args.from + args.count {
      try tx.mutate.insert(User(n))
    }
  }
  static let updateUser = defineMutator("updateUser") { (tx, args: UpdateUser) in
    var row = User(args.a)
    let v = Double(args.value)
    (row.b, row.c, row.d, row.e, row.f, row.g, row.h, row.i, row.j) = (v, v, v, v, v, v, v, v, v)
    try tx.mutate.upsert(row)
  }
  static let all: [any Mutator<Void>] = [insertUsers, updateUser]
}

/// A client on memory storage with `rows` users, on a manual clock;
/// connected to a mock socket when `socket` is set.
@MainActor
final class Client {
  let zero: Zero<Void>
  let scheduler = ManualScheduler(now: 1_700_000_000_000)
  private let sockets = MockSocketFactory()
  private var cookie = 0

  init(rows: Int, connected: Bool = false) throws {
    var options = ZeroOptions(
      cacheURL: connected ? "http://zero.bench" : nil, userID: "bench",
      storageKey: UUID().uuidString, storage: .memory)
    options.logLevel = .error
    zero = try Zero(
      schema: .bench, mutators: Mutators.all, options: options, scheduler: scheduler,
      socketFactory: connected ? sockets : nil)
    if connected {
      // The rows come from the server, so no mutation is pending.
      scheduler.runDueTimers()
      socket.receive(text: #"["connected",{"wsid":"bench"}]"#)
      scheduler.runDueTimers()
      receive(nextPoke(rows: (0..<rows).map { User.json($0, $0) }))
    } else if rows > 0 {
      zero.mutate(Mutators.insertUsers(InsertUsers(from: 0, count: rows)))
    }
  }

  var socket: MockSocket { sockets.sockets.last! }

  /// The frames of the next poke: `rows` and the client's last mutation.
  func nextPoke(rows: [String], lastMutationID: Int? = nil) -> [String] {
    // Cookies compare as strings.
    let base = cookie == 0 ? "null" : "\"\(Self.cookie(cookie))\""
    cookie += 1
    let id = "p\(cookie)"
    var part = #""pokeID":"\#(id)","rowsPatch":[\#(rows.joined(separator: ","))]"#
    if let lastMutationID {
      part += #","lastMutationIDChanges":{"\#(zero.clientID)":\#(lastMutationID)}"#
    }
    return [
      #"["pokeStart",{"pokeID":"\#(id)","baseCookie":\#(base)}]"#,
      "[\"pokePart\",{\(part)}]",
      #"["pokeEnd",{"pokeID":"\#(id)","cookie":"\#(Self.cookie(cookie))"}]"#,
    ]
  }

  private static func cookie(_ n: Int) -> String {
    let digits = String(n)
    return String(repeating: "0", count: 12 - digits.count) + digits
  }

  /// Delivers frames from the server and lets the client apply them.
  func receive(_ frames: [String]) {
    for frame in frames { socket.receive(text: frame) }
    scheduler.runDueTimers()
  }
}

/// The process's resident memory in bytes; on Apple platforms, its peak,
/// which only the memory benchmark, running first, reads.
func residentMemory() -> Int {
  #if canImport(Darwin)
    var usage = rusage()
    getrusage(RUSAGE_SELF, &usage)
    return Int(usage.ru_maxrss)
  #else
    guard let statm = try? String(contentsOfFile: "/proc/self/statm", encoding: .utf8),
      let pages = statm.split(separator: " ").dropFirst().first.flatMap({ Int($0) })
    else { return 0 }
    return pages * Int(sysconf(Int32(_SC_PAGESIZE)))
  #endif
}
