# Getting started

Generate your schema, port the queries and mutators your app uses, and
create a client.

## Add the package

The package is published from this repository's `swift/` directory to
`github.com/wevm/zero-swift`:

```swift
.package(url: "https://github.com/wevm/zero-swift", from: "0.1.0"),
```

Depend on `Zero`, and on `ZeroUI` for SwiftUI, UIKit, and AppKit. It needs
Swift 6.2 and iOS 26 or macOS 26; the core also builds on Linux.

## Generate the schema

Your TypeScript schema stays the source of truth. The `zero-swift` CLI
generates Swift from it: a ``ZeroSchema``, a row struct for each table, and
a `zql` builder with a ``Query`` for each table.

```json
{
  "schema": "../web/shared/schema.ts",
  "output": "Sources/App/Schema.swift",
  "schemaName": "app",
  "tables": {
    "issue": {"columns": {"shortID": {"type": "int"}}}
  }
}
```

```sh
npx zero-swift codegen            # reads zero-swift.json
npx zero-swift codegen --check    # in CI: fails when Schema.swift is stale
```

`number()` columns are `Double`; give a column the type `int` to make it an
`Int`, which must then hold integers. `json<T>()` columns get a generated `Codable` type when `T` is a
plain object or literal union.

## Port your queries and mutators

Write the queries and mutators your app runs in Swift, named as in your
TypeScript registries. The server runs the TypeScript ones; the client runs
the Swift ones to show results before the server answers.

```swift
enum IssueQueries {
  static let open = defineQuery("issues.open") { (_: NoArgs, auth: AuthData?) in
    zql.issue.where(\.open, true).orderBy(\.modified, .desc).limit(50)
  }
  static let byID = defineQuery("issues.byID") { (id: String, auth: AuthData?) in
    zql.issue.where(\.id, id).related(\.comments).one()
  }
}

enum IssueMutators {
  struct Create: Codable, Sendable {
    var id: String
    var title: String
    var created: Double
  }

  static let create = defineMutator("issue.create") { (tx, args: Create, auth: AuthData?) in
    guard let auth else { throw ApplicationError("Not logged in") }
    try tx.mutate.issue.insert(
      Issue(id: args.id, title: args.title, creatorID: auth.sub, created: args.created))
  }
}
```

Mutators are plain `throws` functions: they read and write through the
``Transaction`` synchronously. Pass IDs and timestamps in as arguments
rather than making them up inside, so the client and the server write the
same rows.

The drift harness checks the ports: `zero-swift drift record` runs your
TypeScript queries and mutators on sample arguments and writes what they
did, and `Drift.failures` in `ZeroTesting` lists where the Swift ones
do something else, or where a query or mutator exists on one side only.

## Create a client

```swift
var options = ZeroOptions(cacheURL: "https://zero.example.com", userID: auth?.sub, auth: token)
options.mutateURL = "https://api.example.com/mutate"   // or zero-cache's configured URL
options.queryURL = "https://api.example.com/query"

let zero = try Zero(
  schema: .app, context: auth, mutators: [IssueMutators.create], options: options)
```

Register every mutator the app may run: pending mutations are stored by
name and run again when the server's state arrives, including after a
relaunch. The client keeps its replica in SQLite (``ZeroStorage``) and
connects with `URLSession`.

## Read rows

``LiveQuery`` is an observable object whose `data` stays current:

```swift
let open = try zero.liveQuery(IssueQueries.open())
open.data        // [Issue]
open.resultType  // .unknown until the server has confirmed the rows, then .complete
```

In SwiftUI, `@ZeroQuery` from `ZeroUI` does the same for a view. For UIKit
and AppKit, `DiffableAdapter` feeds a diffable data source.

`zero.materialize(_:ttl:)` returns the untyped ``MaterializedView``, whose
``ViewSnapshot``s say which rows were inserted, removed, or changed.
`zero.run(_:type:ttl:)` reads rows once, and `zero.preload(_:ttl:)` syncs
rows without a view.

## Change rows

```swift
let result = zero.mutate(IssueMutators.create(.init(id: newID(), title: "Crash", created: now)))
try await result.client.value   // the optimistic run
try await result.server.value   // the server's verdict
```

The client applies the mutator at once; views update before `mutate`
returns. The server runs its own mutator; when its result arrives, the
client replaces its optimistic changes with the server's.

## Follow the connection

``Connection`` is observable. Its ``ConnectionState`` is `.needsAuth` when
the server rejects the token: get a new one and call
``Connection/connect(auth:)``.
