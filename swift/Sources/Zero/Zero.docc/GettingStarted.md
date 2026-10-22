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
let options = ZeroOptions(cacheURL: "https://zero.example.com", userID: auth?.sub, auth: token)

let zero = try Zero(
  schema: .app, context: auth, mutators: [IssueMutators.create], options: options)
```

Leave `queryURL` and `mutateURL` unset, so zero-cache uses its own
`ZERO_QUERY_URL` and `ZERO_MUTATE_URL`. Set them only to pick one of the
URLs those allow. zero-cache closes the connection when they name any other
URL, even the same API server by its public address, and the connection
state becomes `.error` with zero-cache's reason.

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
rows without a view. A `run` of type `.complete` or `.cached` waits until
the server confirms the rows, through connection errors, as in JS; cancel
its task to stop waiting.

## Change rows

```swift
let result = zero.mutate(IssueMutators.create(.init(id: newID(), title: "Crash", created: now)))
try await result.client.value   // the optimistic run
try await result.server.value   // the server's verdict
```

The client applies the mutator at once; views update before `mutate`
returns. The server runs its own mutator; when its result arrives, the
client replaces its optimistic changes with the server's.

A server outcome of `.zeroError` means the client couldn't learn the
result, such as when the connection closed. The server may still have
applied the mutation, and the client sends pending mutations again when it
reconnects, so don't retry it as a failure.

## Follow the connection

``Connection`` is observable. Its ``ConnectionState`` is `.needsAuth` when
the server rejects the token: get a new one and call
``Connection/connect(auth:)``.

## Develop against a local backend

The client connects to any `http`, `https`, `ws`, or `wss` `cacheURL`. A
few platform rules apply to a backend on your own machine, in Docker,
OrbStack, or on the LAN:

- **Sandboxed Mac apps.** Besides the `com.apple.security.network.client`
  entitlement, add `NSLocalNetworkUsageDescription` to Info.plist. Hosts
  that resolve to a private address, such as OrbStack's `*.orb.local`,
  count as the local network. Without it, requests fail with
  `NSURLErrorNotConnectedToInternet` (-1009) and no prompt appears.
- **The iOS simulator** doesn't trust your Mac's development certificate
  authorities. For OrbStack's HTTPS domains, export its root and add it to
  the booted simulator:

  ```sh
  security find-certificate -c "OrbStack Development Root CA" -p > /tmp/orbstack.pem
  xcrun simctl keychain booted add-root-cert /tmp/orbstack.pem
  ```

- **A physical device** can't resolve `*.orb.local` or `localhost`. Publish
  zero-cache's and the API server's ports and use the Mac's LAN address
  over `http`. Allow it with `NSAllowsLocalNetworking` under
  `NSAppTransportSecurity`, and add `NSLocalNetworkUsageDescription`; the
  app asks for local network access on its first connection. Take
  `cacheURL` from per-destination build settings through Info.plist, so
  simulator and Mac builds keep their own URLs.
- **Port 4848** is only zero-cache's default. A second local zero-cache
  needs another host port; otherwise the URL may quietly reach the other
  project's zero-cache.
