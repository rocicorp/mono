# Coming from the JS client

What each part of `@rocicorp/zero` is called in Swift, and what isn't
ported.

## Overview

The Swift client behaves as the JS client does. Conformance vectors
generated from the JS client check it, and so do end-to-end tests against
`zero-cache`. Names follow JS where Swift allows. Swift differs in three
ways: queries and mutators are written per app instead of shared from
TypeScript, everything runs synchronously on the main actor, and some
web-only options have no counterpart.

## Options

`ZeroOptions` keeps the JS option names. `schema`, `mutators`, and `context`
are arguments of
``Zero/init(schema:context:mutators:options:scheduler:socketFactory:)``.

| JS option                                                                                         | Swift                                                                                                                            |
| ------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `cacheURL` (`server`, deprecated)                                                                 | `cacheURL`                                                                                                                       |
| `auth`, `userID`, `storageKey`                                                                    | the same                                                                                                                         |
| `logLevel`, `logSink`                                                                             | `logLevel`, `logSink` (a ``LogSink``)                                                                                              |
| `mutateURL`, `mutateHeaders`                                                                      | the same                                                                                                                         |
| `queryURL` (`getQueriesURL`, deprecated), `queryHeaders`                                          | `queryURL`, `queryHeaders`                                                                                                       |
| `onUpdateNeeded`                                                                                  | `onUpdateNeeded`, and ``Connection/updateNeeded``                                                                                  |
| `onClientStateNotFound`, `onStorageFailure`                                                       | the same                                                                                                                         |
| `disconnectTimeoutMs`, `pingTimeoutMs`, `maxHeaderLength`, `queryChangeThrottleMs`                | the same                                                                                                                         |
| `kvStore` (`'idb'` or `'mem'`)                                                                    | `storage`: `.sqlite()` or `.memory`                                                                                              |
| `hiddenTabDisconnectDelay`                                                                        | none: the client pushes pending mutations and disconnects when the app goes to the background, and reconnects when it comes back |
| `onOnlineChange` (deprecated)                                                                     | ``Connection/subscribe(_:)``                                                                                                       |
| `getTraceparent`, `slowMaterializeThreshold`, `batchViewUpdates`, `maxRecentQueries` (deprecated) | not ported                                                                                                                       |

## The client

| JS                                                                                                                                                                                                | Swift                                                                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `new Zero(options)`                                                                                                                                                                               | `try Zero(schema:context:mutators:options:)`                                                          |
| `zero.clientID`, `zero.userID`, `zero.schema`, `zero.context`, `zero.closed`                                                                                                                      | the same                                                                                              |
| `await zero.clientGroupID`                                                                                                                                                                        | `zero.clientGroupID`, known at once                                                                   |
| `zero.query.issue`, `zql.issue`                                                                                                                                                                   | the generated `zql.issue`                                                                             |
| `zero.materialize(query)`                                                                                                                                                                         | `zero.materialize(_:ttl:)` for a ``MaterializedView``, `zero.liveQuery(_:ttl:)` for a typed ``LiveQuery`` |
| `zero.run(query, {type})`                                                                                                                                                                         | `try await zero.run(_:type:ttl:)`                                                                     |
| `zero.preload(query)`                                                                                                                                                                             | `zero.preload(_:ttl:)`, a ``Preload`` with `cleanup`, `cached`, and `complete`                          |
| `zero.mutate(request)`                                                                                                                                                                            | `zero.mutate(_:)`, a ``MutatorResult`` whose `client` and `server` are ``Deferred`` outcomes              |
| `zero.connection.state`, `.subscribe`, `.connect({auth})`                                                                                                                                         | ``Connection/state``, ``Connection/subscribe(_:)``, ``Connection/connect(auth:)``                           |
| `zero.close()`                                                                                                                                                                                    | `zero.close()`                                                                                        |
| `zero.mutate.issue.insert(...)` (CRUD mutators)                                                                                                                                                   | not ported: write a mutator                                                                           |
| `zero.mutateBatch`, `zero.online`, `zero.onOnline`, `zero.inspector`, `zero.delete()`, `dropAllDatabases`, `zero.server`, `zero.storageKey`, `zero.idbName`, `zero.kvStore`, `zero.schemaVersion` | not ported                                                                                            |
| `zero.version`                                                                                                                                                                                    | ``protocolVersion``, the sync protocol version                                                          |

When the server no longer has the client's state, JS asks the app to
reload. Swift replaces the client inside the same `Zero` instead: views,
live queries, and preloads move to the new client and keep their identity.

## Queries

| JS                                                                   | Swift                                                                                          |
| -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `defineQueries({issues: {open: defineQuery(({args, ctx}) => ...)}})` | `defineQuery("issues.open") { (args: Args, ctx: Context) in ... }`, named by its registry path |
| `.where('title', 'LIKE', '%x%')`                                     | `.where(\.title, .like, "%x%")`                                                                |
| `.where('open', true)`                                               | `.where(\.open, true)`                                                                         |
| `.where(({cmp, and, or, not, exists}) => ...)`                       | `.where { $0.open == true && $0.exists(\.labels) }` (``RowExpression``)                          |
| `.whereExists('labels', q => ...)`                                   | `.whereExists(\.labels) { $0.where(...) }`                                                     |
| `.related('comments', q => q.limit(10))`                             | `.related(\.comments) { $0.limit(10) }`                                                        |
| `.orderBy('created', 'desc')`                                        | `.orderBy(\.created, .desc)`                                                                   |
| `.limit(n)`, `.start(row)`, `.one()`                                 | the same                                                                                       |
| `escapeLike(text)`                                                   | ``escapeLike(_:)``                                                                               |

Columns and relationships are key paths of the generated row types. Rows
are structs; a relationship the query didn't ask for with `related` is nil.

Comparing with nil differs by form. The two-argument
`where(\.ownerID, nil)` uses `IS`, since `= NULL` matches nothing. The
three-argument form compares as JS compares `undefined`, so
`where(\.ownerID, .eq, nil)` matches no row.

## Mutators

| JS                                                                                 | Swift                                                                                                |
| ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `defineMutators({issue: {create: defineMutator(async ({tx, args, ctx}) => ...)}})` | `defineMutator("issue.create") { (tx, args: Args, ctx: Context) in ... }`, a plain `throws` function |
| `await tx.mutate.issue.insert(row)`                                                | `try tx.mutate.issue.insert(row)`                                                                    |
| `await tx.run(query)`                                                              | `try tx.run(query)`                                                                                  |
| `tx.location`, `tx.reason`, `tx.clientID`, `tx.mutationID`                         | the same                                                                                             |
| `throw new ApplicationError(message, {details})`                                   | `throw ApplicationError(message, details:)`                                                          |

## React and SolidJS

`ZeroUI` covers the same ground for SwiftUI, UIKit, and AppKit.

| JS                                        | Swift                                                                   |
| ----------------------------------------- | ----------------------------------------------------------------------- |
| `<ZeroProvider zero={zero}>`, `useZero()` | `.zero(zero)` on a view, `@Environment(\.zero)`                         |
| `useQuery(query)`                         | `@ZeroQuery(query) var rows`, with `$rows.resultType` and `$rows.error` |
| `useConnectionState()`                    | `zero.connection.state`, which is observable                            |
| `useSuspenseQuery`, `useZeroOnline`       | not ported                                                              |
| none                                      | `DiffableAdapter`, for diffable data sources                            |

## Behavior to know

- **The main actor.** Queries, views, and mutators run synchronously on the
  main actor. Views and live queries are up to date when `mutate` returns.
  Only the network and storage are asynchronous.
- **Nulls in columns that can't be null.** When the server sends null for
  a column the schema says isn't optional, a `String`, `Double`, `Int`, or
  `Bool` property reads as `""`, `0`, or `false`. JS hands the null
  through. An enum or JSON column still fails to read.
- **JSON objects.** `JSONValue` objects are dictionaries, so JSON columns
  don't keep their key order.
- **Storage.** The replica is a SQLite database per user and URL, not
  IndexedDB. One process uses it at a time; there are no tabs to share it
  with.
