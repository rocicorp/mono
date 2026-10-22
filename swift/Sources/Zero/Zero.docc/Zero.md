# ``Zero``

A native client for Zero: query a local replica of your server's data that
stays in sync, and change it optimistically.

## Overview

Zero keeps the rows your queries need on the device. Queries run against the
local replica and their results stay current as the server's data changes;
mutators change the replica at once and run again on your server, whose
result wins. This package is the Swift port of the JS client
(`@rocicorp/zero`), and behaves the same: it is tested against vectors
generated from the JS client, and against a real `zero-cache`.

Your TypeScript schema stays the source of truth. `zero-swift codegen`
generates the Swift row types and schema from it; you write the queries and
mutators your app uses in Swift, under the same names as in TypeScript, and
the drift harness (`ZeroTesting`) checks that they do what the TypeScript
ones do.

```swift
let zero = try Zero(
  schema: .app, context: auth, mutators: [IssueMutators.create],
  options: ZeroOptions(cacheURL: "https://zero.example.com", userID: auth?.sub, auth: token))

let issues = try zero.liveQuery(IssueQueries.open())   // observable, stays current
zero.mutate(IssueMutators.create(.init(id: newID(), title: "Crash on launch")))
```

The client runs on the main actor: queries, views, and mutators are
synchronous, and only the network and storage are asynchronous. On Apple
platforms it connects with `URLSession`, persists to SQLite, and follows the
app's lifecycle and the network on its own; `ZeroUI` adds SwiftUI, UIKit,
and AppKit integration.

## Topics

### Essentials

- <doc:GettingStarted>
- <doc:JSParity>
- ``Zero/Zero``
- ``ZeroOptions``
- ``AnyZero``

### Defining queries and mutators

Define them with `defineQuery(_:_:)` and `defineMutator(_:_:)`, named as in
your TypeScript registries.

- ``QueryDefinition``
- ``NoArgsQueryDefinition``
- ``QueryRequest``
- ``NamedQuery``
- ``MutatorDefinition``
- ``NoArgsMutatorDefinition``
- ``MutateRequest``
- ``Mutator``
- ``NoArgs``
- ``Transaction``
- ``TransactionMutate``
- ``TableMutator``
- ``ApplicationError``

### Building queries

- ``Query``
- ``RowExpression``
- ``Column``
- ``Condition``
- ``StaticParameter``
- ``escapeLike(_:)``
- ``AnyQuery``
- ``ExpressionBuilder``
- ``AST``
- ``CustomQueryID``

### Reading rows

- ``LiveQuery``
- ``QueryResult``
- ``MaterializedView``
- ``ViewSnapshot``
- ``ViewValue``
- ``ViewEntry``
- ``RowID``
- ``ResultType``
- ``QueryError``
- ``ErroredQuery``
- ``RunType``
- ``Preload``
- ``TTL``

### Mutation results

- ``MutatorResult``
- ``MutationOutcome``
- ``Deferred``

### The connection

- ``Connection``
- ``ConnectionState``
- ``ConnectionStatus``
- ``NeedsAuthReason``
- ``UpdateNeededReason``
- ``protocolVersion``

### The schema and generated code

These are what `zero-swift codegen` writes; apps rarely use them directly.

- ``ZeroSchema``
- ``TableSchema``
- ``ColumnSchema``
- ``ValueType``
- ``RelationshipHop``
- ``Relationship``
- ``Cardinality``
- ``ZeroRow``
- ``ZeroValue``
- ``ZeroEnum``
- ``Nullable``
- ``Indirect``
- ``KeyPathNames``
- ``ZeroValueError``
- ``ZeroObjectReader``
- ``ZeroObjectWriter``

### JSON

- ``JSONValue``
- ``JSONObject``
- ``Row``
- ``JSONValueEncoder``
- ``JSONValueDecoder``

### Storage and logging

- ``ZeroStorage``
- ``StorageFailure``
- ``LogLevel``
- ``LogSink``
- ``DefaultLogSink``

### Transports and scheduling

- ``Transport``
- ``Socket``
- ``SocketMessage``
- ``SocketClosed``
- ``TransportSocketFactory``
- ``SocketFactory``
- ``SocketConnection``
- ``SocketEvents``
- ``platformSocketFactory()``
- ``Scheduler``
- ``ScheduledTimer``
- ``SystemScheduler``
