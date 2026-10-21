# ``ZeroUI``

Zero in SwiftUI, UIKit, and AppKit.

## Overview

Put the client in the environment, and read queries with `@ZeroQuery`.
A view updates when its query's rows change:

```swift
@main
struct IssuesApp: App {
  @State private var zero = try! Zero(
    schema: .app, context: auth, mutators: [IssueMutators.create], options: options)

  var body: some Scene {
    WindowGroup { IssueList().zero(zero) }
  }
}

struct IssueList: View {
  @ZeroQuery(IssueQueries.open()) var issues

  var body: some View {
    List(issues) { Text($0.title) }
      .overlay { if $issues.resultType == .unknown { ProgressView() } }
  }
}
```

Make the client once, in the `App` as here, or in an object the app owns.
Don't make it in a view's `@State` initializer. SwiftUI runs that
initializer each time it re-creates the view, and each client it makes
connects before being dropped.

The `zero-react` hooks map to it as follows:

| zero-react                   | ZeroUI                       |
| ---------------------------- | ---------------------------- |
| `<ZeroProvider zero={zero}>` | `.zero(zero)`                |
| `useZero()`                  | `@Environment(\.zero)`       |
| `useQuery(query)`            | `@ZeroQuery(query) var rows` |
| `useConnectionState()`       | `zero.connection.state`      |

In UIKit and AppKit, `DiffableAdapter` feeds a diffable data source from a
`MaterializedView`. Each row is an item, identified by its primary key, and
its cell is reconfigured when the row changes. `LiveQuery` in `Zero` is the
observable to use elsewhere.

`ZeroUI` re-exports `Zero`.

## Topics

### SwiftUI

- ``ZeroQuery``
- ``QueryState``

### UIKit and AppKit

- ``DiffableAdapter``
