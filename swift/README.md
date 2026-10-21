# Zero for Swift

A native [Zero](https://zero.rocicorp.dev) client for iOS and macOS. Your
app queries a local replica of the server's data, and the replica stays in
sync. Mutations apply to the replica right away and run again on your
server, whose result wins.

It's a port of the JS client (`@rocicorp/zero`) and behaves the same.
Conformance vectors generated from the JS client test it, and so do
end-to-end tests against `zero-cache` and the zbugs app.

> [!NOTE]
> Before 1.0, minor versions may change the API.

## Requirements

- Swift 6.2
- iOS 26 or macOS 26. The `Zero` module also builds on Linux, without a
  socket: pass your own `SocketFactory` to sync there.
- A `zero-cache` that speaks sync protocol 53 (Zero 1.11 or later)

## Install

```swift
// Package.swift
dependencies: [
  .package(url: "https://github.com/wevm/zero-swift", from: "0.1.0"),
],
targets: [
  .target(name: "App", dependencies: [
    .product(name: "Zero", package: "zero-swift"),
    .product(name: "ZeroUI", package: "zero-swift"),  // SwiftUI, UIKit, AppKit
  ]),
  .testTarget(name: "AppTests", dependencies: [
    .product(name: "ZeroTesting", package: "zero-swift"),
  ]),
]
```

The `zero-swift` CLI generates Swift from your TypeScript schema, and
records the vectors the drift harness checks your Swift queries and
mutators against:

```sh
npm install --save-dev zero-swift
npx zero-swift codegen
npx zero-swift drift record
```

## Example

```swift
import SwiftUI
import ZeroUI

let zero = try Zero(
  schema: .app, context: auth, mutators: [IssueMutators.create],
  options: ZeroOptions(cacheURL: "https://zero.example.com", userID: auth?.sub, auth: token))

struct IssueList: View {
  @ZeroQuery(IssueQueries.open()) var issues

  var body: some View {
    List(issues) { Text($0.title) }
  }
}

zero.mutate(IssueMutators.create(.init(id: newID(), title: "Crash on launch", created: now)))
```

## Documentation

The DocC catalogs in `Sources/*/*.docc` cover the following. Open them in
Xcode with Product › Build Documentation.

- [Getting started](Sources/Zero/Zero.docc/GettingStarted.md): schema
  generation, porting queries and mutators, creating a client.
- [Coming from the JS client](Sources/Zero/Zero.docc/JSParity.md): what
  each JS option, method, and React hook is called in Swift, and what isn't
  ported.
- [`ZeroUI`](Sources/ZeroUI/ZeroUI.docc/ZeroUI.md): SwiftUI, UIKit, and
  AppKit.
- [`ZeroTesting`](Sources/ZeroTesting/ZeroTesting.docc/ZeroTesting.md): the
  drift harness and test doubles.

[`DESIGN.md`](DESIGN.md) explains how the port is built and tested, and
[`BENCHMARKS.md`](BENCHMARKS.md) compares it with the JS client.

## Compatibility

| Zero for Swift | Sync protocol | `zero-cache` | Conformance vectors from |
| -------------- | ------------- | ------------ | ------------------------ |
| 0.1            | 53            | 1.11+        | Zero 1.12 (`main`)       |

## License

Apache-2.0
