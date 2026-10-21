# zbugs on the Zero Swift client

[zbugs](../zbugs) as a native app for iOS and macOS:

- `ZBugs`: zbugs' schema, queries, and mutators (`apps/zbugs/shared`),
  ported to Swift. Builds on Linux too.
- `ZBugsUI`: the app's screens in SwiftUI. The issue list and an issue's
  comments page in as they scroll, through `ZeroVirtual`'s `VirtualList`, as
  zbugs' list page and comments do (`useZeroVirtualizer`); the issue list is
  also in UIKit (iOS) and AppKit (macOS), through `VirtualListView`. A
  relaunch opens the list at the selected issue, or else where it was.
- `App/`: the app target, as an [XcodeGen](https://github.com/yonaskolb/XcodeGen)
  project.
- `server/server.ts`: zbugs' backend from this repository's sources, for the
  app and the end-to-end tests.

## Running the app

Start the backend. It needs Node 24, `psql`, and Postgres with
`wal_level = logical` (for example `docker compose up` in
`apps/zbugs/docker`, which listens on 6434):

```sh
ZBUGS_PG=postgresql://user:password@127.0.0.1:6434/postgres \
  node apps/zbugs-swift/server/server.ts
```

It recreates the `zbugs_swift` database with zbugs' seed data, then prints a
token for a crew user and `ready <zero-cache> <API>` (ports 4848 and 5173).

Each start makes new signing keys and a new database, so an app that ran
against the last one needs the new token and starts over from an empty local
store. To keep both across restarts, set `ZBUGS_KEEP=1` and a directory for
the replica and the keys:

```sh
ZBUGS_KEEP=1 ZBUGS_DIR=~/.zbugs-swift ZBUGS_PG=... \
  node apps/zbugs-swift/server/server.ts
```

It seeds the database only when it doesn't exist; drop it, or run once
without `ZBUGS_KEEP`, to start over.

Then generate the Xcode project and run the `ZBugsApp` scheme on a Mac or an
iOS simulator:

```sh
cd apps/zbugs-swift/App
xcodegen generate
open ZBugs.xcodeproj
```

Log in from the account menu with the printed token, or through GitHub when
the API server has `GITHUB_CLIENT_ID` and `GITHUB_CLIENT_SECRET` (it
redirects back to `zbugs-swift://auth`). Servers… points the app elsewhere.

On an iPhone or an iPad, start the backend with `ZBUGS_HOST=0.0.0.0` so the
device can reach it, and in the app set Servers… to this Mac's address on the
network (for example `http://192.168.1.20:4848` and
`http://192.168.1.20:5173`), then log in with the token. The app keeps both.
A token and servers passed in its environment, as Xcode's scheme or the UI
test passes them, last only for that launch: opened from the home screen, the
app goes back to the saved ones.

## End-to-end tests

`BackendTests` runs the Swift queries and mutators against the backend: the
API server runs the TS versions, and the test checks that both sides agree.
It is skipped unless the backend's URLs and a token are set:

```sh
ZBUGS_E2E_URL=http://127.0.0.1:4848 ZBUGS_E2E_API=http://127.0.0.1:5173 \
  ZBUGS_E2E_TOKEN=<token> swift test --filter BackendTests
```

`App/UITests` drives the app through each screen against the backend and
keeps a screenshot of each one. The `zbugs screenshots` workflow runs it on
an iPhone and an iPad simulator and on macOS, then again with the backend
stopped, where each screen comes from the app's local store, and uploads the
screenshots as an artifact. To run it locally, generate the project and pass
the backend's token:

```sh
TEST_RUNNER_ZBUGS_TOKEN=<token> xcodebuild test -project App/ZBugs.xcodeproj \
  -scheme ZBugsApp -destination 'platform=iOS Simulator,name=iPhone 17'
```

On macOS the destination is `platform=macOS`. On an iPhone or an iPad, start
the backend with `ZBUGS_HOST=0.0.0.0` so the device can reach it, and pass
its address on this Mac's network as well, for example
`TEST_RUNNER_ZBUGS_CACHE_URL=http://192.168.1.20:4848` and
`TEST_RUNNER_ZBUGS_API_URL=http://192.168.1.20:5173`.

The app reads `ZBUGS_TOKEN`, `ZBUGS_CACHE_URL`, and `ZBUGS_API_URL` from its
environment as well; they override the saved login and servers.

## Keeping the port current

- `Sources/ZBugs/Schema.swift` is generated from `apps/zbugs/shared/schema.ts`
  by `zero-swift codegen` (config in `zero-swift.json`).
- `Queries.swift` and `Mutators.swift` are ports of `queries.ts` and
  `mutators.ts` (with `auth.ts` in `Auth.swift`).
- `Tests/ZBugsTests/drift-vectors.json` records what the TS queries and
  mutators do on the samples in `drift-samples.ts`. `DriftTests` fails when
  the Swift port does something else, or when a query or mutator exists on
  only one side.

After changing a TS query or mutator (or the samples), re-record and port the
change:

```sh
cd packages/zero-swift-cli
UPDATE_GENERATED=1 pnpm test   # rewrites Schema.swift and drift-vectors.json
cd ../../apps/zbugs-swift
swift test
```

CI fails while the record is out of date (`pnpm --filter zero-swift run test`)
or the port drifts from it (`swift test` here).
