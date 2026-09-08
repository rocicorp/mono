# replicache-perf-rn

Host app for the `replicache-perf` benchmarks on React Native.

It is a plain Expo project that is **deliberately not a pnpm workspace package**.
`pnpm-workspace.yaml` globs one path segment (`packages/*`), so nesting it here
keeps it in git while leaving it out of the workspace. That matters for two
reasons:

- `replicache` declares `expo-sqlite` and `@op-engineering/op-sqlite` as
  optional peers. pnpm keys an installed instance by its resolved peers, so a
  workspace package supplying them would get a *different* instance of
  `replicache` than the rest of the repo, and type identity would stop unifying
  across the workspace. See the comment in `packages/zero/tool/build.ts`.
- It keeps a separate lockfile and dependency graph, so the app's Expo and React
  Native versions move independently of the workspace's.

It has its own lockfile and needs `pnpm install` here once before the harness
can drive it. Nothing in the workspace toolchain (turbo, vitest, oxlint, the
package tsconfig) reaches into this directory.

The empty `pnpm-workspace.yaml` here is deliberate: without it, `pnpm install`
run from this directory walks up, reinstalls the entire monorepo, and installs
none of this app's dependencies. Declaring a workspace root stops that walk.

pnpm's isolated (symlinked) `node_modules` works fine for Expo and React
Native here — autolinking resolves both SQLite modules through the symlinks and
gradle compiles op-sqlite's native code without special configuration, so no
`node-linker=hoisted` is needed.

The benchmarks themselves live in `mono/packages/replicache-perf`. They are
bundled by `tool/build.ts` into a single self-contained `out/rn.js`, which the
runner copies here as `benchmarks.js` (gitignored — it is build output).

## Running

From the monorepo root, not from here:

```bash
pnpm --filter replicache-perf run perf:rn -- --platform android --backend expo
pnpm --filter replicache-perf run perf:rn -- --platform ios --backend all
```

That builds the bundle, copies it in, starts a control server, boots the
device, and launches this app. The app pulls one benchmark at a time from the
server, posts each result back, and reloads its JS context between benchmarks
so each one runs in a fresh context — the same isolation the web runner gets
from `page.reload()`.

## Standalone

Started by hand (`npx expo run:android --port 8082`) with no control server
answering, the app falls back to a manual mode: pick a backend, tap Run, and it
runs the whole `replicache` group in one JS context, printing to the screen.

## Gotchas

- **Metro port.** 8081 is often taken (Docker), so the runner defaults Metro to 8082. A stray `expo start` from another project holding that port will serve
  _its_ bundle to this app — check with `lsof -i :8082` if results look wrong.
- **CocoaPods needs a UTF-8 locale.** `pod install` dies with
  `Encoding::CompatibilityError` in a shell with no `LANG`. The runner sets
  `LANG=en_US.UTF-8` for the process it spawns; if you run `expo run:ios` by
  hand, export it yourself.

## Profiling (Hermes CPU traces)

```bash
pnpm --filter replicache-perf run perf:rn -- \
  --platform android --backend mem \
  --run 'populate 1024x1000 \(clean, indexes: 0\)' \
  --profile /tmp/trace.json
```

Profiling mode does **not** use the control server for the run. The app sits
idle and the runner drives one benchmark straight through CDP
`Runtime.evaluate`, so nothing but the benchmark is inside the traced window.
It also rebuilds the bundle unminified, because otherwise every Hermes frame
reads `Ys` / `ie` / `dr`.

This depends on `App.tsx` publishing `globalThis.__replicachePerf`. If you
refactor that away, profiling breaks with "the app never published
globalThis.__replicachePerf".

Reported self time is windowed to the `performance.mark`/`measure` pairs the
bencher emits, so it covers exactly the region the benchmark reports — without
that, test-data generation is about two thirds of the trace. The written file is
a Chrome trace and can be dropped into the DevTools Performance panel.

Note that a Metro started with `CI=1` does not watch for file changes, so an
edit to `App.tsx` will not reach the device until Metro is restarted.
