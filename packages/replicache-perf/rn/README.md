# replicache-perf-rn

Host app for the `replicache-perf` benchmarks on React Native.

It is a plain Expo project that is **deliberately not a pnpm workspace package**.
`pnpm-workspace.yaml` globs one path segment (`packages/*`), so nesting it here
keeps it in git while leaving it out of the workspace.

As a workspace member the Android app crashes on launch with:

    java.lang.NoClassDefFoundError: Failed resolution of:
      Lexpo/modules/kotlin/types/AnyTypeProvider;
      at expo.modules.webview.DomWebViewModule.definition

an expo-modules-core class that autolinking resolved to a different copy than
the one the app was compiled against. Nested, the app has its own isolated
`node_modules` and the crash does not happen.

What has been ruled out, by trying it:

- It is not `replicache` being split into multiple instances by its optional
  expo-sqlite/op-sqlite peers. The app does not depend on `replicache` at all
  (it imports the prebuilt bundle), and as a workspace member `replicache`
  stays a single instance.
- It is not a version skew between `packages/replicache`'s `expo-sqlite`
  devDependency and this app's. Aligning both (plus `@op-engineering/op-sqlite`
  and `packages/zero`) on the latest did not fix the crash.
- It is not a stale native build. Deleting `android/` for a full regeneration
  did not fix it either.
- It is not Metro or the build: `pnpm install`, gradle and `check-types` all
  succeed as a workspace member, and Expo auto-detects the monorepo with no
  `metro.config.js`.

The root cause is not yet identified. The leading suspect is duplicate
`expo-modules-core` copies left in the shared `.pnpm` store — Expo autolinking
scans the store rather than the resolved graph — but that has not been
confirmed. Until it is, this app stays out of the workspace.

It has its own lockfile and needs `pnpm install` here once before the harness
can drive it.

Most of the workspace toolchain skips it for free, because turbo and vitest only
traverse workspace members and `replicache-perf`'s own tsconfig and oxlint task
cover `src/` only. The two root-level checks that walk the tree by path rather
than by workspace membership each needed an explicit exclusion:

- `oxlint --type-aware` at the repo root, which cannot resolve this app's
  `extends: expo/tsconfig.base` — ignored via `oxlint.base.ts`.
- `syncpack`, which would otherwise demand this app match the workspace's React
  and TypeScript versions — ignored via a version group in `.syncpackrc`.

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

The app has no UI of its own beyond a status line and the result log — the
runner is the interface. Started by hand with no control server answering, it
just reports that and stops.

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
