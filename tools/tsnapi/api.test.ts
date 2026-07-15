import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {extractDts} from 'tsnapi';
import {describePackagesApiSnapshots} from 'tsnapi/vitest';
import {describe, expect, it} from 'vitest';

describePackagesApiSnapshots({
  packages: [
    fileURLToPath(new URL('../../packages/zero', import.meta.url)),
    fileURLToPath(new URL('../../packages/replicache', import.meta.url)),
    fileURLToPath(new URL('../../packages/zero-events', import.meta.url)),
  ],
});

// `@rocicorp/zero` re-exports most of its public API via `export * from
// '.../mod.ts'`. tsnapi records an `export *` line verbatim in the `.d.ts`
// snapshot instead of expanding it, so a type-only export added behind a
// wildcard (e.g. a new exported type) does not change the snapshot above and
// the check passes even though the public API changed. The runtime (`.js`)
// snapshots are unaffected because tsnapi resolves `export *` at runtime, so
// this blind spot is limited to type-only exports.
//
// To close it we snapshot the *resolved* declaration files that
// `pnpm run build` emits under `packages/zero/out`, where each source module
// lists its exports by name. Adding or removing a type now shows up in a diff.
// See https://github.com/rocicorp/mono/pull/6223.
//
// Maps each `@rocicorp/zero` subpath to the explicit source module it
// re-exports. Entrypoints already covered elsewhere are intentionally omitted:
//   - `./sqlite`, `./op-sqlite`, `./expo-sqlite` re-export `replicache`
//     modules that the `replicache` package snapshots above already expand.
//   - `./pg` re-exports only `zero-server` plus the postgresjs adapter (both
//     snapshotted below), so it adds no coverage of its own.
//
// `./change-protocol/v0` re-exports `export * as v0 from './current.ts'`, and
// `current.ts` itself fans out to seven leaf modules via `export *`. Neither
// level lists names, so we snapshot the seven explicit leaf modules directly;
// together they cover the whole `v0` namespace surface.
const resolvedTypeEntries: Record<string, string> = {
  '.': 'zero-client/src/mod.d.ts',
  './bindings': 'zero-client/src/client/bindings.d.ts',
  './react': 'zero-react/src/mod.d.ts',
  './solid': 'zero-solid/src/mod.d.ts',
  './server': 'zero-server/src/mod.d.ts',
  './server/adapters/drizzle': 'zero-server/src/adapters/drizzle.d.ts',
  './server/adapters/kysely': 'zero-server/src/adapters/kysely.d.ts',
  './server/adapters/pg': 'zero-server/src/adapters/pg.d.ts',
  './server/adapters/postgresjs': 'zero-server/src/adapters/postgresjs.d.ts',
  './server/adapters/prisma': 'zero-server/src/adapters/prisma.d.ts',
  './zqlite': 'zqlite/src/mod.d.ts',
  './change-protocol/v0/control':
    'zero-cache/src/services/change-source/protocol/current/control.d.ts',
  './change-protocol/v0/data':
    'zero-cache/src/services/change-source/protocol/current/data.d.ts',
  './change-protocol/v0/downstream':
    'zero-cache/src/services/change-source/protocol/current/downstream.d.ts',
  './change-protocol/v0/json':
    'zero-cache/src/services/change-source/protocol/current/json.d.ts',
  './change-protocol/v0/path':
    'zero-cache/src/services/change-source/protocol/current/path.d.ts',
  './change-protocol/v0/status':
    'zero-cache/src/services/change-source/protocol/current/status.d.ts',
  './change-protocol/v0/upstream':
    'zero-cache/src/services/change-source/protocol/current/upstream.d.ts',
};

describe('@rocicorp/zero resolved type exports', () => {
  for (const [name, rel] of Object.entries(resolvedTypeEntries)) {
    const stem = name === '.' ? 'index' : name.replace(/^\.\//, '');
    it(`dts: ${name}`, async () => {
      const file = fileURLToPath(
        new URL(`../../packages/zero/out/${rel}`, import.meta.url),
      );
      const dts = await extractDts(file, readFileSync(file, 'utf-8'));
      await expect(dts).toMatchFileSnapshot(
        fileURLToPath(
          new URL(
            `__snapshots__/tsnapi/@rocicorp/zero-types/${stem}.snapshot.d.ts`,
            import.meta.url,
          ),
        ),
      );
    });
  }
});
