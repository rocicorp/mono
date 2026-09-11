import type * as DB from '../../../../zqlite/src/db.ts';
import {currentIncarnation, type Incarnation} from './incarnation.ts';

/**
 * SQLite waits out a lock in native code, in real time, where neither the fake
 * clock nor the guard reaches. Fresh file paths for every incarnation leave no
 * lock a simulated run should ever wait for, so a wait is a harness bug and is
 * made to surface in a second rather than thirty seconds per attempt.
 */
const SIM_BUSY_TIMEOUT_MS = 1000;
const BUSY_TIMEOUT_PRAGMA = /^\s*busy_timeout\s*=/i;

/**
 * Wraps the zqlite module for a simulation test file:
 *
 * ```ts
 * vi.mock(import('../../../../zqlite/src/db.ts'), async importOriginal => {
 *   const {trackDatabases} = await import('./sqlite.ts');
 *   return trackDatabases(await importOriginal());
 * });
 * ```
 *
 * Every Database opened inside an incarnation is closed when that incarnation
 * is fenced. A crashed node's handles would otherwise stay open until garbage
 * collection, and with them any `BEGIN IMMEDIATE` they hold. Handles opened
 * outside any incarnation (the simulator's own) are the simulator's to close.
 */
export function trackDatabases(mod: typeof DB): typeof DB {
  const openBy = new WeakMap<Incarnation, Set<DB.Database>>();

  function track(owner: Incarnation, db: DB.Database): void {
    let open = openBy.get(owner);
    if (!open) {
      const created = new Set<DB.Database>();
      open = created;
      openBy.set(owner, created);
      owner.onFence(() => {
        for (const handle of [...created]) {
          try {
            handle.close();
          } catch {
            // Already closed, or mid-statement in dead code.
          }
        }
        created.clear();
      });
    }
    open.add(db);
  }

  class TrackedDatabase extends mod.Database {
    readonly #owner: Incarnation | undefined;

    constructor(...args: ConstructorParameters<typeof mod.Database>) {
      super(...args);
      const owner = currentIncarnation();
      this.#owner = owner;
      if (owner?.fenced) {
        this.close();
      } else if (owner) {
        track(owner, this);
      }
    }

    override close(): void {
      if (this.#owner) {
        openBy.get(this.#owner)?.delete(this);
      }
      super.close();
    }

    override pragma<T = unknown>(sql: string): T[] {
      return super.pragma<T>(
        BUSY_TIMEOUT_PRAGMA.test(sql)
          ? `busy_timeout = ${SIM_BUSY_TIMEOUT_MS}`
          : sql,
      );
    }
  }

  return {...mod, Database: TrackedDatabase};
}
