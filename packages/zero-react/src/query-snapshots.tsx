import {createContext, type ReactNode} from 'react';
import type {QueryResultDetails} from './zero.ts';

/** A query's result as a server render showed it. */
export type QuerySnapshot = {
  readonly data: unknown;
  readonly type: 'unknown' | 'cached' | 'complete';
};

/**
 * The results a server render showed, by query hash. Plain JSON, for sending to
 * the client alongside the HTML.
 */
export type QuerySnapshots = Readonly<Record<string, QuerySnapshot>>;

/**
 * Gathers what `useQuery` and `useSuspenseQuery` show during a server render.
 * Use one per render, and read it after rendering has finished: with streaming,
 * that means after `onAllReady`.
 *
 * Errored queries are skipped because their details hold a retry function. The
 * client has no seed for one, so React recovers from the mismatch by rendering
 * that part of the tree on the client.
 */
export class QuerySnapshotCollector {
  readonly #snapshots: Record<string, QuerySnapshot> = {};

  record(hash: string, data: unknown, details: QueryResultDetails): void {
    if (details.type === 'error') {
      delete this.#snapshots[hash];
    } else {
      this.#snapshots[hash] = {data, type: details.type};
    }
  }

  toJSON(): QuerySnapshots {
    return this.#snapshots;
  }
}

/**
 * Serializes snapshots so they can be embedded in a `<script>` tag. Rows are
 * application data, so plain `JSON.stringify` output could close the tag and
 * inject markup. The result is still valid JSON.
 */
export function serializeQuerySnapshots(snapshots: QuerySnapshots): string {
  return JSON.stringify(snapshots).replace(
    /[<\u2028\u2029]/g,
    c => `\\u${c.charCodeAt(0).toString(16).padStart(4, '0')}`,
  );
}

export type QuerySnapshotContextValue = {
  readonly collector?: QuerySnapshotCollector | undefined;
  readonly snapshots?: QuerySnapshots | undefined;
};

export const QuerySnapshotContext = createContext<QuerySnapshotContextValue>(
  {},
);

/**
 * Hands query results from a server render to the client.
 *
 * On the server, pass a `collector` and send its contents to the client. On the
 * client, pass those contents as `snapshots`: a seeded query shows its snapshot
 * until the server confirms the live view, or the connection is lost.
 *
 * Both sides need a `ZeroProvider` given an existing `zero`, because one built
 * from options renders nothing until its first effect.
 */
export function QuerySnapshotProvider({
  collector,
  snapshots,
  children,
}: QuerySnapshotContextValue & {children: ReactNode}) {
  return (
    <QuerySnapshotContext.Provider value={{collector, snapshots}}>
      {children}
    </QuerySnapshotContext.Provider>
  );
}
