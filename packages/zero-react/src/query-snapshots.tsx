import {createContext, type ReactNode} from 'react';
import {deepEqual, type ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {QueryResultDetails} from './zero.ts';

/** A query's result as a server render showed it. */
export type QuerySnapshot = {
  readonly data: unknown;
  readonly type: 'unknown' | 'cached' | 'complete';
  /** For a named query, so that a client that misses it can say why. */
  readonly name?: string | undefined;
  readonly args?: readonly ReadonlyJSONValue[] | undefined;
};

/** The name and args of a named query. */
export type QuerySnapshotID = {
  readonly name: string;
  readonly args: readonly ReadonlyJSONValue[];
};

/**
 * The results a server render showed, by query hash. Plain JSON, for sending to
 * the client alongside the HTML.
 */
export type QuerySnapshots = Readonly<Record<string, QuerySnapshot>>;

export type QuerySnapshotCollectorOptions = {
  /**
   * What a query that is already live renders while Zero is not connected, when
   * its rows may be out of date and may include rows the user can no longer see.
   *
   * - 'fail': wait while Zero reconnects, and fail the render once the connection
   *   is lost. This is the default.
   * - 'serve': render the rows it holds, with no indication that they may be old.
   *   Only for data that is safe to show after access to it has been revoked.
   *
   * A query with no result yet has nothing to serve, so it behaves the same
   * either way.
   */
  whenDisconnected?: 'fail' | 'serve' | undefined;
};

/**
 * Gathers what `useQuery` and `useSuspenseQuery` show during a server render.
 * Use one per render, and read it after rendering has finished: with streaming,
 * that means after `onAllReady`.
 *
 * While collecting, every query suspends until the server confirms it, so the
 * HTML is complete for a client that never runs it, such as a crawler. That
 * takes a renderer that can wait, like `renderToPipeableStream`, and nothing ends
 * the wait if zero-cache is unreachable, so abort a render that takes too long.
 *
 * Errored queries are skipped because their details hold a retry function. The
 * client has no seed for one, so React recovers from the mismatch by rendering
 * that part of the tree on the client.
 */
export class QuerySnapshotCollector {
  readonly #snapshots: Record<string, QuerySnapshot> = {};
  readonly whenDisconnected: 'fail' | 'serve';

  constructor({whenDisconnected = 'fail'}: QuerySnapshotCollectorOptions = {}) {
    this.whenDisconnected = whenDisconnected;
  }

  record(
    hash: string,
    data: unknown,
    details: QueryResultDetails,
    id?: QuerySnapshotID | undefined,
  ): void {
    if (details.type === 'error') {
      delete this.#snapshots[hash];
    } else {
      this.#snapshots[hash] = {
        data,
        type: details.type,
        ...(id && {name: id.name, args: id.args}),
      };
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

const warnedHashes = new Set<string>();
let warnedNoSnapshots = false;

/**
 * Warns that a query hydrated without a snapshot, which makes React discard the
 * server's HTML for it. Its own message only says that the HTML didn't match.
 */
export function warnHydratedWithoutSnapshot(
  hash: string,
  id: QuerySnapshotID | undefined,
  snapshots: QuerySnapshots | undefined,
): void {
  if (!snapshots) {
    if (!warnedNoSnapshots) {
      warnedNoSnapshots = true;
      // oxlint-disable-next-line no-console
      console.warn(
        'Zero: a query was hydrated with no query snapshots, so it renders ' +
          'empty until it syncs. To show what the server rendered, pass the ' +
          "server's snapshots to a QuerySnapshotProvider.",
      );
    }
    return;
  }
  if (warnedHashes.has(hash)) {
    return;
  }
  warnedHashes.add(hash);
  const label = id ? `"${id.name}"` : `with hash ${hash}`;
  const sameName = id
    ? Object.values(snapshots).filter(s => s.name === id.name)
    : [];
  let reason: string;
  if (sameName.some(s => deepEqual(s.args, id?.args))) {
    reason =
      'The server rendered it with the same args, so the Zero context ' +
      'probably differs between server and client.';
  } else if (sameName.length > 0) {
    reason =
      `The server rendered it with different args: ` +
      `${sameName.map(s => JSON.stringify(s.args)).join(', ')}, and the ` +
      `client with ${JSON.stringify(id?.args)}.`;
  } else {
    reason = 'The server did not render it, or it errored there.';
  }
  // oxlint-disable-next-line no-console
  console.warn(
    `Zero: query ${label} was hydrated with no server snapshot, so React ` +
      `renders it again on the client. ${reason}`,
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
 *
 * Snapshots are matched by query hash, which covers everything the query was
 * built from, so a query that reads the Zero context needs the same context on
 * both sides. One that differs misses its snapshot, and React recovers by
 * rendering it on the client.
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
