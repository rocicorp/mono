import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../../shared/src/asserts.ts';
import {stringify} from '../../../../../shared/src/bigint-json.ts';
import {CustomKeyMap} from '../../../../../shared/src/custom-key-map.ts';
import {must} from '../../../../../shared/src/must.ts';
import {
  majorVersionToString,
  stateVersionFromString,
  stateVersionToString,
  type StateVersion,
} from '../../../types/state-version.ts';
import type {
  BackfillCompleted,
  BackfillRequest,
  ChangeStreamMessage,
  Identifier,
  MessageBackfill,
} from '../protocol/current.ts';
import type {
  Cancelable,
  ChangeStreamMultiplexer,
  Listener,
} from './change-stream-multiplexer.ts';

function tableKey({schema, name}: Identifier) {
  return `${schema}.${name}`;
}

type BackfillStreamer = (
  req: BackfillRequest,
) => AsyncGenerator<MessageBackfill | BackfillCompleted>;

type RunningBackfillState = {
  request: BackfillRequest;
  canceledReason?: string | undefined;
  minMajorVersion: string;
};

/**
 * The BackfillManager initiates backfills for BackfillRequests from the
 * change-streamer (i.e. unfinished backfills from previous sessions)
 * or for new backfills signaled by `create-table` or `add-column` messages
 * from the change-source.
 *
 * The BackfillManager registers itself as a change stream listener in order
 * to track necessary backfills, and potentially invalidate the in-progress
 * backfill (e.g. due to a schema change) so that it can be retried at a
 * new snapshot.
 *
 * The manager also handles low priority streaming of the backfill messages
 * using the {@link ChangeStreamMultiplexer}, implementing a policy of always
 * releasing its reservation if another producer (i.e. the main change stream)
 * has messages to stream.
 */
export class BackfillManager implements Cancelable, Listener {
  readonly #lc: LogContext;

  /**
   * Tracks the metadata of required backfills based on schema changes
   * and initial backfill requests.
   */
  readonly #requiredBackfills = new CustomKeyMap<Identifier, BackfillRequest>(
    tableKey,
  );
  readonly #changeStreamer: ChangeStreamMultiplexer;
  readonly #backfillStreamer: BackfillStreamer;

  /**
   * The current running backfill. The backfill request is always also in
   * `#requiredBackfills` (technically, it can be a subset of what's in
   * `#requiredBackfills`); the request is removed from `#requiredBackfills`
   * upon completion.
   */
  #runningBackfill: RunningBackfillState | null = null;

  /** The last seen watermark in the change stream. */
  #changeStreamWatermark: StateVersion | null = null;

  constructor(
    lc: LogContext,
    changeStreamer: ChangeStreamMultiplexer,
    backfillStreamer: BackfillStreamer,
  ) {
    this.#lc = lc.withContext('component', 'backfill-manager');
    this.#changeStreamer = changeStreamer;
    this.#backfillStreamer = backfillStreamer;
  }

  run(lastWatermark: string, initialRequests: BackfillRequest[]) {
    this.#changeStreamWatermark = stateVersionFromString(lastWatermark);
    initialRequests.forEach(req =>
      this.#setRequiredBackfill('initial-request', req),
    );
    this.#checkAndStartBackfill();
  }

  // TODO: This currently starts pending backfills immediately. Consider adding
  //       backoff logic in case of pathological scenarios where requests
  //       continually fail.
  #checkAndStartBackfill() {
    if (this.#runningBackfill === null) {
      // Use the iterator to pick the first request.
      for (const first of this.#requiredBackfills.values()) {
        const state = {request: first, minMajorVersion: ''};
        const lc = this.#lc.withContext('table', first.table.name);

        this.#runningBackfill = state;
        void this.#runBackfill(lc, state)
          .then(() => this.#stopRunningBackfill('backfill exited', state))
          .catch(e => this.#stopRunningBackfill(String(e), state));
        return;
      }
    }
  }

  async #runBackfill(lc: LogContext, state: RunningBackfillState) {
    const changeStream = this.#changeStreamer; // Purely for readability

    // backfillTx is set if and only if a changeStreamer reservation has been
    // acquired and the backfill stream is inside a transaction.
    let backfillTx: string | null = null;

    /**
     * @returns the new tx watermark, or null if backfill was cancelled
     */
    const beginTxFor = async (
      msg: MessageBackfill | BackfillCompleted,
    ): Promise<string | null> => {
      assert(backfillTx === null);
      const lastWatermark = await changeStream.reserve('backfill');

      // After obtaining the changeStream reservation, check if the stream
      // had changes that resulted in invalidating / canceling this backfill.
      if (
        state.canceledReason ||
        (msg.tag === 'backfill' && msg.watermark < state.minMajorVersion)
      ) {
        if (state.canceledReason === undefined) {
          assert(msg.tag === 'backfill'); // TypeScript should have figured this out.
          this.#stopRunningBackfill(
            `row key change at ${state.minMajorVersion} ` +
              `postdates backfill watermark at ${msg.watermark}`,
            state,
          );
        }
        changeStream.release(lastWatermark);
        return null;
      }

      const {major, minor = 0n} = stateVersionFromString(lastWatermark);
      const tx = stateVersionToString({
        major,
        minor: BigInt(minor) + 1n,
      });

      void changeStream.push(['begin', {tag: 'begin'}, {commitWatermark: tx}]);
      return (backfillTx = tx);
    };

    const commitTx = () => {
      if (backfillTx) {
        void changeStream.push([
          'commit',
          {tag: 'commit'},
          {watermark: backfillTx},
        ]);
        changeStream.release(backfillTx);
      }
      backfillTx = null;
    };

    for await (const msg of this.#backfillStreamer(state.request)) {
      // If necessary, yield the reservation to the main stream.
      backfillTx && changeStream.waiterDelay() > 0 && commitTx();

      // Reserve the changeStreamer if not in a transaction.
      if ((backfillTx ??= await beginTxFor(msg)) === null) {
        lc.info?.(
          `backfill stream canceled: ${state.canceledReason}`,
          state.request,
        );
        this.#checkAndStartBackfill(); // start the next backfill if present
        return; // this backfill is canceled
      }

      // `await` to allow the change streamer to exert back pressure
      // on backfills.
      await changeStream.push(['data', msg]);
    }

    // Flush any final tx and release the stream.
    backfillTx && commitTx();
    lc.debug?.(`backfill stream exited`, state.canceledReason ?? '');
  }

  #backfillRunningFor(table: Identifier): RunningBackfillState | null {
    const state = this.#runningBackfill;
    return state?.request.table.schema === table.schema &&
      state.request.table.name === table.name
      ? state
      : null;
  }

  /**
   * Stops the running backfill for the specified `reason`. If `instance` is
   * specified, the running backfill is stopped only if it is that instance.
   * This allows the running backfill itself to clear backfill state without
   * accidentally stopping a different (e.g. subsequent) backfill.
   */
  #stopRunningBackfill(reason?: string, instance?: RunningBackfillState) {
    const backfill = this.#runningBackfill;
    if (backfill && backfill === (instance ?? backfill)) {
      backfill.canceledReason = reason;
      this.#runningBackfill = null;
      reason && this.#lc.info?.(`canceling backfill:`, reason);
    }
  }

  #setRequiredBackfill(source: string, req: BackfillRequest) {
    const exists = this.#requiredBackfills.has(req.table);
    this.#lc.info?.(`Backfill ${exists ? 'updated' : 'added'} by ${source}`, {
      backfill: req,
    });
    this.#requiredBackfills.set(req.table, req);
  }

  #deleteRequiredBackfill(source: string, id: Identifier) {
    const req = this.#requiredBackfills.get(id);
    if (req) {
      this.#lc.info?.(`Backfill dropped by ${source}`, {backfill: req});
      this.#requiredBackfills.delete(id);
    }
  }

  /**
   * Implements {@link Listener.onChange()}, invoked by the
   * {@link ChangeStreamMultiplexer}.
   */
  onChange(message: ChangeStreamMessage): void {
    if (message[0] === 'begin') {
      this.#changeStreamWatermark = stateVersionFromString(
        message[2].commitWatermark,
      );
      return;
    }
    if (message[0] === 'commit') {
      // Every commit is a candidate for starting the next backfill
      // (if one is not currently running).
      this.#checkAndStartBackfill();
      return;
    }
    if (message[0] !== 'data') {
      return;
    }
    const change = message[1];
    const {tag} = change;
    switch (tag) {
      case 'update-table-metadata': {
        const {table, new: metadata} = change;
        const backfillRequest = this.#requiredBackfills.get(table);
        if (backfillRequest) {
          this.#setRequiredBackfill(tag, {
            ...backfillRequest,
            table: {...backfillRequest.table, metadata},
          });
          if (this.#backfillRunningFor(table)) {
            this.#stopRunningBackfill(`TableMetadata updated`);
          }
        }
        break;
      }
      case 'create-table': {
        const {
          spec: {schema, name},
          metadata = null,
          backfill,
        } = change;

        if (backfill) {
          this.#setRequiredBackfill(tag, {
            table: {schema, name, metadata},
            columns: backfill,
          });
        }
        break;
      }
      case 'rename-table': {
        const {old, new: newTable} = change;
        const backfillRequest = this.#requiredBackfills.get(old);
        if (backfillRequest) {
          const {schema, name} = newTable;
          this.#deleteRequiredBackfill(tag, old);
          this.#setRequiredBackfill(tag, {
            ...backfillRequest,
            table: {...backfillRequest.table, schema, name},
          });
          if (this.#backfillRunningFor(old)) {
            this.#stopRunningBackfill(`table renamed`);
          }
        }
        break;
      }
      case 'drop-table': {
        const {id} = change;
        const backfillRequest = this.#requiredBackfills.get(id);
        if (backfillRequest) {
          this.#deleteRequiredBackfill(tag, id);
          if (this.#backfillRunningFor(id)) {
            this.#stopRunningBackfill(`table dropped`);
          }
        }
        break;
      }
      case 'add-column': {
        const {
          table,
          tableMetadata: metadata = null,
          column,
          backfill,
        } = change;
        if (backfill) {
          const backfillRequest = this.#requiredBackfills.get(table);
          if (!backfillRequest) {
            this.#setRequiredBackfill(tag, {
              table: {...table, metadata},
              columns: {[column.name]: backfill},
            });
          } else {
            this.#setRequiredBackfill(tag, {
              ...backfillRequest,
              table: {...backfillRequest.table, metadata},
              columns: {
                ...backfillRequest.columns,
                [column.name]: backfill,
              },
            });
            // Note: The running backfill need not be canceled if a
            //   new column is added. The new column will be backfilled
            //   by its own stream after the current backfill completes.
          }
        }
        break;
      }
      case 'update-column': {
        const {
          table,
          old: {name: oldName},
          new: {name: newName},
        } = change;
        if (oldName !== newName) {
          const backfillRequest = this.#requiredBackfills.get(table);
          if (backfillRequest && oldName in backfillRequest.columns) {
            const {[oldName]: colSpec, ...otherCols} = backfillRequest.columns;
            this.#setRequiredBackfill(tag, {
              ...backfillRequest,
              columns: {...otherCols, [newName]: colSpec},
            });
            const backfill = this.#backfillRunningFor(table);
            if (backfill && oldName in backfill.request.columns) {
              this.#stopRunningBackfill(`column renamed`);
            }
          }
        }
        break;
      }
      case 'drop-column': {
        const {table, column} = change;
        const backfillRequest = this.#requiredBackfills.get(table);
        if (backfillRequest && column in backfillRequest.columns) {
          const {[column]: _excluded, ...remaining} = backfillRequest.columns;
          this.#setRequiredBackfill(tag, {
            ...backfillRequest,
            columns: remaining,
          });
          const backfill = this.#backfillRunningFor(table);
          if (backfill && column in backfill.request.columns) {
            this.#stopRunningBackfill(`column dropped`);
          }
        }
        break;
      }
      case 'update': {
        const {relation, key, new: row} = change;
        const backfill = this.#backfillRunningFor(relation);
        const {major} = must(this.#changeStreamWatermark, `not in a tx`);
        if (backfill?.request.table.metadata && key !== null) {
          // A corner case that backfill is unable to correctly handle is
          // when a row's key changes; this is decomposed into a delete
          // of the old key and a set of the new key in the replica change
          // log, at which point the backfill algorithm assumes that the
          // (old) row is deleted but does not know to backfill the new row.
          // In these corner cases, the current backfill is canceled and
          // retried if its version precedes this update.
          for (const col of Object.keys(
            backfill.request.table.metadata.rowKey,
          )) {
            if (key[col] !== row[col]) {
              backfill.minMajorVersion = majorVersionToString(major);
              this.#lc.info?.(
                `key for row as changed (col: ${col}). ` +
                  `backfill data must not predate ${backfill.minMajorVersion}`,
              );
              break;
            }
          }
        }
        break;
      }
      case 'backfill-completed': {
        const {relation, columns} = change;
        const backfillRequest = this.#requiredBackfills.get(relation);
        assert(
          backfillRequest,
          () => `No BackfillRequest completed backfill ${stringify(change)}`,
        );
        const remaining = Object.entries(backfillRequest.columns).filter(
          ([col]) =>
            !(columns.includes(col) || relation.rowKey.columns.includes(col)),
        );
        if (remaining.length === 0) {
          this.#deleteRequiredBackfill(tag, relation);
        } else {
          this.#setRequiredBackfill(tag, {
            ...backfillRequest,
            columns: Object.fromEntries(remaining),
          });
        }
        // Technically the backfill is already stopping, but this method
        // cleans up the state that tracks it.
        this.#stopRunningBackfill();
        break;
      }
    }
  }

  cancel(): void {
    this.#stopRunningBackfill(`change stream canceled`);
  }
}
