import type {LogContext} from '@rocicorp/logger';
import {WebSocket} from 'ws';
import {assert, unreachable} from '../../../../../shared/src/asserts.ts';
import {stringify} from '../../../../../shared/src/bigint-json.ts';
import {deepEqual} from '../../../../../shared/src/json.ts';
import type {SchemaValue} from '../../../../../zero-schema/src/table-schema.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {computeZqlSpecs} from '../../../db/lite-tables.ts';
import {StatementRunner} from '../../../db/statements.ts';
import type {ShardConfig, ShardID} from '../../../types/shards.ts';
import {stream} from '../../../types/streams.ts';
import {
  AutoResetSignal,
  type ReplicationConfig,
} from '../../change-streamer/schema/tables.ts';
import {ChangeProcessor} from '../../replicator/change-processor.ts';
import {ReplicationStatusPublisher} from '../../replicator/replication-status.ts';
import {
  createReplicationStateTables,
  getSubscriptionState,
  initReplicationState,
  type SubscriptionState,
} from '../../replicator/schema/replication-state.ts';
import type {ChangeSource, ChangeStream} from '../change-source.ts';
import {changeStreamMessageSchema} from '../protocol/current/downstream.ts';
import {
  type BackfillRequest,
  type ChangeSourceUpstream,
} from '../protocol/current/upstream.ts';
import {initSyncSchema} from './sync-schema.ts';

/**
 * Initializes a Custom change source before streaming changes from the
 * corresponding logical replication stream.
 */
export async function initializeCustomChangeSource(
  lc: LogContext,
  upstreamURI: string,
  shard: ShardConfig,
  replicaDbFile: string,
): Promise<{subscriptionState: SubscriptionState; changeSource: ChangeSource}> {
  await initSyncSchema(
    lc,
    `replica-${shard.appID}-${shard.shardNum}`,
    shard,
    replicaDbFile,
    upstreamURI,
  );

  const replica = new Database(lc, replicaDbFile);
  const subscriptionState = getSubscriptionState(new StatementRunner(replica));
  replica.close();

  if (shard.publications.length) {
    // Verify that the publications match what has been synced.
    const requested = [...shard.publications].sort();
    const replicated = subscriptionState.publications.sort();
    if (!deepEqual(requested, replicated)) {
      throw new Error(
        `Invalid ShardConfig. Requested publications [${requested}] do not match synced publications: [${replicated}]`,
      );
    }
  }

  const changeSource = new CustomChangeSource(
    lc,
    upstreamURI,
    shard,
    subscriptionState,
  );

  return {subscriptionState, changeSource};
}

class CustomChangeSource implements ChangeSource {
  readonly #lc: LogContext;
  readonly #upstreamUri: string;
  readonly #shard: ShardID;
  readonly #replicationConfig: ReplicationConfig;

  constructor(
    lc: LogContext,
    upstreamUri: string,
    shard: ShardID,
    replicationConfig: ReplicationConfig,
  ) {
    this.#lc = lc.withContext('component', 'change-source');
    this.#upstreamUri = upstreamUri;
    this.#shard = shard;
    this.#replicationConfig = replicationConfig;
  }

  initialSync(): ChangeStream {
    return this.#startStream();
  }

  startStream(
    clientWatermark: string,
    backfillRequests: BackfillRequest[] = [],
  ): Promise<ChangeStream> {
    if (backfillRequests?.length) {
      throw new Error(
        'backfill is yet not supported for custom change sources',
      );
    }
    return Promise.resolve(this.#startStream(clientWatermark));
  }

  #startStream(clientWatermark?: string): ChangeStream {
    const {publications, replicaVersion} = this.#replicationConfig;
    const {appID, shardNum} = this.#shard;
    const url = new URL(this.#upstreamUri);
    url.searchParams.set('appID', appID);
    url.searchParams.set('shardNum', String(shardNum));
    for (const pub of publications) {
      url.searchParams.append('publications', pub);
    }
    if (clientWatermark) {
      assert(
        replicaVersion.length,
        'replicaVersion is required when clientWatermark is set',
      );
      url.searchParams.set('lastWatermark', clientWatermark);
      url.searchParams.set('replicaVersion', replicaVersion);
    }

    const ws = new WebSocket(url);
    const {instream, outstream} = stream(
      this.#lc,
      ws,
      changeStreamMessageSchema,
      // Upstream acks coalesce. If upstream exhibits back-pressure,
      // only the last ACK is kept / buffered.
      {coalesce: (curr: ChangeSourceUpstream) => curr},
    );
    return {changes: instream, acks: outstream};
  }
}

/**
 * Initial sync for a custom change source makes a request to the
 * change source endpoint with no `replicaVersion` or `lastWatermark`.
 * The initial transaction returned by the endpoint is treated as
 * the initial sync, and the commit watermark of that transaction
 * becomes the `replicaVersion` of the initialized replica.
 *
 * Note that this is equivalent to how the LSN of the Postgres WAL
 * at initial sync time is the `replicaVersion` (and starting
 * version for all initially-synced rows).
 */
export async function initialSync(
  lc: LogContext,
  shard: ShardConfig,
  tx: Database,
  upstreamURI: string,
) {
  const {appID: id, publications} = shard;
  const changeSource = new CustomChangeSource(lc, upstreamURI, shard, {
    replicaVersion: '', // ignored for initialSync()
    publications,
  });
  const {changes} = changeSource.initialSync();

  createReplicationStateTables(tx);
  const processor = new ChangeProcessor(
    new StatementRunner(tx),
    'initial-sync',
    (_, err) => {
      throw err;
    },
  );

  const statusPublisher = new ReplicationStatusPublisher(tx);
  try {
    let num = 0;
    for await (const change of changes) {
      const [tag] = change;
      switch (tag) {
        case 'begin': {
          const {commitWatermark} = change[2];
          lc.info?.(
            `initial sync of shard ${id} at replicaVersion ${commitWatermark}`,
          );
          statusPublisher.publish(
            lc,
            'Initializing',
            `Copying upstream tables at version ${commitWatermark}`,
            5000,
          );
          initReplicationState(
            tx,
            [...publications].sort(),
            commitWatermark,
            false,
          );
          processor.processMessage(lc, change);
          break;
        }
        case 'data':
          processor.processMessage(lc, change);
          if (++num % 1000 === 0) {
            lc.debug?.(`processed ${num} changes`);
          }
          break;
        case 'commit':
          processor.processMessage(lc, change);
          validateInitiallySyncedData(lc, tx, shard);
          lc.info?.(`finished initial-sync of ${num} changes`);
          return;

        case 'status':
          break; // Ignored
        // @ts-expect-error: falls through if the tag is not 'reset-required
        case 'control': {
          const {tag, message} = change[1];
          if (tag === 'reset-required') {
            throw new AutoResetSignal(
              message ?? 'auto-reset signaled by change source',
            );
          }
        }
        // falls through
        case 'rollback':
          throw new Error(
            `unexpected message during initial-sync: ${stringify(change)}`,
          );
        default:
          unreachable(change);
      }
    }
    throw new Error(
      `change source ${upstreamURI} closed before initial-sync completed`,
    );
  } catch (e) {
    await statusPublisher.publishAndThrowError(lc, 'Initializing', e);
  } finally {
    statusPublisher.stop();
  }
}

// Verify that the upstream tables expected by the sync logic
// have been properly initialized.
function getRequiredTables({
  appID,
  shardNum,
}: ShardID): Record<string, Record<string, SchemaValue>> {
  return {
    [`${appID}_${shardNum}.clients`]: {
      clientGroupID: {type: 'string'},
      clientID: {type: 'string'},
      lastMutationID: {type: 'number'},
      userID: {type: 'string'},
    },
    [`${appID}_${shardNum}.mutations`]: {
      clientGroupID: {type: 'string'},
      clientID: {type: 'string'},
      mutationID: {type: 'number'},
      mutation: {type: 'json'},
    },
    [`${appID}.permissions`]: {
      permissions: {type: 'json'},
      hash: {type: 'string'},
    },
  };
}

function validateInitiallySyncedData(
  lc: LogContext,
  db: Database,
  shard: ShardID,
) {
  const tables = computeZqlSpecs(lc, db);
  const required = getRequiredTables(shard);
  for (const [name, columns] of Object.entries(required)) {
    const table = tables.get(name)?.zqlSpec;
    if (!table) {
      throw new Error(
        `Upstream is missing the "${name}" table. (Found ${[
          ...tables.keys(),
        ]})` +
          `Please ensure that each table has a unique index over one ` +
          `or more non-null columns.`,
      );
    }
    for (const [col, {type}] of Object.entries(columns)) {
      const found = table[col];
      if (!found) {
        throw new Error(
          `Upstream "${table}" table is missing the "${col}" column`,
        );
      }
      if (found.type !== type) {
        throw new Error(
          `Upstream "${table}.${col}" column is a ${found.type} type but must be a ${type} type.`,
        );
      }
    }
  }
}
