import {LogContext} from '@rocicorp/logger';
import {WebSocket} from 'ws';
import {assert, unreachable} from '../../../../../shared/src/asserts.ts';
import {deepEqual} from '../../../../../shared/src/json.ts';
import type {SchemaValue} from '../../../../../zero-schema/src/table-schema.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {computeZqlSpecs} from '../../../db/lite-tables.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {stringify} from '../../../types/bigint-json.ts';
import {stream} from '../../../types/streams.ts';
import type {
  ChangeSource,
  ChangeStream,
} from '../../change-streamer/change-streamer-service.ts';
import {type ReplicationConfig} from '../../change-streamer/schema/tables.ts';
import {ChangeProcessor} from '../../replicator/change-processor.ts';
import {initChangeLog} from '../../replicator/schema/change-log.ts';
import {
  getSubscriptionState,
  initReplicationState,
} from '../../replicator/schema/replication-state.ts';
import type {ShardConfig} from '../pg/shard-config.ts';
import {changeStreamMessageSchema} from '../protocol/current/downstream.ts';
import {type ChangeSourceUpstream} from '../protocol/current/upstream.ts';
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
): Promise<{replicationConfig: ReplicationConfig; changeSource: ChangeSource}> {
  await initSyncSchema(
    lc,
    `replica-${shard.id}`,
    shard,
    replicaDbFile,
    upstreamURI,
  );

  const replica = new Database(lc, replicaDbFile);
  const replicationConfig = getSubscriptionState(new StatementRunner(replica));
  replica.close();

  if (shard.publications.length) {
    // Verify that the publications match what has been synced.
    const requested = [...shard.publications].sort();
    const replicated = replicationConfig.publications.sort();
    if (!deepEqual(requested, replicated)) {
      throw new Error(
        `Invalid ShardConfig. Requested publications [${requested}] do not match synced publications: [${replicated}]`,
      );
    }
  }

  const changeSource = new CustomChangeSource(
    lc,
    upstreamURI,
    shard.id,
    replicationConfig,
  );

  return {replicationConfig, changeSource};
}

class CustomChangeSource implements ChangeSource {
  readonly #lc: LogContext;
  readonly #upstreamUri: string;
  readonly #shardID: string;
  readonly #replicationConfig: ReplicationConfig;

  constructor(
    lc: LogContext,
    upstreamUri: string,
    shardID: string,
    replicationConfig: ReplicationConfig,
  ) {
    this.#lc = lc.withContext('component', 'change-source');
    this.#upstreamUri = upstreamUri;
    this.#shardID = shardID;
    this.#replicationConfig = replicationConfig;
  }

  initialSync(): ChangeStream {
    return this.#startStream();
  }

  startStream(clientWatermark: string): Promise<ChangeStream> {
    return Promise.resolve(this.#startStream(clientWatermark));
  }

  #startStream(clientWatermark?: string): ChangeStream {
    const {publications, replicaVersion} = this.#replicationConfig;
    const url = new URL(this.#upstreamUri);
    url.searchParams.set('shardID', this.#shardID);
    for (const pub of publications) {
      url.searchParams.append('shardPublications', pub);
    }
    if (clientWatermark) {
      assert(replicaVersion.length);
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
  const {id, publications} = shard;
  const changeSource = new CustomChangeSource(lc, upstreamURI, id, {
    replicaVersion: '', // ignored for initialSync()
    publications,
  });
  const {changes} = changeSource.initialSync();

  const processor = new ChangeProcessor(
    new StatementRunner(tx),
    'INITIAL-SYNC',
    (_, err) => {
      throw err;
    },
  );

  let num = 0;
  for await (const change of changes) {
    const [tag] = change;
    switch (tag) {
      case 'begin': {
        const {commitWatermark} = change[2];
        lc.info?.(
          `initial sync of shard ${id} at replicaVersion ${commitWatermark}`,
        );
        initReplicationState(tx, [...publications].sort(), commitWatermark);
        initChangeLog(tx);
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
        validateInitiallySyncedData(lc, tx, id);
        lc.info?.(`finished initial-sync of ${num} changes`);
        return;

      case 'status':
        break; // Ignored
      case 'control':
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
}

// Verify that the upstream tables expected by the sync logic
// have been properly initialized.
function getRequiredTables(
  shardID: string,
): Record<string, Record<string, SchemaValue>> {
  return {
    [`zero_${shardID}.clients`]: {
      clientGroupID: {type: 'string'},
      clientID: {type: 'string'},
      lastMutationID: {type: 'number'},
      userID: {type: 'string'},
    },
    [`zero.permissions`]: {
      permissions: {type: 'json'},
      hash: {type: 'string'},
    },
    [`zero.schemaVersions`]: {
      minSupportedVersion: {type: 'number'},
      maxSupportedVersion: {type: 'number'},
    },
  };
}

function validateInitiallySyncedData(
  lc: LogContext,
  db: Database,
  shardID: string,
) {
  const tables = computeZqlSpecs(lc, db);
  const required = getRequiredTables(shardID);
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
