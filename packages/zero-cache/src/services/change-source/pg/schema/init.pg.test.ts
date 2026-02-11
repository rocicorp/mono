import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe} from 'vitest';
import {createSilentLogContext} from '../../../../../../shared/src/logging-test-utils.ts';
import {
  createVersionHistoryTable,
  type VersionHistory,
} from '../../../../db/migration.ts';
import {
  expectTablesToMatch,
  initDB,
  type PgTest,
  test,
} from '../../../../test/db.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import {id} from '../../../../types/sql.ts';
import {ensureShardSchema, updateShardSchema} from './init.ts';
import {addReplica, metadataPublicationName} from './shard.ts';

const APP_ID = 'zappz';
const SHARD_NUM = 23;

// Update as necessary.
const CURRENT_SCHEMA_VERSIONS = {
  dataVersion: 14,
  schemaVersion: 14,
  minSafeVersion: 1,
  lock: 'v',
} as const;

describe('change-streamer/pg/schema/init', () => {
  let lc: LogContext;
  let upstream: PostgresDB;

  beforeEach<PgTest>(async ({testDBs}) => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('shard_schema_migration_upstream');

    return () => testDBs.drop(upstream);
  });

  type Case = {
    name: string;
    upstreamSetup?: string;
    existingVersionHistory?: VersionHistory;
    newReplica?: [slot: string, replicaVersion: string];
    requestedPublications?: string[];
    upstreamPreState?: Record<string, object[]>;
    upstreamPostState?: Record<string, object[]>;
  };

  const cases: Case[] = [
    {
      name: 'initial db',
      newReplica: [`${APP_ID}_${SHARD_NUM}_1234`, '2dhf29ef'],
      upstreamPostState: {
        [`${APP_ID}_${SHARD_NUM}.shardConfig`]: [
          {
            lock: true,
            publications: [`_${APP_ID}_metadata_23`, `_${APP_ID}_public_23`],
            ddlDetection: true,
          },
        ],
        [`${APP_ID}_${SHARD_NUM}.replicas`]: [
          {
            slot: `${APP_ID}_${SHARD_NUM}_1234`,
            version: '2dhf29ef',
            initialSchema: {tables: [], indexes: []},
          },
        ],
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        [`${APP_ID}_${SHARD_NUM}.versionHistory`]: [CURRENT_SCHEMA_VERSIONS],
      },
    },
    {
      name: 'db with table and publication',
      upstreamSetup: `
        CREATE TABLE foo(id TEXT PRIMARY KEY);
        CREATE PUBLICATION ${APP_ID}_foo FOR TABLE foo;
      `,
      newReplica: [`${APP_ID}_${SHARD_NUM}_5678`, 's8dfh2d'],
      requestedPublications: [`${APP_ID}_foo`],
      upstreamPostState: {
        [`${APP_ID}_${SHARD_NUM}.shardConfig`]: [
          {
            lock: true,
            publications: [`_${APP_ID}_metadata_23`, `${APP_ID}_foo`],
            ddlDetection: true,
          },
        ],
        [`${APP_ID}_${SHARD_NUM}.replicas`]: [
          {
            slot: `${APP_ID}_${SHARD_NUM}_5678`,
            version: 's8dfh2d',
            initialSchema: {tables: [], indexes: []},
          },
        ],
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        [`${APP_ID}_${SHARD_NUM}.versionHistory`]: [CURRENT_SCHEMA_VERSIONS],
      },
    },
    {
      name: 'db with existing schemaVersions',
      upstreamSetup: `
          CREATE SCHEMA IF NOT EXISTS ${APP_ID};
          CREATE TABLE ${APP_ID}."schemaVersions" 
            ("lock" BOOL PRIMARY KEY, "minSupportedVersion" INT4, "maxSupportedVersion" INT4);
          INSERT INTO ${APP_ID}."schemaVersions" 
            ("lock", "minSupportedVersion", "maxSupportedVersion") VALUES (true, 2, 3);
        `,
      upstreamPostState: {
        [`${APP_ID}_${SHARD_NUM}.shardConfig`]: [
          {
            lock: true,
            publications: [`_${APP_ID}_metadata_23`, `_${APP_ID}_public_23`],
            ddlDetection: true,
          },
        ],
        [`${APP_ID}_${SHARD_NUM}.replicas`]: [],
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        [`${APP_ID}_${SHARD_NUM}.versionHistory`]: [CURRENT_SCHEMA_VERSIONS],
      },
    },
    {
      name: 'Migration from v5',
      upstreamSetup: `
        CREATE SCHEMA ${APP_ID}_${SHARD_NUM};
        CREATE TABLE ${APP_ID}_${SHARD_NUM}."shardConfig" (
          "publications"  TEXT[] NOT NULL,
          "ddlDetection"  BOOL NOT NULL,
          "initialSchema" JSON,

          -- Ensure that there is only a single row in the table.
          "lock" BOOL PRIMARY KEY DEFAULT true CHECK (lock)
        );

        INSERT INTO ${APP_ID}_${SHARD_NUM}."shardConfig" 
          ("lock", "publications", "ddlDetection", "initialSchema")
          VALUES (true, 
            ARRAY['_${APP_ID}_metadata_23', '_${APP_ID}_public_23'], 
            true,
            '{"tables":[],"indexes":[]}'
          );
        CREATE TABLE ${APP_ID}_${SHARD_NUM}."clients" 
            ("clientGroupID" TEXT PRIMARY KEY, "clientID" TEXT, "lastMutationID" INT8);

        CREATE PUBLICATION ${id(metadataPublicationName(APP_ID, SHARD_NUM))}
            FOR TABLE ${APP_ID}_${SHARD_NUM}."clients";
  `,
      existingVersionHistory: {
        schemaVersion: 5,
        dataVersion: 5,
        minSafeVersion: 1,
      },
      upstreamPostState: {
        [`${APP_ID}_${SHARD_NUM}.shardConfig`]: [
          {
            lock: true,
            publications: [`_${APP_ID}_metadata_23`, `_${APP_ID}_public_23`],
            ddlDetection: true,
          },
        ],
        [`${APP_ID}_${SHARD_NUM}.schemaVersions`]: [
          {
            lock: true,
            minSupportedVersion: 1,
            maxSupportedVersion: 1,
          },
        ],
        [`${APP_ID}_${SHARD_NUM}.replicas`]: [
          {
            slot: `${APP_ID}_${SHARD_NUM}`,
            version: '123',
            initialSchema: {tables: [], indexes: []},
          },
        ],
      },
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      await initDB(upstream, c.upstreamSetup, c.upstreamPreState);

      if (c.existingVersionHistory) {
        const schema = `${APP_ID}_${SHARD_NUM}`;
        await createVersionHistoryTable(upstream, schema);
        await upstream`INSERT INTO ${upstream(schema)}."versionHistory"
          ${upstream(c.existingVersionHistory)}`;
        await updateShardSchema(
          lc,
          upstream,
          {
            appID: APP_ID,
            shardNum: SHARD_NUM,
            publications: c.requestedPublications ?? [],
          },
          '123',
        );
      } else {
        await ensureShardSchema(lc, upstream, {
          appID: APP_ID,
          shardNum: SHARD_NUM,
          publications: c.requestedPublications ?? [],
        });
        if (c.newReplica) {
          await addReplica(
            upstream,
            {appID: APP_ID, shardNum: SHARD_NUM},
            c.newReplica[0],
            c.newReplica[1],
            {tables: [], indexes: []},
          );
        }
      }

      await expectTablesToMatch(upstream, c.upstreamPostState);
    });
  }
});
