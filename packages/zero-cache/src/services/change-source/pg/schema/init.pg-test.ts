import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, test} from 'vitest';
import {createSilentLogContext} from '../../../../../../shared/src/logging-test-utils.ts';
import {
  createVersionHistoryTable,
  type VersionHistory,
} from '../../../../db/migration.ts';
import {expectTablesToMatch, initDB, testDBs} from '../../../../test/db.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import {initShardSchema, updateShardSchema} from './init.ts';

const APP_ID = 'zappz';
const SHARD_NUM = 23;

// Update as necessary.
const CURRENT_SCHEMA_VERSIONS = {
  dataVersion: 7,
  schemaVersion: 7,
  minSafeVersion: 1,
  lock: 'v',
} as const;

describe('change-streamer/pg/schema/init', () => {
  let lc: LogContext;
  let upstream: PostgresDB;

  beforeEach(async () => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('shard_schema_migration_upstream');
  });

  afterEach(async () => {
    await testDBs.drop(upstream);
  });

  type Case = {
    name: string;
    upstreamSetup?: string;
    existingVersionHistory?: VersionHistory;
    requestedPublications?: string[];
    upstreamPreState?: Record<string, object[]>;
    upstreamPostState?: Record<string, object[]>;
  };

  const cases: Case[] = [
    {
      name: 'initial db',
      upstreamPostState: {
        [`${APP_ID}_${SHARD_NUM}.shardConfig`]: [
          {
            lock: true,
            publications: [`_${APP_ID}_metadata_23`, `_${APP_ID}_public_23`],
            ddlDetection: true,
            initialSchema: null,
          },
        ],
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        [`${APP_ID}_${SHARD_NUM}.versionHistory`]: [CURRENT_SCHEMA_VERSIONS],
        [`${APP_ID}.schemaVersions`]: [
          {minSupportedVersion: 1, maxSupportedVersion: 1},
        ],
      },
    },
    {
      name: 'db with table and publication',
      upstreamSetup: `
        CREATE TABLE foo(id TEXT PRIMARY KEY);
        CREATE PUBLICATION ${APP_ID}_foo FOR TABLE foo;
      `,
      requestedPublications: [`${APP_ID}_foo`],
      upstreamPostState: {
        [`${APP_ID}_${SHARD_NUM}.shardConfig`]: [
          {
            lock: true,
            publications: [`_${APP_ID}_metadata_23`, `${APP_ID}_foo`],
            ddlDetection: true,
            initialSchema: null,
          },
        ],
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        [`${APP_ID}_${SHARD_NUM}.versionHistory`]: [CURRENT_SCHEMA_VERSIONS],
        [`${APP_ID}.schemaVersions`]: [
          {minSupportedVersion: 1, maxSupportedVersion: 1},
        ],
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
            initialSchema: null,
          },
        ],
        [`${APP_ID}_${SHARD_NUM}.clients`]: [],
        [`${APP_ID}_${SHARD_NUM}.versionHistory`]: [CURRENT_SCHEMA_VERSIONS],
        [`${APP_ID}.schemaVersions`]: [
          {minSupportedVersion: 2, maxSupportedVersion: 3},
        ],
      },
    },
    {
      name: 'v5 to v7',
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
            null
          );
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
            initialSchema: null,
            replicaVersion: '123',
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
            publications: c.requestedPublications ?? [
              `_${APP_ID}_metadata_23`,
              `_${APP_ID}_public_23`,
            ],
          },
          '123',
        );
      } else {
        await initShardSchema(lc, upstream, {
          appID: APP_ID,
          shardNum: SHARD_NUM,
          publications: c.requestedPublications ?? [],
        });
      }

      await expectTablesToMatch(upstream, c.upstreamPostState);
    });
  }
});
