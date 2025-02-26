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
const SHARD_ID = 'shard_schema_test_id';

// Update as necessary.
const CURRENT_SCHEMA_VERSIONS = {
  dataVersion: 4,
  schemaVersion: 4,
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
        [`${APP_ID}_${SHARD_ID}.shardConfig`]: [
          {
            lock: true,
            publications: [
              `_${APP_ID}_metadata_shard_schema_test_id`,
              `_${APP_ID}_public_shard_schema_test_id`,
            ],
            ddlDetection: true,
            initialSchema: null,
          },
        ],
        [`${APP_ID}_${SHARD_ID}.clients`]: [],
        [`${APP_ID}_${SHARD_ID}.versionHistory`]: [CURRENT_SCHEMA_VERSIONS],
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
        [`${APP_ID}_${SHARD_ID}.shardConfig`]: [
          {
            lock: true,
            publications: [
              `_${APP_ID}_metadata_shard_schema_test_id`,
              `${APP_ID}_foo`,
            ],
            ddlDetection: true,
            initialSchema: null,
          },
        ],
        [`${APP_ID}_${SHARD_ID}.clients`]: [],
        [`${APP_ID}_${SHARD_ID}.versionHistory`]: [CURRENT_SCHEMA_VERSIONS],
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
        [`${APP_ID}_${SHARD_ID}.shardConfig`]: [
          {
            lock: true,
            publications: [
              `_${APP_ID}_metadata_shard_schema_test_id`,
              `_${APP_ID}_public_shard_schema_test_id`,
            ],
            ddlDetection: true,
            initialSchema: null,
          },
        ],
        [`${APP_ID}_${SHARD_ID}.clients`]: [],
        [`${APP_ID}_${SHARD_ID}.versionHistory`]: [CURRENT_SCHEMA_VERSIONS],
        [`${APP_ID}.schemaVersions`]: [
          {minSupportedVersion: 2, maxSupportedVersion: 3},
        ],
      },
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      await initDB(upstream, c.upstreamSetup, c.upstreamPreState);

      if (c.existingVersionHistory) {
        const schema = `${APP_ID}_${SHARD_ID}`;
        await createVersionHistoryTable(upstream, schema);
        await upstream`INSERT INTO ${upstream(schema)}."versionHistory"
          ${upstream(c.existingVersionHistory)}`;
        await updateShardSchema(lc, upstream, APP_ID, {
          id: SHARD_ID,
          publications: c.requestedPublications ?? [],
        });
      } else {
        await initShardSchema(lc, upstream, APP_ID, {
          id: SHARD_ID,
          publications: c.requestedPublications ?? [],
        });
      }

      await expectTablesToMatch(upstream, c.upstreamPostState);
    });
  }
});
