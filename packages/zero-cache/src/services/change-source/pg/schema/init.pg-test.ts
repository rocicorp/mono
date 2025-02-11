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
        [`zero_${SHARD_ID}.shardConfig`]: [
          {
            lock: true,
            publications: [
              '_zero_metadata_shard_schema_test_id',
              '_zero_public_shard_schema_test_id',
            ],
            ddlDetection: true,
            initialSchema: null,
          },
        ],
        [`zero_${SHARD_ID}.clients`]: [],
        [`zero_${SHARD_ID}.versionHistory`]: [CURRENT_SCHEMA_VERSIONS],
        ['zero.schemaVersions']: [
          {minSupportedVersion: 1, maxSupportedVersion: 1},
        ],
      },
    },
    {
      name: 'db with table and publication',
      upstreamSetup: `
        CREATE TABLE foo(id TEXT PRIMARY KEY);
        CREATE PUBLICATION zero_foo FOR TABLE foo;
      `,
      requestedPublications: ['zero_foo'],
      upstreamPostState: {
        [`zero_${SHARD_ID}.shardConfig`]: [
          {
            lock: true,
            publications: ['_zero_metadata_shard_schema_test_id', 'zero_foo'],
            ddlDetection: true,
            initialSchema: null,
          },
        ],
        [`zero_${SHARD_ID}.clients`]: [],
        [`zero_${SHARD_ID}.versionHistory`]: [CURRENT_SCHEMA_VERSIONS],
        ['zero.schemaVersions']: [
          {minSupportedVersion: 1, maxSupportedVersion: 1},
        ],
      },
    },
    {
      name: 'db with existing schemaVersions',
      upstreamSetup: `
          CREATE SCHEMA IF NOT EXISTS zero;
          CREATE TABLE zero."schemaVersions" 
            ("lock" BOOL PRIMARY KEY, "minSupportedVersion" INT4, "maxSupportedVersion" INT4);
          INSERT INTO zero."schemaVersions" 
            ("lock", "minSupportedVersion", "maxSupportedVersion") VALUES (true, 2, 3);
        `,
      upstreamPostState: {
        [`zero_${SHARD_ID}.shardConfig`]: [
          {
            lock: true,
            publications: [
              '_zero_metadata_shard_schema_test_id',
              '_zero_public_shard_schema_test_id',
            ],
            ddlDetection: true,
            initialSchema: null,
          },
        ],
        [`zero_${SHARD_ID}.clients`]: [],
        [`zero_${SHARD_ID}.versionHistory`]: [CURRENT_SCHEMA_VERSIONS],
        ['zero.schemaVersions']: [
          {minSupportedVersion: 2, maxSupportedVersion: 3},
        ],
      },
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      await initDB(upstream, c.upstreamSetup, c.upstreamPreState);

      if (c.existingVersionHistory) {
        const schema = `zero_${SHARD_ID}`;
        await createVersionHistoryTable(upstream, schema);
        await upstream`INSERT INTO ${upstream(schema)}."versionHistory"
          ${upstream(c.existingVersionHistory)}`;
        await updateShardSchema(lc, upstream, {
          id: SHARD_ID,
          publications: c.requestedPublications ?? [],
        });
      } else {
        await initShardSchema(lc, upstream, {
          id: SHARD_ID,
          publications: c.requestedPublications ?? [],
        });
      }

      await expectTablesToMatch(upstream, c.upstreamPostState);
    });
  }
});
