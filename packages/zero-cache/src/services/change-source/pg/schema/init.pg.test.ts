import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect} from 'vitest';
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
import {upstreamSchema, type ShardConfig} from '../../../../types/shards.ts';
import {id} from '../../../../types/sql.ts';
import {
  CURRENT_SCHEMA_VERSION,
  ensureShardSchema,
  updateShardSchema,
} from './init.ts';
import {createReplica, initReplica, metadataPublicationName} from './shard.ts';

const APP_ID = 'zappz';
const SHARD_NUM = 23;
const PRE_DEFINER_SCHEMA_VERSION = 23;
const TEST_REPLICA_VERSION = 'ddl-trigger-security-definer-test';
const DDL_END_FUNCTION = 'emit_ddl_end';
const DDL_START_FUNCTION = 'emit_ddl_start';
const DDL_EVENT_TRIGGER_FUNCTIONS = [
  DDL_END_FUNCTION,
  DDL_START_FUNCTION,
] as const;
const LOCKED_SEARCH_PATH_CONFIG = 'search_path=pg_catalog, pg_temp';

// Update as necessary.
const CURRENT_SCHEMA_VERSIONS = {
  dataVersion: CURRENT_SCHEMA_VERSION,
  schemaVersion: CURRENT_SCHEMA_VERSION,
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
            id: /\d{10,}/,
            rank: expect.any(BigInt),
            slot: `${APP_ID}_${SHARD_NUM}_1234`,
            version: '2dhf29ef',
            initialSchema: {tables: [], indexes: []},
            initialSyncContext: {foo: 'bar'},
            subscriberContext: null,
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
            id: /\d{10,}/,
            rank: expect.any(BigInt),
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
            id: /[a-z0-9]{10,}/, // Random ID is backfilled
            rank: expect.any(BigInt),
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
          await createReplica(
            upstream,
            {appID: APP_ID, shardNum: SHARD_NUM},
            '12345',
            c.newReplica[0],
            c.newReplica[1],
          );
          await initReplica(
            upstream,
            {appID: APP_ID, shardNum: SHARD_NUM},
            '12345',
            {tables: [], indexes: []},
            {foo: 'bar'},
          );
        }
      }

      await expectTablesToMatch(upstream, c.upstreamPostState);
    });
  }

  test('upgrades and locks down existing DDL hook security definers', async () => {
    const shard: ShardConfig = {
      appID: APP_ID,
      shardNum: SHARD_NUM,
      publications: [],
    };
    const schema = upstreamSchema(shard);

    await ensureShardSchema(lc, upstream, shard);
    await upstream.unsafe(/*sql*/ `
      ALTER FUNCTION ${id(schema)}.${id(DDL_START_FUNCTION)}() SECURITY INVOKER;
      ALTER FUNCTION ${id(schema)}.${id(DDL_START_FUNCTION)}() RESET ALL;
      ALTER FUNCTION ${id(schema)}.${id(DDL_END_FUNCTION)}() SECURITY INVOKER;
      ALTER FUNCTION ${id(schema)}.${id(DDL_END_FUNCTION)}() RESET ALL;
      GRANT EXECUTE ON FUNCTION ${id(schema)}.${id(DDL_START_FUNCTION)}() TO PUBLIC;
      GRANT EXECUTE ON FUNCTION ${id(schema)}.${id(DDL_END_FUNCTION)}() TO PUBLIC;
      GRANT CREATE ON SCHEMA ${id(schema)} TO PUBLIC;
    `);
    await upstream`
      UPDATE ${upstream(schema)}."versionHistory"
      SET "dataVersion" = ${PRE_DEFINER_SCHEMA_VERSION},
          "schemaVersion" = ${PRE_DEFINER_SCHEMA_VERSION}
    `;

    const getEventTriggerFunctions = () =>
      upstream<
        {
          functionName: string;
          owner: string;
          securityDefiner: boolean;
          config: string | null;
          publicExecute: boolean;
          publicSchemaCreate: boolean;
        }[]
      >`
        SELECT
          p.proname AS "functionName",
          pg_catalog.pg_get_userbyid(p.proowner) AS owner,
          p.prosecdef AS "securityDefiner",
          array_to_string(p.proconfig, ',') AS config,
          EXISTS (
            SELECT FROM pg_catalog.aclexplode(
              COALESCE(
                p.proacl,
                pg_catalog.acldefault('f', p.proowner)
              )
            ) AS acl
            WHERE acl.grantee = 0
              AND acl.privilege_type = 'EXECUTE'
          ) AS "publicExecute",
          EXISTS (
            SELECT FROM pg_catalog.aclexplode(
              COALESCE(
                n.nspacl,
                pg_catalog.acldefault('n', n.nspowner)
              )
            ) AS acl
            WHERE acl.grantee = 0
              AND acl.privilege_type = 'CREATE'
          ) AS "publicSchemaCreate"
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = ${schema}
          AND p.proname IN (${DDL_END_FUNCTION}, ${DDL_START_FUNCTION})
        ORDER BY p.proname
      `;

    const expectedEventTriggerFunctions = (
      owner: string,
      securityDefiner: boolean,
      config: string | null,
      publicExecute: boolean,
      publicSchemaCreate: boolean,
    ) =>
      DDL_EVENT_TRIGGER_FUNCTIONS.map(functionName => ({
        functionName,
        owner,
        securityDefiner,
        config,
        publicExecute,
        publicSchemaCreate,
      }));

    const [{owner}] = await getEventTriggerFunctions();
    expect(await getEventTriggerFunctions()).toEqual(
      expectedEventTriggerFunctions(owner, false, null, true, true),
    );

    await updateShardSchema(lc, upstream, shard, TEST_REPLICA_VERSION);

    expect(await getEventTriggerFunctions()).toEqual(
      expectedEventTriggerFunctions(
        owner,
        true,
        LOCKED_SEARCH_PATH_CONFIG,
        false,
        false,
      ),
    );
    await expectTablesToMatch(upstream, {
      [`${schema}.versionHistory`]: [CURRENT_SCHEMA_VERSIONS],
    });
  });
});
