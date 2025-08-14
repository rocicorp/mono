import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {replicationStatusEvent} from './replication-status.ts';

describe('replicator/replication-status', () => {
  let lc: LogContext;
  let replica: Database;

  beforeEach(() => {
    lc = createSilentLogContext();
    replica = new Database(createSilentLogContext(), ':memory:');
  });

  test('initializing', () => {
    replica.exec(/*sql*/ `
    CREATE TABLE foo(a "int|NOT_NULL", b text);
    CREATE UNIQUE INDEX foo_pk ON foo(a DESC);

    CREATE TABLE bar(c "varchar|NOT_NULL", d "bool|NOT_NULL");
    CREATE UNIQUE INDEX bar_pk ON bar(c DESC, d ASC);
    `);

    expect(
      replicationStatusEvent(
        lc,
        replica,
        'Initializing',
        'OK',
        'my description',
        new Date(Date.UTC(2025, 9, 14, 1, 2, 3)),
      ),
    ).toMatchInlineSnapshot(`
      {
        "component": "replication",
        "description": "my description",
        "indexes": [
          {
            "columns": [
              {
                "column": "c",
                "dir": "DESC",
              },
              {
                "column": "d",
                "dir": "ASC",
              },
            ],
            "table": "bar",
            "unique": true,
          },
          {
            "columns": [
              {
                "column": "a",
                "dir": "DESC",
              },
            ],
            "table": "foo",
            "unique": true,
          },
        ],
        "replicaSize": 20480,
        "stage": "Initializing",
        "status": "OK",
        "tables": [
          {
            "columns": [
              {
                "clientType": "string",
                "column": "c",
                "upstreamType": "varchar",
              },
              {
                "clientType": "boolean",
                "column": "d",
                "upstreamType": "bool",
              },
            ],
            "table": "bar",
          },
          {
            "columns": [
              {
                "clientType": "number",
                "column": "a",
                "upstreamType": "int",
              },
              {
                "clientType": "string",
                "column": "b",
                "upstreamType": "TEXT",
              },
            ],
            "table": "foo",
          },
        ],
        "time": "2025-10-14T01:02:03.000Z",
        "type": "zero/events/status/replication/v1",
      }
    `);
  });

  test('replicating', () => {
    replica.exec(/*sql*/ `
    CREATE TABLE foo(a "int|NOT_NULL", b text);
    CREATE UNIQUE INDEX foo_pk ON foo(a DESC);

    CREATE TABLE bar(c "varchar|NOT_NULL", d "bool|NOT_NULL");
    CREATE UNIQUE INDEX bar_pk ON bar(c DESC, d ASC);
    `);

    expect(
      replicationStatusEvent(
        lc,
        replica,
        'Replicating',
        'OK',
        undefined,
        new Date(Date.UTC(2025, 9, 14, 1, 2, 3)),
      ),
    ).toMatchInlineSnapshot(`
      {
        "component": "replication",
        "description": undefined,
        "indexes": [
          {
            "columns": [
              {
                "column": "c",
                "dir": "DESC",
              },
              {
                "column": "d",
                "dir": "ASC",
              },
            ],
            "table": "bar",
            "unique": true,
          },
          {
            "columns": [
              {
                "column": "a",
                "dir": "DESC",
              },
            ],
            "table": "foo",
            "unique": true,
          },
        ],
        "replicaSize": 20480,
        "stage": "Replicating",
        "status": "OK",
        "tables": [
          {
            "columns": [
              {
                "clientType": "string",
                "column": "c",
                "upstreamType": "varchar",
              },
              {
                "clientType": "boolean",
                "column": "d",
                "upstreamType": "bool",
              },
            ],
            "table": "bar",
          },
          {
            "columns": [
              {
                "clientType": "number",
                "column": "a",
                "upstreamType": "int",
              },
              {
                "clientType": "string",
                "column": "b",
                "upstreamType": "TEXT",
              },
            ],
            "table": "foo",
          },
        ],
        "time": "2025-10-14T01:02:03.000Z",
        "type": "zero/events/status/replication/v1",
      }
    `);
  });

  test('non-synced column', () => {
    replica.exec(/*sql*/ `
    CREATE TABLE foo(a "int|NOT_NULL", not_synced bytea);
    CREATE UNIQUE INDEX foo_pk ON foo(a DESC);
    `);

    expect(
      replicationStatusEvent(
        lc,
        replica,
        'Initializing',
        'OK',
        'another description',
        new Date(Date.UTC(2025, 9, 14, 1, 2, 3)),
      ),
    ).toMatchInlineSnapshot(`
      {
        "component": "replication",
        "description": "another description",
        "indexes": [
          {
            "columns": [
              {
                "column": "a",
                "dir": "DESC",
              },
            ],
            "table": "foo",
            "unique": true,
          },
        ],
        "replicaSize": 12288,
        "stage": "Initializing",
        "status": "OK",
        "tables": [
          {
            "columns": [
              {
                "clientType": "number",
                "column": "a",
                "upstreamType": "int",
              },
              {
                "clientType": null,
                "column": "not_synced",
                "upstreamType": "bytea",
              },
            ],
            "table": "foo",
          },
        ],
        "time": "2025-10-14T01:02:03.000Z",
        "type": "zero/events/status/replication/v1",
      }
    `);
  });
});
