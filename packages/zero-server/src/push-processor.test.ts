import {describe, expect, test, vi} from 'vitest';
import * as v from '../../shared/src/valita.ts';
import {type PushBody} from '../../zero-protocol/src/push.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {CustomMutatorDefs} from './custom.ts';
import {PushProcessor} from './push-processor.ts';
import {PostgresJSConnection} from './adapters/postgresjs.ts';
import {ZQLDatabase} from './zql-database.ts';
import type postgres from 'postgres';
describe('PushProcessor', () => {
  const body = {
    pushVersion: 2,
    requestID: 'test_request_id',
    timestamp: 1234567890,
    schemaVersion: 1,
    clientGroupID: 'test_client_group',
    mutations: [],
  } satisfies PushBody;

  const mockSchema = {
    tables: {},
    relationships: {},
  } satisfies Schema;

  const mockPgClient = {} as postgres.Sql;

  // Mock mutators
  const mockMutators = {} as CustomMutatorDefs<unknown>;

  test('should accept Record<string, string> as params', async () => {
    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(mockPgClient), mockSchema),
    );

    const params: Record<string, string> = {
      schema: 'test_schema',
      appID: 'test_client_group',
    };

    const spy = vi.spyOn(v, 'parse');
    await processor.process(mockMutators, params, body);

    expect(spy.mock.calls[1][0]).toMatchInlineSnapshot(`
      {
        "appID": "test_client_group",
        "schema": "test_schema",
      }
    `);
  });

  test('should accept URLSearchParams as params', async () => {
    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(mockPgClient), mockSchema),
    );

    const urlParams = new URLSearchParams();
    urlParams.append('schema', 'test_schema');
    urlParams.append('appID', 'test_client_group');

    const spy = vi.spyOn(v, 'parse');
    await processor.process(mockMutators, urlParams, body);

    expect(spy.mock.calls[1][0]).toMatchInlineSnapshot(`
      {
        "appID": "test_client_group",
        "schema": "test_schema",
      }
    `);
  });

  test('should accept Request as a param', async () => {
    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(mockPgClient), mockSchema),
    );

    const req = new Request(
      'https://example.com?schema=test_schema&appID=test_client_group',
      {
        method: 'POST',
        body: JSON.stringify(body),
      },
    );

    const spy = vi.spyOn(v, 'parse');
    await processor.process(mockMutators, req);

    expect(spy.mock.calls[1][0]).toMatchInlineSnapshot(`
      {
        "appID": "test_client_group",
        "schema": "test_schema",
      }
    `);
  });

  test('invalid params throw', async () => {
    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(mockPgClient), mockSchema),
    );

    const invalidParams: Record<string, string> = {
      // Missing schema and clientGroupID
    };

    await expect(
      processor.process(mockMutators, invalidParams, body),
    ).rejects.toThrow('Missing property schema');
  });
});
