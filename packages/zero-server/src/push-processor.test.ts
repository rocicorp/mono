import type postgres from 'postgres';
import {assert, describe, expect, test, vi} from 'vitest';
import * as v from '../../shared/src/valita.ts';
import {ErrorKind} from '../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../zero-protocol/src/error-reason.ts';
import {type PushBody} from '../../zero-protocol/src/push.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import {PostgresJSConnection} from './adapters/postgresjs.ts';
import type {CustomMutatorDefs} from './custom.ts';
import {PushProcessor} from './push-processor.ts';
import {ZQLDatabase} from './zql-database.ts';

describe('PushProcessor', () => {
  const body = {
    pushVersion: 1,
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

  test('invalid params return parse error', async () => {
    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(mockPgClient), mockSchema),
    );

    const invalidParams: Record<string, string> = {
      // Missing schema and appID
    };

    const result = await processor.process(mockMutators, invalidParams, body);

    assert('kind' in result, 'expected push failed response');

    expect(result).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Parse,
      message: expect.stringContaining('Missing property schema'),
    });
  });
});
