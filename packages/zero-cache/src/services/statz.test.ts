import fastify, {type FastifyInstance} from 'fastify';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {handleStatzRequest} from './statz.ts';

const {dbCloseSpy, dbPragmaSpy, statSyncSpy} = vi.hoisted(() => ({
  dbCloseSpy: vi.fn(),
  dbPragmaSpy: vi.fn(() => [{value: 'ok'}]),
  statSyncSpy: vi.fn(() => ({size: 1})),
}));

vi.mock('fs', () => ({
  default: {statSync: statSyncSpy},
  statSync: statSyncSpy,
}));

vi.mock('../../../zqlite/src/db.ts', () => ({
  Database: class {
    constructor(_lc: unknown, _path: string) {}
    pragma = dbPragmaSpy;
    close() {
      dbCloseSpy();
    }
  },
}));

vi.mock('../db/statements.ts', () => ({
  StatementRunner: class {
    constructor(_db: unknown) {}
  },
}));

vi.mock('./replicator/schema/replication-state.ts', () => ({
  getReplicationState: () => ({stateVersion: '00'}),
}));

describe('statz endpoint', () => {
  const lc = createSilentLogContext();
  const config = {
    adminPassword: 'secret',
    replica: {file: '/tmp/replica.db'},
    log: {level: 'error'},
  } as unknown as NormalizedZeroConfig;

  const authHeader = {
    authorization: `Basic ${Buffer.from('user:secret').toString('base64')}`,
  };

  let app: FastifyInstance;

  beforeEach(async () => {
    dbCloseSpy.mockClear();
    dbPragmaSpy.mockClear();
    statSyncSpy.mockClear();
    app = fastify();
    app.get('/statz', (req, res) => handleStatzRequest(lc, config, req, res));
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
  });

  test('returns json stats for os group', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/statz?group=os&format=json',
      headers: authHeader,
    });

    expect(res.statusCode).toBe(200);
    expect(res.headers['content-type']).toContain('application/json');
    const body = JSON.parse(res.body) as {os: Record<string, unknown>};
    expect(body).toHaveProperty('os');
    expect(body.os).toHaveProperty('platform');
  });

  test('returns text stats for os group', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/statz?group=os',
      headers: authHeader,
    });

    expect(res.statusCode).toBe(200);
    expect(res.headers['content-type']).toContain('text/plain');
    expect(res.body).toContain('=== os ===');
    expect(res.body).toContain('platform');
  });

  test('closes replica databases when requested', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/statz?group=replica,replication&format=json',
      headers: authHeader,
    });

    expect(res.statusCode).toBe(200);
    expect(dbCloseSpy).toHaveBeenCalledTimes(2);
  });

  test('rejects requests with bad auth', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/statz?group=os&format=json',
      headers: {
        authorization: `Basic ${Buffer.from('user:wrong').toString('base64')}`,
      },
    });

    expect(res.statusCode).toBe(401);
    expect(res.headers['www-authenticate']).toContain('Basic');
  });
});
