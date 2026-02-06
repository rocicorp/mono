import {beforeEach, describe, expect, test, vi} from 'vitest';
import fastify from 'fastify';
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

  beforeEach(() => {
    dbCloseSpy.mockClear();
    dbPragmaSpy.mockClear();
    statSyncSpy.mockClear();
  });

  async function buildApp() {
    const app = fastify();
    app.get('/statz', (req, res) => handleStatzRequest(lc, config, req, res));
    await app.ready();
    return app;
  }

  test('returns json stats for os group', async () => {
    const app = await buildApp();
    try {
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
    } finally {
      await app.close();
    }
  });

  test('returns text stats for os group', async () => {
    const app = await buildApp();
    try {
      const res = await app.inject({
        method: 'GET',
        url: '/statz?group=os',
        headers: authHeader,
      });

      expect(res.statusCode).toBe(200);
      expect(res.headers['content-type']).toContain('text/plain');
      expect(res.body).toContain('=== os ===');
      expect(res.body).toContain('platform');
    } finally {
      await app.close();
    }
  });

  test('closes replica databases when requested', async () => {
    const app = await buildApp();
    try {
      const res = await app.inject({
        method: 'GET',
        url: '/statz?group=replica,replication&format=json',
        headers: authHeader,
      });

      expect(res.statusCode).toBe(200);
      expect(dbCloseSpy).toHaveBeenCalledTimes(2);
    } finally {
      await app.close();
    }
  });
});
