import type {Kysely} from 'kysely';
import type {Client} from 'pg';
import type postgres from 'postgres';
import {describe, expect, test, vi} from 'vitest';
import {DrizzleConnection} from './drizzle.ts';
import {KyselyConnection} from './kysely.ts';
import {NodePgConnection} from './pg.ts';
import {PostgresJSConnection} from './postgresjs.ts';
import {PrismaConnection} from './prisma.ts';

// Each adapter opens its transaction through a different API, so the option is
// checked against what that API actually receives rather than against the SQL.

describe('KyselyConnection isolation level', () => {
  function fakeKysely() {
    const setIsolationLevel = vi.fn();
    const builder = {
      setIsolationLevel(level: string) {
        setIsolationLevel(level);
        return builder;
      },
      execute<T>(cb: (tx: unknown) => Promise<T>) {
        return cb({executeQuery: vi.fn().mockResolvedValue({rows: []})});
      },
    };
    const client = {transaction: () => builder} as unknown as Kysely<unknown>;
    return {client, setIsolationLevel};
  }

  test('is not set when no option is given', async () => {
    const {client, setIsolationLevel} = fakeKysely();
    await new KyselyConnection(client).transaction(() =>
      Promise.resolve(undefined),
    );
    expect(setIsolationLevel).not.toHaveBeenCalled();
  });

  test('is passed to setIsolationLevel when given', async () => {
    const {client, setIsolationLevel} = fakeKysely();
    await new KyselyConnection(client, {
      isolationLevel: 'repeatable read',
    }).transaction(() => Promise.resolve(undefined));
    expect(setIsolationLevel).toHaveBeenCalledWith('repeatable read');
  });
});

describe('NodePgConnection isolation level', () => {
  function fakeClient() {
    const query = vi.fn().mockResolvedValue({rows: []});
    return {client: {query} as unknown as Client, query};
  }

  test('opens a bare BEGIN when no option is given', async () => {
    const {client, query} = fakeClient();
    await new NodePgConnection(client).transaction(() =>
      Promise.resolve(undefined),
    );
    expect(query).toHaveBeenCalledWith('BEGIN');
  });

  test('carries the level on BEGIN when given', async () => {
    const {client, query} = fakeClient();
    await new NodePgConnection(client, {
      isolationLevel: 'serializable',
    }).transaction(() => Promise.resolve(undefined));
    expect(query).toHaveBeenCalledWith('BEGIN ISOLATION LEVEL SERIALIZABLE');
  });
});

describe('PostgresJSConnection isolation level', () => {
  function fakePg() {
    const begin = vi.fn((...args: unknown[]) => {
      const fn = args.at(-1) as (tx: unknown) => Promise<unknown>;
      return fn({unsafe: vi.fn().mockResolvedValue([])});
    });
    return {pg: {begin} as unknown as postgres.Sql<never>, begin};
  }

  test('calls begin with only the callback when no option is given', async () => {
    const {pg, begin} = fakePg();
    await new PostgresJSConnection(pg).transaction(() =>
      Promise.resolve(undefined),
    );
    expect(begin).toHaveBeenCalledTimes(1);
    expect(begin.mock.calls[0]).toHaveLength(1);
  });

  test('passes the level as the begin options string when given', async () => {
    const {pg, begin} = fakePg();
    await new PostgresJSConnection(pg, {
      isolationLevel: 'repeatable read',
    }).transaction(() => Promise.resolve(undefined));
    expect(begin.mock.calls[0][0]).toBe('isolation level repeatable read');
  });
});

describe('DrizzleConnection isolation level', () => {
  function fakeDrizzle() {
    const transaction = vi.fn(
      (fn: (tx: unknown) => Promise<unknown>, _config?: unknown) =>
        fn({_: {session: {prepareQuery: {length: 4}}}}),
    );
    const drizzle = {
      _: {session: {prepareQuery: {length: 4}}},
      transaction,
    };
    return {drizzle, transaction};
  }

  test('passes no config when no option is given', async () => {
    const {drizzle, transaction} = fakeDrizzle();
    await new DrizzleConnection(drizzle as never).transaction(() =>
      Promise.resolve(undefined),
    );
    expect(transaction.mock.calls[0][1]).toBeUndefined();
  });

  test('passes the level as drizzle transaction config when given', async () => {
    const {drizzle, transaction} = fakeDrizzle();
    await new DrizzleConnection(drizzle as never, {
      isolationLevel: 'repeatable read',
    }).transaction(() => Promise.resolve(undefined));
    expect(transaction.mock.calls[0][1]).toStrictEqual({
      isolationLevel: 'repeatable read',
    });
  });
});

describe('PrismaConnection isolation level', () => {
  function fakePrisma() {
    const $transaction = vi.fn(
      (fn: (tx: unknown) => Promise<unknown>, _options?: unknown) =>
        fn({$queryRawUnsafe: vi.fn().mockResolvedValue([])}),
    );
    const client = {
      $queryRawUnsafe: vi.fn().mockResolvedValue([]),
      $transaction,
    };
    return {client, $transaction};
  }

  test('passes no options when none is given', async () => {
    const {client, $transaction} = fakePrisma();
    await new PrismaConnection(client as never).transaction(() =>
      Promise.resolve(undefined),
    );
    expect($transaction.mock.calls[0][1]).toBeUndefined();
  });

  // Prisma spells the levels in PascalCase, so the adapter maps them rather
  // than passing the Postgres spelling through.
  test('maps the level to prisma spelling when given', async () => {
    const {client, $transaction} = fakePrisma();
    await new PrismaConnection(client as never, {
      isolationLevel: 'repeatable read',
    }).transaction(() => Promise.resolve(undefined));
    expect($transaction.mock.calls[0][1]).toStrictEqual({
      isolationLevel: 'RepeatableRead',
    });
  });
});
