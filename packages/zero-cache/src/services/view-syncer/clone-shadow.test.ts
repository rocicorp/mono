import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import type {JSONValue} from '../../../../shared/src/json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.ts';
import {
  CloneShadow,
  type CloneShadowResult,
  type PipelineStateSource,
} from './clone-shadow.ts';
import {PipelineDriver, type Timer} from './pipeline-driver.ts';
import {Snapshotter} from './snapshotter.ts';

const SHARD = {appID: 'this_app', shardNum: 2};
const timer: Timer = {elapsedLap: () => 0, totalElapsed: () => 0};
const messages = new ReplicationMessages({issues: 'id', comments: 'id'});

const CLIENT_SCHEMA: ClientSchema = {
  tables: {
    issues: {
      columns: {
        id: {type: 'string'},
        rank: {type: 'number'},
      },
      primaryKey: ['id'],
    },
    comments: {
      columns: {
        id: {type: 'string'},
        issueID: {type: 'string'},
      },
      primaryKey: ['id'],
    },
  },
};

const ALL: AST = {table: 'issues', orderBy: [['id', 'asc']]};
const TOP_2: AST = {
  table: 'issues',
  orderBy: [
    ['rank', 'asc'],
    ['id', 'asc'],
  ],
  limit: 2,
};
const WITH_COMMENTS: AST = {
  table: 'issues',
  orderBy: [['id', 'asc']],
  where: {
    type: 'correlatedSubquery',
    op: 'EXISTS',
    related: {
      correlation: {parentField: ['id'], childField: ['issueID']},
      subquery: {table: 'comments', alias: 'zsubq_c', orderBy: [['id', 'asc']]},
    },
  },
};

describe('clone shadow', () => {
  const lc = createSilentLogContext();
  let dbFile: DbFile;
  let replica: Database;
  let replicator: FakeReplicator;
  let storage: DatabaseStorage;
  let shadow: CloneShadow;
  let nextGroup: number;
  let version: number;
  const results: CloneShadowResult[] = [];

  beforeEach(() => {
    dbFile = new DbFile('clone_shadow');
    replica = dbFile.connect(lc);
    initReplicationState(replica, ['zero_data'], '01');
    replica.pragma('journal_mode = WAL2');
    replica.pragma('busy_timeout = 1');
    replica.exec(`
      CREATE TABLE "this_app.permissions" (lock INT PRIMARY KEY, permissions JSON, hash TEXT, _0_version TEXT NOT NULL);
      INSERT INTO "this_app.permissions" (lock, permissions, hash, _0_version) VALUES (1, NULL, NULL, '01');
      CREATE TABLE issues (id TEXT PRIMARY KEY, rank INTEGER, _0_version TEXT NOT NULL);
      CREATE TABLE comments (id TEXT PRIMARY KEY, issueID TEXT, _0_version TEXT NOT NULL);
      INSERT INTO issues VALUES ('1', 10, '01'), ('2', 20, '01'), ('3', 30, '01'), ('4', 40, '01');
      INSERT INTO comments VALUES ('c1', '1', '01'), ('c2', '3', '01');
    `);
    replicator = fakeReplicator(lc, replica);
    const storageDB = new Database(lc, ':memory:');
    storageDB.prepare(CREATE_STORAGE_TABLE).run();
    storage = new DatabaseStorage(storageDB);
    shadow = newShadow();
    nextGroup = 0;
    version = 1;
    results.length = 0;
  });

  afterEach(() => {
    replica.close();
    dbFile.delete();
  });

  function newShadow(maxStateEntries?: number) {
    return new CloneShadow({
      lc,
      maxStateEntries,
      onResult: r => results.push(r),
    });
  }

  function newDriver(
    clientSchema = CLIENT_SCHEMA,
    cloneShadow: CloneShadow | null = shadow,
  ) {
    const id = `group${nextGroup++}`;
    const driver = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, SHARD),
      SHARD,
      storage.createClientGroupStorage(id),
      id,
      new InspectorDelegate(undefined),
      () => 200,
      undefined,
      undefined,
      undefined,
      undefined,
      cloneShadow ?? undefined,
    );
    driver.init(clientSchema);
    return driver;
  }

  function hydrate(driver: PipelineDriver, hash: string, ast: AST) {
    return [...driver.addQuery(hash, `q-${hash}`, ast, timer)];
  }

  function hydrated(driver: PipelineDriver, hash: string, ast: AST) {
    hydrate(driver, hash, ast);
    return results.at(-1)?.outcome;
  }

  function commit(...changes: object[]) {
    version += 1;
    replicator.processTransaction(
      String(version).padStart(2, '0'),
      ...(changes as never[]),
    );
  }

  function advance(driver: PipelineDriver) {
    return [...driver.advance(timer).changes];
  }

  test('reports no donor for the first group to hydrate a query', () => {
    expect(hydrated(newDriver(), 'a', ALL)).toBe('no-donor');
  });

  test.each([
    ['a plain query', ALL],
    ['a limited query', TOP_2],
    ['an EXISTS query', WITH_COMMENTS],
  ])('matches a second group hydrating %s', (_name, ast) => {
    hydrate(newDriver(), 'a', ast);
    expect(hydrated(newDriver(), 'a', ast)).toBe('match');
  });

  test('counts the operator state it compared', () => {
    hydrate(newDriver(), 'a', TOP_2);
    hydrate(newDriver(), 'a', TOP_2);
    expect(results.at(-1)?.stateEntries).toBeGreaterThan(0);
  });

  test('a different transformation hash is not a donor', () => {
    hydrate(newDriver(), 'a', ALL);
    expect(hydrated(newDriver(), 'b', ALL)).toBe('no-donor');
  });

  test('a different client schema is not a donor', () => {
    hydrate(newDriver(), 'a', ALL);
    const other: ClientSchema = {
      tables: {issues: {columns: {id: {type: 'string'}}, primaryKey: ['id']}},
    };
    expect(hydrated(newDriver(other), 'a', ALL)).toBe('no-donor');
  });

  test('a donor at another version is reported as a version mismatch', () => {
    hydrate(newDriver(), 'a', ALL);
    commit(messages.insert('issues', {id: '5', rank: 50}));
    expect(hydrated(newDriver(), 'a', ALL)).toBe('version-mismatch');
  });

  test('a donor advanced to the same version still matches', () => {
    const donor = newDriver();
    hydrate(donor, 'a', TOP_2);
    commit(messages.insert('issues', {id: '0', rank: 5}));
    advance(donor);
    expect(hydrated(newDriver(), 'a', TOP_2)).toBe('match');
  });

  test('a donor advanced past a removal at the limit still matches', () => {
    const donor = newDriver();
    hydrate(donor, 'a', TOP_2);
    commit(messages.delete('issues', {id: '1'}));
    advance(donor);
    expect(hydrated(newDriver(), 'a', TOP_2)).toBe('match');
  });

  test('an EXISTS donor advanced to the same version still matches', () => {
    const donor = newDriver();
    hydrate(donor, 'a', WITH_COMMENTS);
    commit(messages.insert('comments', {id: 'c3', issueID: '2'}));
    advance(donor);
    expect(hydrated(newDriver(), 'a', WITH_COMMENTS)).toBe('match');
  });

  test('a donor in the middle of advancing is reported as busy', () => {
    const donor = newDriver();
    hydrate(donor, 'a', ALL);
    commit(messages.insert('issues', {id: '5', rank: 50}));
    const changes = donor.advance(timer).changes[Symbol.iterator]();
    changes.next();
    expect(hydrated(newDriver(), 'a', ALL)).toBe('donor-busy');
    while (!changes.next().done) {
      // Finishes the advance.
    }
  });

  test('a removed query is no longer a donor', () => {
    const donor = newDriver();
    hydrate(donor, 'a', ALL);
    donor.removeQuery('q-a');
    expect(hydrated(newDriver(), 'a', ALL)).toBe('no-donor');
  });

  test('a destroyed driver is no longer a donor', () => {
    const donor = newDriver();
    hydrate(donor, 'a', ALL);
    donor.destroy();
    expect(hydrated(newDriver(), 'a', ALL)).toBe('no-donor');
  });

  test('reports a state too large to compare without comparing it', () => {
    shadow = newShadow(0);
    hydrate(newDriver(), 'a', TOP_2);
    expect(hydrated(newDriver(), 'a', TOP_2)).toBe('too-large');
  });

  test('leftover state from advancing is reported, not treated as a conflict', () => {
    const donor = newDriver();
    hydrate(donor, 'a', WITH_COMMENTS);
    commit(messages.delete('comments', {id: 'c1'}));
    advance(donor);
    expect(hydrated(newDriver(), 'a', WITH_COMMENTS)).toMatch(
      /^(match|donor-has-extra-state)$/,
    );
  });

  test('a donor with leftover state keeps producing the same rows', () => {
    const donor = newDriver();
    hydrate(donor, 'a', WITH_COMMENTS);
    commit(
      messages.delete('comments', {id: 'c1'}),
      messages.insert('comments', {id: 'c3', issueID: '2'}),
    );
    advance(donor);
    const fresh = newDriver();
    hydrate(fresh, 'a', WITH_COMMENTS);

    for (const change of [
      messages.insert('comments', {id: 'c4', issueID: '1'}),
      messages.delete('comments', {id: 'c3'}),
      messages.insert('comments', {id: 'c5', issueID: '4'}),
      messages.delete('issues', {id: '4'}),
      messages.insert('issues', {id: '9', rank: 90}),
      messages.insert('comments', {id: 'c6', issueID: '9'}),
    ]) {
      commit(change);
      advance(donor);
      advance(fresh);
      expect(donor.rowSetSignature('q-a')).toBe(fresh.rowSetSignature('q-a'));
    }
  });

  test('does nothing without a shadow', () => {
    hydrate(newDriver(CLIENT_SCHEMA, null), 'a', ALL);
    expect(results).toEqual([]);
  });

  test('detects rows that differ from the donor', () => {
    hydrate(newDriver(), 'a', ALL);
    // The same hash on a different query is what a wrong identity would be.
    expect(hydrated(newDriver(), 'a', {...ALL, limit: 1})).toBe('rows-differ');
  });

  describe('comparing operator state', () => {
    function source(
      operators: Record<string, JSONValue>[],
      {signature = 1n, version = '01', busy = false} = {},
    ): PipelineStateSource {
      return {
        version: () => version,
        busy: () => busy,
        rowSetSignature: () => signature,
        operators: operators.map(entries => ({
          scan: () => Object.entries(entries),
        })),
      };
    }

    function compare(donor: PipelineStateSource, target: PipelineStateSource) {
      const s = newShadow();
      s.register('key', donor);
      return s.compare('key', target, 0).outcome;
    }

    test('matches identical state', () => {
      expect(compare(source([{k: {a: 1}}]), source([{k: {a: 1}}]))).toBe(
        'match',
      );
    });

    test('detects a changed value', () => {
      expect(compare(source([{k: {a: 1}}]), source([{k: {a: 2}}]))).toBe(
        'state-differ',
      );
    });

    test('reports the first difference', () => {
      const s = newShadow();
      s.register('key', source([{same: 1}, {k: {a: 1}}]));
      expect(
        s.compare('key', source([{same: 1}, {k: {a: 2}}]), 0).difference,
      ).toEqual({operator: 1, key: 'k', donor: {a: 1}, target: {a: 2}});
    });

    test('tells state only the donor holds from a real difference', () => {
      expect(compare(source([{k: 1, l: 2}]), source([{k: 1}]))).toBe(
        'donor-has-extra-state',
      );
    });

    test('counts the extra entries across operators', () => {
      const s = newShadow();
      s.register(
        'key',
        source([
          {k: 1, x: 2},
          {y: 3, z: 4},
        ]),
      );
      expect(s.compare('key', source([{k: 1}, {}]), 0).extraDonorEntries).toBe(
        3,
      );
    });

    test('state only the new pipeline holds is a real difference', () => {
      expect(compare(source([{k: 1}]), source([{k: 1, l: 2}]))).toBe(
        'state-differ',
      );
    });

    test('a conflict outranks extra donor state', () => {
      const s = newShadow();
      s.register('key', source([{x: 1, k: {a: 1}}]));
      const result = s.compare('key', source([{k: {a: 2}}]), 0);
      expect(result.outcome).toBe('state-differ');
      expect(result.difference?.key).toBe('k');
    });

    test('detects a different number of operators', () => {
      expect(compare(source([{k: 1}]), source([{k: 1}, {}]))).toBe(
        'state-differ',
      );
    });

    test('does not treat a group as its own donor', () => {
      const s = newShadow();
      const self = source([{k: 1}]);
      s.register('key', self);
      expect(s.compare('key', self, 0).outcome).toBe('no-donor');
    });

    test('prefers a usable donor over a busy or stale one', () => {
      const s = newShadow();
      s.register('key', source([{k: 2}], {busy: true}));
      s.register('key', source([{k: 2}], {version: '00'}));
      s.register('key', source([{k: 1}]));
      expect(s.compare('key', source([{k: 1}]), 0).outcome).toBe('match');
    });
  });
});
