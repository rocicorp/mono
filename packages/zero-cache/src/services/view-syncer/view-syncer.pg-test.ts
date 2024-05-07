import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import {assert} from 'shared/src/asserts.js';
import {Queue} from 'shared/src/queue.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import type {Downstream, PokePartBody, Upstream} from 'zero-protocol';
import {TransactionPool} from '../../db/transaction-pool.js';
import {DurableStorage} from '../../storage/durable-storage.js';
import {testDBs} from '../../test/db.js';
import {FakeDurableObjectStorage} from '../../test/fake-do.js';
import {createSilentLogContext} from '../../test/logger.js';
import type {PostgresDB} from '../../types/pg.js';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import type {
  InvalidationWatcher,
  QueryInvalidationUpdate,
  WatchRequest,
} from '../invalidation-watcher/invalidation-watcher.js';
import type {InvalidationWatcherRegistry} from '../invalidation-watcher/registry.js';
import {getPublicationInfo} from '../replicator/tables/published.js';
import type {TableSpec} from '../replicator/tables/specs.js';
import {loadCVR} from './cvr.js';
import {ViewSyncerService} from './view-syncer.js';

describe('view-syncer/service', () => {
  let db: PostgresDB;
  const lc = createSilentLogContext();

  let storage: FakeDurableObjectStorage;
  let watcher: MockInvalidationWatcher;
  let vs: ViewSyncerService;
  let upstream: Subscription<Upstream>;
  let viewSyncerDone: Promise<void>;
  let downstream: Queue<Downstream>;

  beforeEach(async () => {
    db = await testDBs.create('view_syncer_service_test');
    await db`
    CREATE SCHEMA zero;
    CREATE TABLE zero.clients (
      "clientID" TEXT PRIMARY KEY,
      "lastMutationID" BIGINT,
      _0_version VARCHAR(38)
    );
    CREATE TABLE issues (
      id text PRIMARY KEY,
      owner_id text,
      parent_id text,
      big int8,
      title text,
      _0_version VARCHAR(38)
    );
    CREATE TABLE users (
      id text PRIMARY KEY,
      name text,
      _0_version VARCHAR(38)
    );

    INSERT INTO zero.clients ("clientID", "lastMutationID", _0_version)
                      VALUES ('foo', 42, '0a');

    INSERT INTO users (id, name, _0_version) VALUES ('100', 'Alice', '0a');
    INSERT INTO users (id, name,  _0_version) VALUES ('101', 'Bob', '0b');
    INSERT INTO users (id, name, _0_version) VALUES ('102', 'Candice', '0c');

    INSERT INTO issues (id, title, owner_id, big, _0_version) VALUES ('1', 'parent issue foo', 100, 9007199254740991, '1a0');
    INSERT INTO issues (id, title, owner_id, big, _0_version) VALUES ('2', 'parent issue bar', 101, -9007199254740991, '1ab');
    INSERT INTO issues (id, title, owner_id, parent_id, big, _0_version) VALUES ('3', 'foo', 102, 1, 123, '1ca');
    INSERT INTO issues (id, title, owner_id, parent_id, big, _0_version) VALUES ('4', 'bar', 101, 2, 100, '1cd');
    -- The last row should not match the ISSUES_TITLE_QUERY: "WHERE id IN (1, 2, 3, 4)"
    INSERT INTO issues (id, title, owner_id, parent_id, big, _0_version) VALUES ('5', 'not matched', 101, 2, 100, '1cd');

    CREATE PUBLICATION zero_all FOR ALL TABLES;
    `.simple();

    storage = new FakeDurableObjectStorage();
    watcher = new MockInvalidationWatcher();
    vs = new ViewSyncerService(
      lc,
      serviceID,
      new DurableStorage(storage),
      watcher,
      db,
    );
    upstream = clientUpstream();
    viewSyncerDone = vs.run();
    downstream = new Queue();
    const stream = await vs.sync(
      {clientID: 'foo', baseCookie: null},
      {
        desiredQueriesPatch: [
          {op: 'put', hash: 'query-hash1', ast: ISSUES_TITLE_QUERY},
        ],
      },
      upstream,
    );
    void pipeToQueue(stream, downstream);
  });

  async function pipeToQueue(
    stream: AsyncIterable<Downstream>,
    queue: Queue<Downstream>,
  ) {
    try {
      for await (const msg of stream) {
        await queue.enqueue(msg);
      }
    } catch (e) {
      await queue.enqueueRejection(e);
    }
  }

  afterEach(async () => {
    await vs.stop();
    await viewSyncerDone;
    await testDBs.drop(db);
  });

  const serviceID = '9876';

  function clientUpstream(...up: Upstream[]): Subscription<Upstream> {
    const stream = new Subscription<Upstream>();
    up.forEach(msg => stream.push(msg));
    return stream;
  }

  const ISSUES_TITLE_QUERY: AST = {
    select: [
      ['id', 'id'],
      ['title', 'title'],
      ['big', 'big'],
    ],
    table: 'issues',
    where: {
      type: 'simple',
      field: 'id',
      op: 'IN',
      value: {
        type: 'literal',
        value: ['1', '2', '3', '4'],
      },
    },
  };

  const USERS_NAME_QUERY: AST = {
    select: [
      ['id', 'id'],
      ['name', 'name'],
    ],
    table: 'users',
  };

  test('initializes schema', async () => {
    expect(await storage.get('/vs/storage_schema_meta')).toEqual({
      // Update versions as necessary
      version: 1,
      maxVersion: 1,
      minSafeRollbackVersion: 1,
    });
  });

  test('adds desired queries from initConnectionMessage', async () => {
    const cvr = await loadCVR(new DurableStorage(storage), serviceID);
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash1'],
          id: 'foo',
          patchVersion: {stateVersion: '00', minorVersion: 1},
        },
      },
      id: '9876',
      queries: {
        'query-hash1': {
          ast: ISSUES_TITLE_QUERY,
          desiredBy: {foo: {stateVersion: '00', minorVersion: 1}},
          id: 'query-hash1',
        },
      },
      version: {stateVersion: '00', minorVersion: 1},
    });
  });

  test('subscribes and responds to initial invalidation', async () => {
    const request = await watcher.requests.dequeue();
    expect(request.fromVersion).toBeUndefined();
    expect(Object.keys(request.queries).length).toBe(
      2, // including internal "lmids" query
    );

    await watcher.notify({
      fromVersion: null,
      newVersion: '1xz',
      invalidatedQueries: new Set(),
    });

    expect(await downstream.dequeue()).toEqual([
      'pokeStart',
      {pokeID: '1xz', baseCookie: null, cookie: '1xz'},
    ]);
    expect(await downstream.dequeue()).toEqual([
      'pokePart',
      {
        pokeID: '1xz',
        clientsPatch: [{clientID: 'foo', op: 'put'}],
        lastMutationIDChanges: {foo: 42},
        desiredQueriesPatches: {
          foo: [{ast: ISSUES_TITLE_QUERY, hash: 'query-hash1', op: 'put'}],
        },
        entitiesPatch: [
          {
            op: 'put',
            entityID: {id: '1'},
            entityType: 'issues',
            value: {
              id: '1',
              title: 'parent issue foo',
              big: 9007199254740991,
            },
          },
          {
            op: 'put',
            entityID: {id: '2'},
            entityType: 'issues',
            value: {
              id: '2',
              title: 'parent issue bar',
              big: -9007199254740991,
            },
          },
          {
            op: 'put',
            entityID: {id: '3'},
            entityType: 'issues',
            value: {id: '3', title: 'foo', big: 123},
          },
          {
            op: 'put',
            entityID: {id: '4'},
            entityType: 'issues',
            value: {id: '4', title: 'bar', big: 100},
          },
        ],
        gotQueriesPatch: [
          {ast: ISSUES_TITLE_QUERY, hash: 'query-hash1', op: 'put'},
        ],
      } satisfies PokePartBody,
    ]);
    expect(await downstream.dequeue()).toEqual(['pokeEnd', {pokeID: '1xz'}]);

    const cvr = await loadCVR(new DurableStorage(storage), serviceID);
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash1'],
          id: 'foo',
          patchVersion: {stateVersion: '00', minorVersion: 1},
        },
      },
      id: '9876',
      queries: {
        'lmids': {
          ast: {
            schema: 'zero',
            table: 'clients',
            select: [
              ['clientID', 'clientID'],
              ['lastMutationID', 'lastMutationID'],
            ],
            where: {
              type: 'conjunction',
              op: 'OR',
              conditions: [
                {
                  type: 'simple',
                  op: '=',
                  field: 'clientID',
                  value: {type: 'literal', value: 'foo'},
                },
              ],
            },
          },
          internal: true,
          id: 'lmids',
          transformationVersion: {stateVersion: '1xz'},
        },
        'query-hash1': {
          ast: ISSUES_TITLE_QUERY,
          desiredBy: {foo: {stateVersion: '00', minorVersion: 1}},
          id: 'query-hash1',
          patchVersion: {stateVersion: '1xz'},
          transformationVersion: {stateVersion: '1xz'},
        },
      },
      version: {stateVersion: '1xz'},
    });

    const rowRecords = await storage.list({
      prefix: `/vs/cvr/${serviceID}/d/`,
    });
    expect(new Set(rowRecords.values())).toEqual(
      new Set([
        {
          id: {rowKey: {id: '1'}, schema: 'public', table: 'issues'},
          patchVersion: {stateVersion: '1xz'},
          queriedColumns: {
            id: ['query-hash1'],
            title: ['query-hash1'],
            big: ['query-hash1'],
          },
          rowVersion: '1a0',
        },
        {
          id: {rowKey: {id: '2'}, schema: 'public', table: 'issues'},
          patchVersion: {stateVersion: '1xz'},
          queriedColumns: {
            id: ['query-hash1'],
            title: ['query-hash1'],
            big: ['query-hash1'],
          },
          rowVersion: '1ab',
        },
        {
          id: {rowKey: {id: '3'}, schema: 'public', table: 'issues'},
          patchVersion: {stateVersion: '1xz'},
          queriedColumns: {
            id: ['query-hash1'],
            title: ['query-hash1'],
            big: ['query-hash1'],
          },
          rowVersion: '1ca',
        },
        {
          id: {rowKey: {id: '4'}, schema: 'public', table: 'issues'},
          patchVersion: {stateVersion: '1xz'},
          queriedColumns: {
            id: ['query-hash1'],
            title: ['query-hash1'],
            big: ['query-hash1'],
          },
          rowVersion: '1cd',
        },
        {
          id: {rowKey: {clientID: 'foo'}, schema: 'zero', table: 'clients'},
          patchVersion: {stateVersion: '1xz'},
          queriedColumns: {clientID: ['lmids'], lastMutationID: ['lmids']},
          rowVersion: '0a',
        },
      ]),
    );

    const rowPatches = await storage.list({
      prefix: `/vs/cvr/${serviceID}/p/d/`,
    });
    expect(rowPatches).toEqual(
      new Map([
        [
          '/vs/cvr/9876/p/d/1xz/r/Qxp2tFD-UOgu7-78ZYiLHw',
          {
            columns: ['big', 'id', 'title'],
            id: {rowKey: {id: '4'}, schema: 'public', table: 'issues'},
            op: 'put',
            rowVersion: '1cd',
            type: 'row',
          },
        ],
        [
          '/vs/cvr/9876/p/d/1xz/r/VPg9hxKPhJtHB6oYkGqBpw',
          {
            columns: ['big', 'id', 'title'],
            id: {rowKey: {id: '2'}, schema: 'public', table: 'issues'},
            op: 'put',
            rowVersion: '1ab',
            type: 'row',
          },
        ],
        [
          '/vs/cvr/9876/p/d/1xz/r/oA1bf0ulYhik9qypZFPeLQ',
          {
            columns: ['big', 'id', 'title'],
            id: {rowKey: {id: '1'}, schema: 'public', table: 'issues'},
            op: 'put',
            rowVersion: '1a0',
            type: 'row',
          },
        ],
        [
          '/vs/cvr/9876/p/d/1xz/r/wfZrxQPRsszHpdfLRWoPzA',
          {
            columns: ['big', 'id', 'title'],
            id: {rowKey: {id: '3'}, schema: 'public', table: 'issues'},
            op: 'put',
            rowVersion: '1ca',
            type: 'row',
          },
        ],
        [
          '/vs/cvr/9876/p/d/1xz/r/RRjZLHnRXDtSeGWxUc_a4w',
          {
            columns: ['clientID', 'lastMutationID'],
            id: {
              schema: 'zero',
              table: 'clients',
              rowKey: {clientID: 'foo'},
            },
            op: 'put',
            rowVersion: '0a',
            type: 'row',
          },
        ],
      ]),
    );
  });

  test('responds to changeQueriesPatch', async () => {
    await watcher.requests.dequeue();

    // Change the set of queries.
    upstream.push([
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {op: 'put', hash: 'query-hash2', ast: USERS_NAME_QUERY},
          {op: 'del', hash: 'query-hash1'},
        ],
      },
    ]);

    // Should ask the invalidation watcher for a new subscription.
    await watcher.requests.dequeue();

    await watcher.notify({
      fromVersion: null,
      newVersion: '1xz',
      invalidatedQueries: new Set(),
    });

    expect(await downstream.dequeue()).toEqual([
      'pokeStart',
      {pokeID: '1xz', baseCookie: null, cookie: '1xz'},
    ]);
    expect(await downstream.dequeue()).toEqual([
      'pokePart',
      {
        pokeID: '1xz',
        clientsPatch: [{clientID: 'foo', op: 'put'}],
        lastMutationIDChanges: {foo: 42},
        desiredQueriesPatches: {
          foo: [
            {hash: 'query-hash1', op: 'del'},
            {ast: USERS_NAME_QUERY, hash: 'query-hash2', op: 'put'},
          ],
        },
        entitiesPatch: [
          {
            op: 'put',
            entityID: {id: '100'},
            entityType: 'users',
            value: {id: '100', name: 'Alice'},
          },
          {
            op: 'put',
            entityID: {id: '101'},
            entityType: 'users',
            value: {id: '101', name: 'Bob'},
          },
          {
            op: 'put',
            entityID: {id: '102'},
            entityType: 'users',
            value: {id: '102', name: 'Candice'},
          },
        ],
        gotQueriesPatch: [
          {hash: 'query-hash1', op: 'del'},
          {ast: USERS_NAME_QUERY, hash: 'query-hash2', op: 'put'},
        ],
      } satisfies PokePartBody,
    ]);
    expect(await downstream.dequeue()).toEqual(['pokeEnd', {pokeID: '1xz'}]);

    const cvr = await loadCVR(new DurableStorage(storage), serviceID);
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash2'],
          id: 'foo',
          patchVersion: {stateVersion: '00', minorVersion: 1},
        },
      },
      id: '9876',
      queries: {
        'lmids': {
          ast: {
            schema: 'zero',
            table: 'clients',
            select: [
              ['clientID', 'clientID'],
              ['lastMutationID', 'lastMutationID'],
            ],
            where: {
              type: 'conjunction',
              op: 'OR',
              conditions: [
                {
                  type: 'simple',
                  op: '=',
                  field: 'clientID',
                  value: {type: 'literal', value: 'foo'},
                },
              ],
            },
          },
          internal: true,
          id: 'lmids',
          transformationVersion: {stateVersion: '1xz'},
        },
        'query-hash2': {
          ast: USERS_NAME_QUERY,
          desiredBy: {foo: {stateVersion: '00', minorVersion: 2}},
          id: 'query-hash2',
          patchVersion: {stateVersion: '1xz'},
          transformationVersion: {stateVersion: '1xz'},
        },
      },
      version: {stateVersion: '1xz'},
    });
  });

  const INVALID_QUERY: AST = {
    table: 'users',
    select: [['non_existent_column', 'non_existent_column']],
  };

  test('rejects a bad initConnectionMessage', async () => {
    const stream = await vs.sync(
      {clientID: 'boo', baseCookie: null},
      {
        desiredQueriesPatch: [
          {op: 'put', hash: 'bad-query', ast: INVALID_QUERY},
        ],
      },
      upstream,
    );
    const down = new Queue<Downstream>();
    void pipeToQueue(stream, down);

    let err;
    try {
      for (let i = 0; i < 3; i++) {
        await down.dequeue();
      }
    } catch (e) {
      err = e;
    }
    expect(err).not.toBeUndefined();

    // Bad client / query should not have been added to the CVR.
    const cvr = await loadCVR(new DurableStorage(storage), serviceID);
    expect(Object.keys(cvr.clients)).not.toContain('boo');
    expect(Object.keys(cvr.queries)).not.toContain('bad-query');
  });

  test('rejects a bad changeQueriesPatch', async () => {
    await watcher.requests.dequeue();

    // Try adding an invalid query.
    upstream.push([
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {op: 'put', hash: 'query-hash3', ast: INVALID_QUERY},
        ],
      },
    ]);

    let err;
    try {
      for (let i = 0; i < 3; i++) {
        await downstream.dequeue();
      }
    } catch (e) {
      err = e;
    }
    expect(err).not.toBeUndefined();

    // Bad query should not have been added to the CVR.
    const cvr = await loadCVR(new DurableStorage(storage), serviceID);
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash1'],
          id: 'foo',
          patchVersion: {stateVersion: '00', minorVersion: 1},
        },
      },
      id: '9876',
      queries: {
        'lmids': {
          ast: {
            schema: 'zero',
            table: 'clients',
            select: [
              ['clientID', 'clientID'],
              ['lastMutationID', 'lastMutationID'],
            ],
            where: {
              type: 'conjunction',
              op: 'OR',
              conditions: [
                {
                  type: 'simple',
                  op: '=',
                  field: 'clientID',
                  value: {type: 'literal', value: 'foo'},
                },
              ],
            },
          },
          internal: true,
          id: 'lmids',
        },
        'query-hash1': {
          ast: ISSUES_TITLE_QUERY,
          desiredBy: {foo: {stateVersion: '00', minorVersion: 1}},
          id: 'query-hash1',
        },
      },
    });
  });

  test('fails pokes with error on unsafe integer', async () => {
    // Make one value too large to send back in the current zero-protocol.
    await db`UPDATE issues SET big = 10000000000000000 WHERE id = '4';`;

    await watcher.requests.dequeue();
    await watcher.notify({
      fromVersion: null,
      newVersion: '1xz',
      invalidatedQueries: new Set(),
    });

    let err;
    try {
      for (let i = 0; i < 3; i++) {
        await downstream.dequeue();
      }
    } catch (e) {
      err = e;
    }
    expect(err).not.toBeUndefined();

    // Everything else should succeed, however, because CVRs are agnostic to row
    // contents, and the data in the DB is technically "valid" (and available when
    // the protocol supports it).
    const cvr = await loadCVR(new DurableStorage(storage), serviceID);
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash1'],
          id: 'foo',
          patchVersion: {stateVersion: '00', minorVersion: 1},
        },
      },
      id: '9876',
      queries: {
        'lmids': {
          ast: {
            schema: 'zero',
            table: 'clients',
            select: [
              ['clientID', 'clientID'],
              ['lastMutationID', 'lastMutationID'],
            ],
            where: {
              type: 'conjunction',
              op: 'OR',
              conditions: [
                {
                  type: 'simple',
                  op: '=',
                  field: 'clientID',
                  value: {type: 'literal', value: 'foo'},
                },
              ],
            },
          },
          internal: true,
          id: 'lmids',
          transformationVersion: {stateVersion: '1xz'},
        },
        'query-hash1': {
          ast: ISSUES_TITLE_QUERY,
          desiredBy: {foo: {stateVersion: '00', minorVersion: 1}},
          id: 'query-hash1',
          patchVersion: {stateVersion: '1xz'},
          transformationVersion: {stateVersion: '1xz'},
        },
      },
      version: {stateVersion: '1xz'},
    });

    const rowRecords = await storage.list({
      prefix: `/vs/cvr/${serviceID}/d/`,
    });
    expect(new Set(rowRecords.values())).toEqual(
      new Set([
        {
          id: {rowKey: {id: '1'}, schema: 'public', table: 'issues'},
          patchVersion: {stateVersion: '1xz'},
          queriedColumns: {
            id: ['query-hash1'],
            title: ['query-hash1'],
            big: ['query-hash1'],
          },
          rowVersion: '1a0',
        },
        {
          id: {rowKey: {id: '2'}, schema: 'public', table: 'issues'},
          patchVersion: {stateVersion: '1xz'},
          queriedColumns: {
            id: ['query-hash1'],
            title: ['query-hash1'],
            big: ['query-hash1'],
          },
          rowVersion: '1ab',
        },
        {
          id: {rowKey: {id: '3'}, schema: 'public', table: 'issues'},
          patchVersion: {stateVersion: '1xz'},
          queriedColumns: {
            id: ['query-hash1'],
            title: ['query-hash1'],
            big: ['query-hash1'],
          },
          rowVersion: '1ca',
        },
        {
          id: {rowKey: {id: '4'}, schema: 'public', table: 'issues'},
          patchVersion: {stateVersion: '1xz'},
          queriedColumns: {
            id: ['query-hash1'],
            title: ['query-hash1'],
            big: ['query-hash1'],
          },
          rowVersion: '1cd',
        },
        {
          id: {rowKey: {clientID: 'foo'}, schema: 'zero', table: 'clients'},
          patchVersion: {stateVersion: '1xz'},
          queriedColumns: {clientID: ['lmids'], lastMutationID: ['lmids']},
          rowVersion: '0a',
        },
      ]),
    );

    const rowPatches = await storage.list({
      prefix: `/vs/cvr/${serviceID}/p/d/`,
    });
    expect(rowPatches).toEqual(
      new Map([
        [
          '/vs/cvr/9876/p/d/1xz/r/Qxp2tFD-UOgu7-78ZYiLHw',
          {
            columns: ['big', 'id', 'title'],
            id: {rowKey: {id: '4'}, schema: 'public', table: 'issues'},
            op: 'put',
            rowVersion: '1cd',
            type: 'row',
          },
        ],
        [
          '/vs/cvr/9876/p/d/1xz/r/VPg9hxKPhJtHB6oYkGqBpw',
          {
            columns: ['big', 'id', 'title'],
            id: {rowKey: {id: '2'}, schema: 'public', table: 'issues'},
            op: 'put',
            rowVersion: '1ab',
            type: 'row',
          },
        ],
        [
          '/vs/cvr/9876/p/d/1xz/r/oA1bf0ulYhik9qypZFPeLQ',
          {
            columns: ['big', 'id', 'title'],
            id: {rowKey: {id: '1'}, schema: 'public', table: 'issues'},
            op: 'put',
            rowVersion: '1a0',
            type: 'row',
          },
        ],
        [
          '/vs/cvr/9876/p/d/1xz/r/wfZrxQPRsszHpdfLRWoPzA',
          {
            columns: ['big', 'id', 'title'],
            id: {rowKey: {id: '3'}, schema: 'public', table: 'issues'},
            op: 'put',
            rowVersion: '1ca',
            type: 'row',
          },
        ],
        [
          '/vs/cvr/9876/p/d/1xz/r/RRjZLHnRXDtSeGWxUc_a4w',
          {
            columns: ['clientID', 'lastMutationID'],
            id: {
              schema: 'zero',
              table: 'clients',
              rowKey: {clientID: 'foo'},
            },
            op: 'put',
            rowVersion: '0a',
            type: 'row',
          },
        ],
      ]),
    );
  });

  class MockInvalidationWatcher
    implements InvalidationWatcher, InvalidationWatcherRegistry
  {
    readonly requests = new Queue<WatchRequest>();
    readonly consumed = new Queue<true>();
    subscription: Subscription<QueryInvalidationUpdate> | undefined;

    watch(
      request: WatchRequest,
    ): Promise<CancelableAsyncIterable<QueryInvalidationUpdate>> {
      void this.requests.enqueue(request);
      assert(
        this.subscription === undefined,
        'Only one subscription expected (at a time) in this test',
      );
      this.subscription = new Subscription<QueryInvalidationUpdate>({
        consumed: () => this.consumed.enqueue(true),
        cleanup: () => (this.subscription = undefined),
      });
      return Promise.resolve(this.subscription);
    }

    async notify(invalidation: Omit<QueryInvalidationUpdate, 'reader'>) {
      const reader = new TransactionPool(lc);
      const readerDone = reader.run(db);

      assert(this.subscription, 'no subscription');
      this.subscription.push({...invalidation, reader});
      await this.consumed.dequeue();

      reader.setDone();
      await readerDone;
    }

    async getTableSchemas(): Promise<readonly TableSpec[]> {
      const published = await getPublicationInfo(db);
      return published.tables;
    }

    getInvalidationWatcher(): Promise<InvalidationWatcher> {
      return Promise.resolve(this);
    }
  }
});
