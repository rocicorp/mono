import {resolver} from '@rocicorp/resolver';
import {beforeEach, describe, expect, test} from 'vitest';
import type {JSONObject} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import type {
  PokeEndMessage,
  PokePartMessage,
  PokeStartMessage,
} from '../../../../zero-protocol/src/poke.ts';
import {Subscription} from '../../types/subscription.ts';
import {
  ClientHandler,
  ensureSafeJSON,
  startPoke,
  type Patch,
  type PokeHandler,
} from './client-handler.ts';

const APP_ID = 'zapp';
const SHARD_NUM = 6;
const SHARD = {appID: APP_ID, shardNum: SHARD_NUM};

describe('view-syncer/client-handler', () => {
  const lc = createSilentLogContext();

  function createSubscription() {
    const received: Downstream[] = [];
    const unconsumed: Downstream[] = [];
    const subscription = Subscription.create<Downstream>({
      cleanup: msgs => unconsumed.push(...msgs),
    });
    let err: Error | undefined;
    const {promise: loopDone, resolve: onDone} = resolver();
    void (async function () {
      try {
        for await (const msg of subscription) {
          received.push(msg);
        }
      } catch (e) {
        err = e instanceof Error ? e : new Error(String(e));
      } finally {
        onDone();
      }
    })();

    return {
      subscription,
      close: async () => {
        subscription.cancel();
        await loopDone;
        return {received: [...received, ...unconsumed], err};
      },
    };
  }

  test('no-op and canceled pokes', async () => {
    const poke1Version = {stateVersion: '123'};
    const poke2Version = {stateVersion: '125'};
    const poke3Version = {stateVersion: '127'};
    const poke4Version = {stateVersion: '129'};

    const {subscription, close} = createSubscription();

    const handler = new ClientHandler(
      lc,
      'g1',
      'id1',
      'ws1',
      SHARD,
      '121',
      subscription,
    );

    // One poke advances from 121 => 123.
    let poker = handler.startPoke(poke1Version);
    await poker.end(poke1Version);

    // The second poke starts the advancement to 125 but then reverts to 123.
    poker = handler.startPoke(poke2Version);
    await poker.end(poke1Version);

    // The third poke gets canceled.
    poker = handler.startPoke(poke3Version);
    await poker.cancel();

    // The fourth poke advances to 129.
    poker = handler.startPoke(poke4Version);
    await poker.end(poke4Version);

    const {received} = await close();

    // Only the first and last pokes should have been received.
    expect(received).toEqual([
      [
        'pokeStart',
        {
          baseCookie: '121',
          pokeID: '123',
        },
      ],
      [
        'pokeEnd',
        {
          cookie: '123',
          pokeID: '123',
        },
      ],
      [
        'pokeStart',
        {
          baseCookie: '123',
          pokeID: '129',
        },
      ],
      [
        'pokeEnd',
        {
          cookie: '129',
          pokeID: '129',
        },
      ],
    ]);
  });

  test('poke handler for multiple clients', async () => {
    const poke1Version = {stateVersion: '121'};
    const poke2Version = {stateVersion: '123'};

    const subscriptions = [
      createSubscription(),
      createSubscription(),
      createSubscription(),
    ];

    const handlers = [
      // Client 1 is already caught up.
      new ClientHandler(
        lc,
        'g1',
        'id1',
        'ws1',
        SHARD,
        '121',
        subscriptions[0].subscription,
      ),
      // Client 2 is a bit behind.
      new ClientHandler(
        lc,
        'g1',
        'id2',
        'ws2',
        SHARD,
        '120:01',
        subscriptions[1].subscription,
      ),
      // Client 3 is more behind.
      new ClientHandler(
        lc,
        'g1',
        'id3',
        'ws3',
        SHARD,
        '11z',
        subscriptions[2].subscription,
      ),
    ];

    let pokers = startPoke(handlers, poke1Version);
    await pokers.addPatch({
      toVersion: {stateVersion: '11z', minorVersion: 1},
      patch: {
        type: 'query',
        op: 'put',
        id: 'foohash',
        clientID: 'foo',
      },
    });
    await pokers.addPatch({
      toVersion: {stateVersion: '120'},
      patch: {
        type: 'row',
        op: 'put',
        id: {
          schema: '',
          table: 'zapp_6.clients',
          rowKey: {clientID: 'bar'},
        },
        contents: {
          clientGroupID: 'g1',
          clientID: 'bar',
          lastMutationID: 321n,
          userID: 'ignored',
        },
      },
    });
    await pokers.addPatch({
      toVersion: {stateVersion: '120', minorVersion: 2},
      patch: {type: 'query', op: 'del', id: 'barhash', clientID: 'foo'},
    });
    await pokers.addPatch({
      toVersion: {stateVersion: '121'},
      patch: {
        type: 'query',
        op: 'put',
        id: 'bazhash',
      },
    });

    await pokers.addPatch({
      toVersion: {stateVersion: '120', minorVersion: 2},
      patch: {
        type: 'row',
        op: 'put',
        id: {schema: 'public', table: 'issues', rowKey: {id: 'bar'}},
        contents: {id: 'bar', name: 'hello', num: 123},
      },
    });
    await pokers.addPatch({
      toVersion: {stateVersion: '120'},
      patch: {
        type: 'row',
        op: 'put',
        id: {
          schema: '',
          table: 'zapp_6.clients',
          rowKey: {clientID: 'foo'},
        },
        contents: {
          clientGroupID: 'g1',
          clientID: 'foo',
          lastMutationID: 123n,
        },
      },
    });
    await pokers.addPatch({
      toVersion: {stateVersion: '11z', minorVersion: 1},
      patch: {
        type: 'row',
        op: 'del',
        id: {schema: 'public', table: 'issues', rowKey: {id: 'foo'}},
      },
    });
    await pokers.addPatch({
      toVersion: {stateVersion: '121'},
      patch: {
        type: 'row',
        op: 'put',
        id: {schema: 'public', table: 'issues', rowKey: {id: 'boo'}},
        contents: {id: 'boo', name: 'world', num: 123456},
      },
    });
    await pokers.addPatch({
      toVersion: {stateVersion: '121'},
      patch: {
        type: 'row',
        op: 'put',
        id: {
          schema: '',
          table: 'zapp_6.clients',
          rowKey: {clientID: 'foo'},
        },
        contents: {
          clientGroupID: 'g1',
          clientID: 'foo',
          lastMutationID: 124n,
        },
      },
    });

    await pokers.end(poke1Version);

    // Now send another (empty) poke with everyone at the same baseCookie.
    pokers = startPoke(handlers, poke2Version);
    await pokers.end(poke2Version);

    const results = await Promise.all(subscriptions.map(sub => sub.close()));

    // Client 1 was already caught up. Only gets the second poke.
    expect(results[0].received).toEqual([
      [
        'pokeStart',
        {pokeID: '123', baseCookie: '121'},
      ] satisfies PokeStartMessage,
      ['pokeEnd', {pokeID: '123', cookie: '123'}] satisfies PokeEndMessage,
    ]);

    // Client 2 is a bit behind.
    expect(results[1].received).toEqual([
      [
        'pokeStart',
        {pokeID: '121', baseCookie: '120:01'},
      ] satisfies PokeStartMessage,
      [
        'pokePart',
        {
          pokeID: '121',
          lastMutationIDChanges: {foo: 124},
          desiredQueriesPatches: {
            foo: [{op: 'del', hash: 'barhash'}],
          },
          gotQueriesPatch: [{op: 'put', hash: 'bazhash'}],
          rowsPatch: [
            {
              op: 'put',
              tableName: 'issues',
              value: {id: 'bar', name: 'hello', num: 123},
            },
            {
              op: 'put',
              tableName: 'issues',
              value: {id: 'boo', name: 'world', num: 123456},
            },
          ],
        },
      ] satisfies PokePartMessage,
      ['pokeEnd', {pokeID: '121', cookie: '121'}] satisfies PokeEndMessage,

      // Second poke
      [
        'pokeStart',
        {pokeID: '123', baseCookie: '121'},
      ] satisfies PokeStartMessage,
      ['pokeEnd', {pokeID: '123', cookie: '123'}] satisfies PokeEndMessage,
    ]);

    // Client 3 is more behind.
    expect(results[2].received).toEqual([
      [
        'pokeStart',
        {pokeID: '121', baseCookie: '11z'},
      ] satisfies PokeStartMessage,
      [
        'pokePart',
        {
          pokeID: '121',
          lastMutationIDChanges: {
            bar: 321,
            foo: 124,
          },
          desiredQueriesPatches: {
            foo: [
              {op: 'put', hash: 'foohash'},
              {op: 'del', hash: 'barhash'},
            ],
          },
          gotQueriesPatch: [{op: 'put', hash: 'bazhash'}],
          rowsPatch: [
            {
              op: 'put',
              tableName: 'issues',
              value: {id: 'bar', name: 'hello', num: 123},
            },
            {op: 'del', tableName: 'issues', id: {id: 'foo'}},
            {
              op: 'put',
              tableName: 'issues',
              value: {id: 'boo', name: 'world', num: 123456},
            },
          ],
        },
      ] satisfies PokePartMessage,
      ['pokeEnd', {pokeID: '121', cookie: '121'}] satisfies PokeEndMessage,

      // Second poke
      [
        'pokeStart',
        {pokeID: '123', baseCookie: '121'},
      ] satisfies PokeStartMessage,
      ['pokeEnd', {pokeID: '123', cookie: '123'}] satisfies PokeEndMessage,
    ]);
  });

  describe('mutation results', () => {
    let poker: PokeHandler;
    let closer: ReturnType<typeof createSubscription>['close'];

    beforeEach(() => {
      const {subscription, close} = createSubscription();

      const handler = new ClientHandler(
        lc,
        'g1',
        'id1',
        'ws1',
        SHARD,
        '121',
        subscription,
      );
      poker = handler.startPoke({stateVersion: '123'});
      closer = close;
    });

    test('successful mutation result', async () => {
      await poker.addPatch({
        toVersion: {stateVersion: '123'},
        patch: {
          type: 'row',
          op: 'put',
          id: {
            schema: '',
            table: 'zapp_6.mutations',
            rowKey: {clientGroupID: 'g1', clientID: 'boo', mutationID: 123n},
          },
          contents: {
            clientGroupID: 'g1',
            clientID: 'boo',
            mutationID: 123n,
            result: {},
          },
        },
      });

      await poker.end({stateVersion: '123'});

      const {received, err} = await closer();
      expect(received).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "121",
              "pokeID": "123",
            },
          ],
          [
            "pokePart",
            {
              "mutationsPatch": [
                {
                  "mutation": {
                    "id": {
                      "clientID": "boo",
                      "id": 123,
                    },
                    "result": {},
                  },
                  "op": "put",
                },
              ],
              "pokeID": "123",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "123",
              "pokeID": "123",
            },
          ],
        ]
      `);
      expect(err).toBeUndefined();
    });

    test('failed mutation result', async () => {
      await poker.addPatch({
        toVersion: {stateVersion: '123'},
        patch: {
          type: 'row',
          op: 'put',
          id: {
            schema: '',
            table: 'zapp_6.mutations',
            rowKey: {clientGroupID: 'g1', clientID: 'boo', mutationID: 123n},
          },
          contents: {
            clientGroupID: 'g1',
            clientID: 'boo',
            mutationID: 123n,
            result: {
              error: 'app',
              message: 'Something went wrong',
            },
          },
        },
      });

      await poker.end({stateVersion: '123'});

      const {received, err} = await closer();
      expect(received).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "121",
              "pokeID": "123",
            },
          ],
          [
            "pokePart",
            {
              "mutationsPatch": [
                {
                  "mutation": {
                    "id": {
                      "clientID": "boo",
                      "id": 123,
                    },
                    "result": {
                      "error": "app",
                      "message": "Something went wrong",
                    },
                  },
                  "op": "put",
                },
              ],
              "pokeID": "123",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "123",
              "pokeID": "123",
            },
          ],
        ]
      `);
      expect(err).toBeUndefined();
    });

    // nothing to do here.
    // the client stores mutation results ephemerally and discards them on `put`
    // so no need to send a `del` for the mutation result.
    test('removed mutation result', async () => {
      await poker.addPatch({
        toVersion: {stateVersion: '123'},
        patch: {
          type: 'row',
          op: 'del',
          id: {
            schema: '',
            table: 'zapp_6.mutations',
            rowKey: {clientGroupID: 'g1', clientID: 'boo', mutationID: 123n},
          },
        },
      });

      await poker.end({stateVersion: '123'});

      const {received, err} = await closer();
      expect(received).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "121",
              "pokeID": "123",
            },
          ],
          [
            "pokePart",
            {
              "mutationsPatch": [
                {
                  "id": {
                    "clientID": "boo",
                    "id": 123,
                  },
                  "op": "del",
                },
              ],
              "pokeID": "123",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "123",
              "pokeID": "123",
            },
          ],
        ]
      `);
      expect(err).toBeUndefined();
    });
  });

  test('error on unsafe integer', async () => {
    for (const patch of [
      {
        type: 'row',
        op: 'put',
        id: {schema: 'public', table: 'issues', rowKey: {id: 'boo'}},
        contents: {id: 'boo', name: 'world', big: 12345231234123414n},
      },
      {
        type: 'row',
        op: 'put',
        id: {schema: 'public', table: 'issues', rowKey: {id: 'boo'}},
        contents: {id: 'boo', name: 'world', big: 983712341234123412348n},
      },
      {
        type: 'row',
        op: 'put',
        id: {schema: '', table: 'zapp_6.clients', rowKey: {clientID: 'boo'}},
        contents: {
          clientGroupID: 'g1',
          clientID: 'boo',
          lastMutationID: 98371234123423412341238n,
        },
      },
    ] satisfies Patch[]) {
      const {subscription, close} = createSubscription();

      const handler = new ClientHandler(
        createSilentLogContext(),
        'g1',
        'id1',
        'ws1',
        SHARD,
        '121',
        subscription,
      );
      const poker = handler.startPoke({stateVersion: '123'});

      await poker.addPatch({toVersion: {stateVersion: '123'}, patch});
      const {err} = await close();
      expect(String(err)).toMatch(
        /Error: Value of "\w+" exceeds safe Number range \(\d+\)/,
      );
    }
  });

  test('ensureSafeJSON', () => {
    for (const {input, expected} of [
      {
        input: {foo: 1, bar: 2n},
        expected: {foo: 1, bar: 2},
      },
      {
        input: {foo: '1', bar: 234n},
        expected: {foo: '1', bar: 234},
      },
      {
        input: {foo: 123n, bar: {baz: 23423423}},
        expected: {foo: 123, bar: {baz: 23423423}},
      },
      {
        input: {foo: '1', bar: 23423423434923874239487n},
      },
      {
        input: {foo: '1', bar: {baz: 23423423434923874239487n}},
      },
    ] satisfies {input: JSONObject; expected?: JSONObject}[]) {
      let result;
      try {
        result = ensureSafeJSON(input);
      } catch {
        // expected === undefined
      }
      expect(result).toEqual(expected);
    }
  });
});
