import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import * as ErrorKind from '../../../../zero-protocol/src/error-kind-enum.ts';
import type {
  PokeEndMessage,
  PokePartMessage,
  PokeStartMessage,
} from '../../../../zero-protocol/src/poke.ts';
import type {JSONObject} from '../../types/bigint-json.ts';
import {ErrorForClient} from '../../types/error-for-client.ts';
import {Subscription} from '../../types/subscription.ts';
import {ClientHandler, ensureSafeJSON, type Patch} from './client-handler.ts';

const APP_ID = 'zapp';
const SHARD_NUM = 6;
const SHARD = {appID: APP_ID, shardNum: SHARD_NUM};

describe('view-syncer/client-handler', () => {
  const lc = createSilentLogContext();

  test('no-op and canceled pokes', () => {
    const poke1Version = {stateVersion: '123'};
    const poke2Version = {stateVersion: '125'};
    const poke3Version = {stateVersion: '127'};
    const poke4Version = {stateVersion: '129'};

    const received: Downstream[] = [];
    // Subscriptions that dump unconsumed pokes to `received`
    const subscription = Subscription.create<Downstream>({
      cleanup: msgs => received.push(...msgs),
    });

    const schemaVersion = 1;
    const schemaVersions = {minSupportedVersion: 1, maxSupportedVersion: 1};
    const handler = new ClientHandler(
      lc,
      'g1',
      'id1',
      'ws1',
      SHARD,
      '121',
      schemaVersion,
      subscription,
    );

    // One poke advances from 121 => 123.
    let poker = handler.startPoke(poke1Version, schemaVersions);
    poker.end(poke1Version);

    // The second poke starts the advancement to 125 but then reverts to 123.
    poker = handler.startPoke(poke2Version, schemaVersions);
    poker.end(poke1Version);

    // The third poke gets canceled.
    poker = handler.startPoke(poke3Version, schemaVersions);
    poker.cancel();

    // The fourth poke advances to 129.
    poker = handler.startPoke(poke4Version, schemaVersions);
    poker.end(poke4Version);

    subscription.cancel(); // Drains any pushed messages to received.

    // Only the first and last pokes should have been received.
    expect(received).toEqual([
      [
        'pokeStart',
        {
          baseCookie: '121',
          pokeID: '123',
          schemaVersions: {
            maxSupportedVersion: 1,
            minSupportedVersion: 1,
          },
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
          schemaVersions: {
            maxSupportedVersion: 1,
            minSupportedVersion: 1,
          },
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

  test('poke handler for multiple clients', () => {
    const poke1Version = {stateVersion: '121'};
    const poke2Version = {stateVersion: '123'};

    const received: Downstream[][] = [[], [], []];
    // Subscriptions that dump unconsumed pokes to `received`
    const subscriptions = received.map(bucket =>
      Subscription.create<Downstream>({
        cleanup: msgs => bucket.push(...msgs),
      }),
    );

    const schemaVersion = 1;
    const schemaVersions = {minSupportedVersion: 1, maxSupportedVersion: 1};
    const handlers = [
      // Client 1 is already caught up.
      new ClientHandler(
        lc,
        'g1',
        'id1',
        'ws1',
        SHARD,
        '121',
        schemaVersion,
        subscriptions[0],
      ),
      // Client 2 is a bit behind.
      new ClientHandler(
        lc,
        'g1',
        'id2',
        'ws2',
        SHARD,
        '120:01',
        schemaVersion,
        subscriptions[1],
      ),
      // Client 3 is more behind.
      new ClientHandler(
        lc,
        'g1',
        'id3',
        'ws3',
        SHARD,
        '11z',
        schemaVersion,
        subscriptions[2],
      ),
    ];

    let pokers = handlers.map(client =>
      client.startPoke(poke1Version, schemaVersions),
    );
    for (const poker of pokers) {
      poker.addPatch({
        toVersion: {stateVersion: '11z', minorVersion: 1},
        patch: {
          type: 'query',
          op: 'put',
          id: 'foohash',
          clientID: 'foo',
          ast: {table: 'issues'},
        },
      });
      poker.addPatch({
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
      poker.addPatch({
        toVersion: {stateVersion: '120', minorVersion: 2},
        patch: {type: 'query', op: 'del', id: 'barhash', clientID: 'foo'},
      });
      poker.addPatch({
        toVersion: {stateVersion: '121'},
        patch: {
          type: 'query',
          op: 'put',
          id: 'bazhash',
          ast: {table: 'labels'},
        },
      });

      poker.addPatch({
        toVersion: {stateVersion: '120', minorVersion: 2},
        patch: {
          type: 'row',
          op: 'put',
          id: {schema: 'public', table: 'issues', rowKey: {id: 'bar'}},
          contents: {id: 'bar', name: 'hello', num: 123},
        },
      });
      poker.addPatch({
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
      poker.addPatch({
        toVersion: {stateVersion: '11z', minorVersion: 1},
        patch: {
          type: 'row',
          op: 'del',
          id: {schema: 'public', table: 'issues', rowKey: {id: 'foo'}},
        },
      });
      poker.addPatch({
        toVersion: {stateVersion: '121'},
        patch: {
          type: 'row',
          op: 'put',
          id: {schema: 'public', table: 'issues', rowKey: {id: 'boo'}},
          contents: {id: 'boo', name: 'world', num: 123456},
        },
      });
      poker.addPatch({
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

      poker.end(poke1Version);
    }

    // Now send another (empty) poke with everyone at the same baseCookie.
    pokers = handlers.map(client =>
      client.startPoke(poke2Version, schemaVersions),
    );
    for (const poker of pokers) {
      poker.end(poke2Version);
    }

    // Cancel the subscriptions to collect the unconsumed messages.
    subscriptions.forEach(sub => sub.cancel());

    // Client 1 was already caught up. Only gets the second poke.
    expect(received[0]).toEqual([
      [
        'pokeStart',
        {pokeID: '123', baseCookie: '121', schemaVersions},
      ] satisfies PokeStartMessage,
      ['pokeEnd', {pokeID: '123', cookie: '123'}] satisfies PokeEndMessage,
    ]);

    // Client 2 is a bit behind.
    expect(received[1]).toEqual([
      [
        'pokeStart',
        {pokeID: '121', baseCookie: '120:01', schemaVersions},
      ] satisfies PokeStartMessage,
      [
        'pokePart',
        {
          pokeID: '121',
          lastMutationIDChanges: {foo: 124},
          desiredQueriesPatches: {
            foo: [{op: 'del', hash: 'barhash'}],
          },
          gotQueriesPatch: [
            {op: 'put', hash: 'bazhash', ast: {table: 'labels'}},
          ],
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
        {pokeID: '123', baseCookie: '121', schemaVersions},
      ] satisfies PokeStartMessage,
      ['pokeEnd', {pokeID: '123', cookie: '123'}] satisfies PokeEndMessage,
    ]);

    // Client 3 is more behind.
    expect(received[2]).toEqual([
      [
        'pokeStart',
        {pokeID: '121', baseCookie: '11z', schemaVersions},
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
              {op: 'put', hash: 'foohash', ast: {table: 'issues'}},
              {op: 'del', hash: 'barhash'},
            ],
          },
          gotQueriesPatch: [
            {op: 'put', hash: 'bazhash', ast: {table: 'labels'}},
          ],
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
        {pokeID: '123', baseCookie: '121', schemaVersions},
      ] satisfies PokeStartMessage,
      ['pokeEnd', {pokeID: '123', cookie: '123'}] satisfies PokeEndMessage,
    ]);
  });

  test('schemaVersion unsupported', () => {
    const received: Downstream[] = [];
    let e: Error | undefined = undefined;
    const subscription = Subscription.create<Downstream>({
      cleanup: (msgs, err) => {
        received.push(...msgs);
        e = err;
      },
    });

    const lc = createSilentLogContext();
    const schemaVersion = 1;
    const schemaVersions = {minSupportedVersion: 2, maxSupportedVersion: 3};
    const clientHandler = new ClientHandler(
      lc,
      'g1',
      'id1',
      'ws1',
      SHARD,
      '120',
      schemaVersion,
      subscription,
    );

    const poker = clientHandler.startPoke(
      {stateVersion: '121'},
      schemaVersions,
    );
    poker.end({stateVersion: '121'});

    subscription.cancel();

    expect(received).toEqual([]);
    expect(e).toBeInstanceOf(ErrorForClient);
    expect((e as unknown as ErrorForClient).errorBody).toEqual({
      kind: ErrorKind.SchemaVersionNotSupported,
      message:
        'Schema version 1 is not in range of supported schema versions [2, 3].',
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
      let terminated = false;
      const downstream = Subscription.create<Downstream>({
        cleanup: () => {
          terminated = true;
        },
      });

      const schemaVersion = 1;
      const schemaVersions = {minSupportedVersion: 1, maxSupportedVersion: 1};
      const handler = new ClientHandler(
        createSilentLogContext(),
        'g1',
        'id1',
        'ws1',
        SHARD,
        '121',
        schemaVersion,
        downstream,
      );
      const poker = handler.startPoke({stateVersion: '123'}, schemaVersions);

      expect(terminated).toBe(false);
      poker.addPatch({toVersion: {stateVersion: '123'}, patch});
      expect(terminated).toBe(true);

      let err;
      try {
        for await (const _ of downstream) {
          // Should not be reached.
        }
      } catch (e) {
        err = e;
      }
      expect(err).not.toBeUndefined();
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
      } catch (e) {
        // expected === undefined
      }
      expect(result).toEqual(expected);
    }
  });
});
