import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {
  type MockInstance,
  afterEach,
  beforeEach,
  describe,
  expect,
  suite,
  test,
  vi,
} from 'vitest';
import type {MutationPatch} from '../../../zero-protocol/src/mutations-patch.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {serverToClient} from '../../../zero-schema/src/name-mapper.ts';
import {MutationTracker} from './mutation-tracker.ts';
import {PokeHandler, mergePokes} from './zero-poke-handler.ts';

let rafStub: MockInstance<(cb: FrameRequestCallback) => number>;
// The FrameRequestCallback in PokeHandler does not use
// its time argument, so use an arbitrary constant for it in tests.
const UNUSED_RAF_ARG = 10;

const ackMutationResponses = () => {};
const onFatalError = () => {};

const schema = createSchema({
  tables: [
    table('issue')
      .from('issues')
      .columns({
        id: string().from('issue_id'),
        title: string(),
      })
      .primaryKey('id'),
    table('label')
      .from('labels')
      .columns({
        id: string().from('label_id'),
        name: string(),
      })
      .primaryKey('id'),
  ],
});

describe('poke handler', () => {
  beforeEach(() => {
    rafStub = vi
      .spyOn(globalThis, 'requestAnimationFrame')
      .mockImplementation(() => 0);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  test('completed poke plays on first raf', async () => {
    const onPokeErrorStub = vi.fn();
    const replicachePokeStub = vi.fn();
    const clientID = 'c1';
    const logContext = new LogContext('error');
    const pokeHandler = new PokeHandler(
      replicachePokeStub,
      onPokeErrorStub,
      clientID,
      schema,
      logContext,
      new MutationTracker(logContext, ackMutationResponses, onFatalError),
    );
    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke1',
      baseCookie: '1',
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 1,
        c2: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {
            ['issue_id']: 'issue1',
            title: 'foo1',
            description: 'columns not in client schema pass through',
          },
        },
      ],
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo2'},
        },
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue2', title: 'bar1'},
        },
      ],
    });

    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeEnd({pokeID: 'poke1', cookie: '2'});

    expect(rafStub).toHaveBeenCalledTimes(1);
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);

    const rafCallback0 = rafStub.mock.calls[0][0];
    await rafCallback0(UNUSED_RAF_ARG);

    expect(replicachePokeStub).toHaveBeenCalledTimes(1);
    const replicachePoke0 = replicachePokeStub.mock.calls[0][0];
    expect(replicachePoke0).toEqual({
      baseCookie: '1',
      pullResponse: {
        cookie: '2',
        lastMutationIDChanges: {
          c1: 2,
          c2: 2,
        },
        patch: [
          {
            op: 'put',
            key: 'e/issue/issue1',
            value: {
              id: 'issue1',
              title: 'foo1',
              description: 'columns not in client schema pass through',
            },
          },
          {
            op: 'put',
            key: 'e/issue/issue1',
            value: {id: 'issue1', title: 'foo2'},
          },
          {
            op: 'put',
            key: 'e/issue/issue2',
            value: {id: 'issue2', title: 'bar1'},
          },
        ],
      },
    });

    expect(rafStub).toHaveBeenCalledTimes(2);

    const rafCallback1 = rafStub.mock.calls[1][0];
    await rafCallback1(UNUSED_RAF_ARG);
    expect(replicachePokeStub).toHaveBeenCalledTimes(1);
    expect(rafStub).toHaveBeenCalledTimes(2);
  });

  test('canceled poke is not applied', async () => {
    const onPokeErrorStub = vi.fn();
    const replicachePokeStub = vi.fn();
    const clientID = 'c1';
    const logContext = new LogContext('error');
    const pokeHandler = new PokeHandler(
      replicachePokeStub,
      onPokeErrorStub,
      clientID,
      schema,
      logContext,
      new MutationTracker(logContext, ackMutationResponses, onFatalError),
    );
    expect(rafStub).toHaveBeenCalledTimes(0);

    const pokeStartAndParts = (pokeID: string) => {
      pokeHandler.handlePokeStart({
        pokeID,
        baseCookie: '1',
      });
      pokeHandler.handlePokePart({
        pokeID,
        lastMutationIDChanges: {
          c1: 1,
          c2: 2,
        },
        rowsPatch: [
          {
            op: 'put',
            tableName: 'issues',
            value: {['issue_id']: 'issue1', title: 'foo1'},
          },
        ],
      });
      pokeHandler.handlePokePart({
        pokeID,
        lastMutationIDChanges: {
          c1: 2,
        },
        rowsPatch: [
          {
            op: 'put',
            tableName: 'issues',
            value: {['issue_id']: 'issue1', title: 'foo2'},
          },
          {
            op: 'put',
            tableName: 'issues',
            value: {['issue_id']: 'issue2', title: 'bar1'},
          },
        ],
      });
    };
    pokeStartAndParts('poke1');

    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeEnd({pokeID: 'poke1', cookie: '', cancel: true});

    // raf is not scheduled because poke was canceled;
    expect(rafStub).toHaveBeenCalledTimes(0);
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);

    // now test receiving a poke after the canceled poke
    pokeStartAndParts('poke2');

    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeEnd({pokeID: 'poke2', cookie: '2'});

    expect(rafStub).toHaveBeenCalledTimes(1);
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);

    const rafCallback0 = rafStub.mock.calls[0][0];
    await rafCallback0(UNUSED_RAF_ARG);

    expect(replicachePokeStub).toHaveBeenCalledTimes(1);
    const replicachePoke0 = replicachePokeStub.mock.calls[0][0];
    expect(replicachePoke0).toEqual({
      baseCookie: '1',
      pullResponse: {
        cookie: '2',
        lastMutationIDChanges: {
          c1: 2,
          c2: 2,
        },
        patch: [
          {
            op: 'put',
            key: 'e/issue/issue1',
            value: {id: 'issue1', title: 'foo1'},
          },
          {
            op: 'put',
            key: 'e/issue/issue1',
            value: {id: 'issue1', title: 'foo2'},
          },
          {
            op: 'put',
            key: 'e/issue/issue2',
            value: {id: 'issue2', title: 'bar1'},
          },
        ],
      },
    });

    expect(rafStub).toHaveBeenCalledTimes(2);

    const rafCallback1 = rafStub.mock.calls[1][0];
    await rafCallback1(UNUSED_RAF_ARG);
    expect(replicachePokeStub).toHaveBeenCalledTimes(1);
    expect(rafStub).toHaveBeenCalledTimes(2);
  });

  test('multiple pokes received before raf callback are merged', async () => {
    const onPokeErrorStub = vi.fn();
    const replicachePokeStub = vi.fn();
    const clientID = 'c1';
    const logContext = new LogContext('error');
    const pokeHandler = new PokeHandler(
      replicachePokeStub,
      onPokeErrorStub,
      clientID,
      schema,
      logContext,
      new MutationTracker(logContext, ackMutationResponses, onFatalError),
    );

    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke1',
      baseCookie: '1',
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 1,
        c2: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo1'},
        },
      ],
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo2'},
        },
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue2', title: 'bar1'},
        },
      ],
    });

    expect(rafStub).toHaveBeenCalledTimes(0);
    pokeHandler.handlePokeEnd({pokeID: 'poke1', cookie: '2'});

    expect(rafStub).toHaveBeenCalledTimes(1);
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke2',
      baseCookie: '2',
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke2',
      lastMutationIDChanges: {
        c1: 3,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue3', title: 'baz1'},
        },
      ],
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke2',
      lastMutationIDChanges: {
        c3: 1,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue2', title: 'bar2'},
        },
      ],
    });

    expect(rafStub).toHaveBeenCalledTimes(1);

    pokeHandler.handlePokeEnd({pokeID: 'poke2', cookie: '3'});

    expect(rafStub).toHaveBeenCalledTimes(1);
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);

    const rafCallback0 = rafStub.mock.calls[0][0];
    await rafCallback0(UNUSED_RAF_ARG);

    expect(replicachePokeStub).toHaveBeenCalledTimes(1);
    const replicachePoke0 = replicachePokeStub.mock.calls[0][0];
    expect(replicachePoke0).toEqual({
      baseCookie: '1',
      pullResponse: {
        cookie: '3',
        lastMutationIDChanges: {
          c1: 3,
          c2: 2,
          c3: 1,
        },
        patch: [
          {
            op: 'put',
            key: 'e/issue/issue1',
            value: {id: 'issue1', title: 'foo1'},
          },
          {
            op: 'put',
            key: 'e/issue/issue1',
            value: {id: 'issue1', title: 'foo2'},
          },
          {
            op: 'put',
            key: 'e/issue/issue2',
            value: {id: 'issue2', title: 'bar1'},
          },
          {
            op: 'put',
            key: 'e/issue/issue3',
            value: {id: 'issue3', title: 'baz1'},
          },
          {
            op: 'put',
            key: 'e/issue/issue2',
            value: {id: 'issue2', title: 'bar2'},
          },
        ],
      },
    });

    expect(rafStub).toHaveBeenCalledTimes(2);

    const rafCallback1 = rafStub.mock.calls[1][0];
    await rafCallback1(UNUSED_RAF_ARG);
    expect(replicachePokeStub).toHaveBeenCalledTimes(1);
    expect(rafStub).toHaveBeenCalledTimes(2);
  });

  test('multiple pokes received before raf callback are merged, canceled pokes are not merged', async () => {
    const onPokeErrorStub = vi.fn();
    const replicachePokeStub = vi.fn();
    const clientID = 'c1';
    const logContext = new LogContext('error');
    const pokeHandler = new PokeHandler(
      replicachePokeStub,
      onPokeErrorStub,
      clientID,
      schema,
      logContext,
      new MutationTracker(logContext, ackMutationResponses, onFatalError),
    );

    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke1',
      baseCookie: '1',
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 1,
        c2: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo1'},
        },
      ],
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo2'},
        },
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue2', title: 'bar1'},
        },
      ],
    });

    expect(rafStub).toHaveBeenCalledTimes(0);
    pokeHandler.handlePokeEnd({pokeID: 'poke1', cookie: '2'});

    expect(rafStub).toHaveBeenCalledTimes(1);
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke2',
      baseCookie: '2',
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke2',
      lastMutationIDChanges: {
        c1: 3,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue3', title: 'baz1'},
        },
      ],
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke2',
      lastMutationIDChanges: {
        c3: 1,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue2', title: 'bar2'},
        },
      ],
    });

    expect(rafStub).toHaveBeenCalledTimes(1);

    pokeHandler.handlePokeEnd({pokeID: 'poke2', cookie: '', cancel: true});

    expect(rafStub).toHaveBeenCalledTimes(1);
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke3',
      baseCookie: '2',
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke3',
      lastMutationIDChanges: {
        c1: 3,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue4', title: 'baz2'},
        },
      ],
    });

    expect(rafStub).toHaveBeenCalledTimes(1);

    pokeHandler.handlePokeEnd({pokeID: 'poke3', cookie: '3'});

    const rafCallback0 = rafStub.mock.calls[0][0];
    await rafCallback0(UNUSED_RAF_ARG);

    expect(replicachePokeStub).toHaveBeenCalledTimes(1);
    const replicachePoke0 = replicachePokeStub.mock.calls[0][0];
    expect(replicachePoke0).toEqual({
      baseCookie: '1',
      pullResponse: {
        cookie: '3',
        lastMutationIDChanges: {
          c1: 3,
          c2: 2,
          // Not included because corresponding poke was canceled
          // c3: 1,
        },
        patch: [
          {
            op: 'put',
            key: 'e/issue/issue1',
            value: {id: 'issue1', title: 'foo1'},
          },
          {
            op: 'put',
            key: 'e/issue/issue1',
            value: {id: 'issue1', title: 'foo2'},
          },
          {
            op: 'put',
            key: 'e/issue/issue2',
            value: {id: 'issue2', title: 'bar1'},
          },
          // Following not included because corresponding poke was canceled
          // {
          //   op: 'put',
          //   key: 'e/issue/issue3',
          //   value: {id: 'issue3', title: 'baz1'},
          // },
          // {
          //   op: 'put',
          //   key: 'e/issue/issue2',
          //   value: {id: 'issue2', title: 'bar2'},
          // },
          // non-canceled poke after canceled poke is merged
          {
            op: 'put',
            key: 'e/issue/issue4',
            value: {id: 'issue4', title: 'baz2'},
          },
        ],
      },
    });

    expect(rafStub).toHaveBeenCalledTimes(2);

    const rafCallback1 = rafStub.mock.calls[1][0];
    await rafCallback1(UNUSED_RAF_ARG);
    expect(replicachePokeStub).toHaveBeenCalledTimes(1);
    expect(rafStub).toHaveBeenCalledTimes(2);
  });

  test('playback over series of rafs', async () => {
    const onPokeErrorStub = vi.fn();
    const replicachePokeStub = vi.fn();
    const clientID = 'c1';
    const logContext = new LogContext('error');
    const pokeHandler = new PokeHandler(
      replicachePokeStub,
      onPokeErrorStub,
      clientID,
      schema,
      logContext,
      new MutationTracker(logContext, ackMutationResponses, onFatalError),
    );

    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke1',
      baseCookie: '1',
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 1,
        c2: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo1'},
        },
      ],
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo2'},
        },
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue2', title: 'bar1'},
        },
      ],
    });

    expect(rafStub).toHaveBeenCalledTimes(0);
    pokeHandler.handlePokeEnd({pokeID: 'poke1', cookie: '2'});

    expect(rafStub).toHaveBeenCalledTimes(1);
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);

    const rafCallback0 = rafStub.mock.calls[0][0];
    await rafCallback0(UNUSED_RAF_ARG);

    expect(replicachePokeStub).toHaveBeenCalledTimes(1);
    const replicachePoke0 = replicachePokeStub.mock.calls[0][0];
    expect(replicachePoke0).toEqual({
      baseCookie: '1',
      pullResponse: {
        cookie: '2',
        lastMutationIDChanges: {
          c1: 2,
          c2: 2,
        },
        patch: [
          {
            op: 'put',
            key: 'e/issue/issue1',
            value: {id: 'issue1', title: 'foo1'},
          },
          {
            op: 'put',
            key: 'e/issue/issue1',
            value: {id: 'issue1', title: 'foo2'},
          },
          {
            op: 'put',
            key: 'e/issue/issue2',
            value: {id: 'issue2', title: 'bar1'},
          },
        ],
      },
    });

    expect(rafStub).toHaveBeenCalledTimes(2);

    pokeHandler.handlePokeStart({
      pokeID: 'poke2',
      baseCookie: '2',
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke2',
      lastMutationIDChanges: {
        c1: 3,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue3', title: 'baz1'},
        },
      ],
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke2',
      lastMutationIDChanges: {
        c3: 1,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue2', title: 'bar2'},
        },
      ],
    });

    expect(rafStub).toHaveBeenCalledTimes(2);

    pokeHandler.handlePokeEnd({pokeID: 'poke2', cookie: '3'});

    expect(rafStub).toHaveBeenCalledTimes(2);
    expect(replicachePokeStub).toHaveBeenCalledTimes(1);

    const rafCallback1 = rafStub.mock.calls[1][0];
    await rafCallback1(UNUSED_RAF_ARG);

    expect(replicachePokeStub).toHaveBeenCalledTimes(2);
    const replicachePoke1 = replicachePokeStub.mock.calls[1][0];
    expect(replicachePoke1).toEqual({
      baseCookie: '2',
      pullResponse: {
        cookie: '3',
        lastMutationIDChanges: {
          c1: 3,
          c3: 1,
        },
        patch: [
          {
            op: 'put',
            key: 'e/issue/issue3',
            value: {id: 'issue3', title: 'baz1'},
          },
          {
            op: 'put',
            key: 'e/issue/issue2',
            value: {id: 'issue2', title: 'bar2'},
          },
        ],
      },
    });

    expect(rafStub).toHaveBeenCalledTimes(3);

    const rafCallback2 = rafStub.mock.calls[2][0];
    await rafCallback2(UNUSED_RAF_ARG);
    expect(replicachePokeStub).toHaveBeenCalledTimes(2);
    expect(rafStub).toHaveBeenCalledTimes(3);
  });

  suite('onPokeErrors', () => {
    const cases: {
      name: string;
      causeError: (pokeHandler: PokeHandler) => void;
    }[] = [
      {
        name: 'pokePart before pokeStart',
        causeError: pokeHandler => {
          pokeHandler.handlePokePart({pokeID: 'poke1'});
        },
      },
      {
        name: 'pokeEnd before pokeStart',
        causeError: pokeHandler => {
          pokeHandler.handlePokeEnd({pokeID: 'poke1', cookie: '2'});
        },
      },
      {
        name: 'pokePart with wrong pokeID',
        causeError: pokeHandler => {
          pokeHandler.handlePokeStart({
            pokeID: 'poke1',
            baseCookie: '1',
          });
          pokeHandler.handlePokePart({pokeID: 'poke2'});
        },
      },
      {
        name: 'pokeEnd with wrong pokeID',
        causeError: pokeHandler => {
          pokeHandler.handlePokeStart({
            pokeID: 'poke1',
            baseCookie: '1',
          });
          pokeHandler.handlePokeEnd({pokeID: 'poke2', cookie: '2'});
        },
      },
    ];
    for (const c of cases) {
      test(c.name, () => {
        const onPokeErrorStub = vi.fn();
        const replicachePokeStub = vi.fn();
        const clientID = 'c1';
        const logContext = new LogContext('error');
        const pokeHandler = new PokeHandler(
          replicachePokeStub,
          onPokeErrorStub,
          clientID,
          schema,
          logContext,
          new MutationTracker(logContext, ackMutationResponses, onFatalError),
        );

        expect(onPokeErrorStub).toHaveBeenCalledTimes(0);
        c.causeError(pokeHandler);
        expect(onPokeErrorStub).toHaveBeenCalledTimes(1);
      });
    }
  });

  test('replicachePoke throwing error calls onPokeError and clears', async () => {
    const onPokeErrorStub = vi.fn();
    const replicachePokeStub = vi.fn();
    const clientID = 'c1';
    const logContext = new LogContext('error');
    const pokeHandler = new PokeHandler(
      replicachePokeStub,
      onPokeErrorStub,
      clientID,
      schema,
      logContext,
      new MutationTracker(logContext, ackMutationResponses, onFatalError),
    );
    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke1',
      baseCookie: '1',
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 1,
        c2: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo1'},
        },
      ],
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo2'},
        },
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue2', title: 'bar1'},
        },
      ],
    });

    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeEnd({pokeID: 'poke1', cookie: '2'});

    expect(rafStub).toHaveBeenCalledTimes(1);
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);

    const {promise, reject} = resolver();
    replicachePokeStub.mockReturnValue(promise);
    expect(onPokeErrorStub).toHaveBeenCalledTimes(0);

    const rafCallback0 = rafStub.mock.calls[0][0];
    const rafCallback0Result = rafCallback0(UNUSED_RAF_ARG);

    expect(onPokeErrorStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke2',
      baseCookie: '2',
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke2',
      lastMutationIDChanges: {
        c1: 3,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue3', title: 'baz1'},
        },
      ],
    });
    pokeHandler.handlePokeEnd({
      pokeID: 'poke2',
      cookie: '3',
    });

    reject('error in poke');
    await rafCallback0Result;

    expect(replicachePokeStub).toHaveBeenCalledTimes(1);
    expect(rafStub).toHaveBeenCalledTimes(2);

    expect(onPokeErrorStub).toHaveBeenCalledTimes(1);

    const rafCallback1 = rafStub.mock.calls[1][0];
    await rafCallback1(UNUSED_RAF_ARG);
    // poke 2 cleared so replicachePokeStub not called
    expect(replicachePokeStub).toHaveBeenCalledTimes(1);
    expect(rafStub).toHaveBeenCalledTimes(2);
  });

  test('cookie gap during mergePoke calls onPokeError and clears', async () => {
    const onPokeErrorStub = vi.fn();
    const replicachePokeStub = vi.fn();
    const clientID = 'c1';
    const logContext = new LogContext('error');
    const pokeHandler = new PokeHandler(
      replicachePokeStub,
      onPokeErrorStub,
      clientID,
      schema,
      logContext,
      new MutationTracker(logContext, ackMutationResponses, onFatalError),
    );
    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke1',
      baseCookie: '1',
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 1,
        c2: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo1'},
        },
      ],
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo2'},
        },
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue2', title: 'bar1'},
        },
      ],
    });

    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeEnd({pokeID: 'poke1', cookie: '2'});

    expect(rafStub).toHaveBeenCalledTimes(1);
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke2',
      baseCookie: '3', // gap, should be 2
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke2',
      lastMutationIDChanges: {
        c1: 2,
      },
    });

    pokeHandler.handlePokeEnd({pokeID: 'poke2', cookie: '4'});

    expect(rafStub).toHaveBeenCalledTimes(1);
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);

    expect(onPokeErrorStub).toHaveBeenCalledTimes(0);

    const rafCallback0 = rafStub.mock.calls[0][0];
    const rafCallback0Result = rafCallback0(UNUSED_RAF_ARG);

    expect(onPokeErrorStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke3',
      baseCookie: '4',
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke3',
      lastMutationIDChanges: {
        c1: 3,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue3', title: 'baz1'},
        },
      ],
    });
    pokeHandler.handlePokeEnd({
      pokeID: 'poke3',
      cookie: '5',
    });
    await rafCallback0Result;

    // not called because error is in merge before call
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);
    expect(rafStub).toHaveBeenCalledTimes(2);

    expect(onPokeErrorStub).toHaveBeenCalledTimes(1);

    const rafCallback1 = rafStub.mock.calls[1][0];
    await rafCallback1(UNUSED_RAF_ARG);
    // poke 3 cleared so replicachePokeStub not called
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);
    expect(rafStub).toHaveBeenCalledTimes(2);
  });

  test('onDisconnect clears pending pokes', async () => {
    const onPokeErrorStub = vi.fn();
    const replicachePokeStub = vi.fn();
    const clientID = 'c1';
    const logContext = new LogContext('error');
    const pokeHandler = new PokeHandler(
      replicachePokeStub,
      onPokeErrorStub,
      clientID,
      schema,
      logContext,
      new MutationTracker(logContext, ackMutationResponses, onFatalError),
    );
    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke1',
      baseCookie: '1',
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 1,
        c2: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo1'},
        },
      ],
    });
    pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo2'},
        },
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue2', title: 'bar1'},
        },
      ],
    });

    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeEnd({pokeID: 'poke1', cookie: '2'});

    expect(rafStub).toHaveBeenCalledTimes(1);
    expect(replicachePokeStub).toHaveBeenCalledTimes(0);

    pokeHandler.handleDisconnect();

    const rafCallback0 = rafStub.mock.calls[0][0];
    await rafCallback0(UNUSED_RAF_ARG);

    expect(replicachePokeStub).toHaveBeenCalledTimes(0);
    expect(rafStub).toHaveBeenCalledTimes(1);
  });

  test('handlePoke returns the last mutation id change for this client from pokePart or undefined if none or error', () => {
    const onPokeErrorStub = vi.fn();
    const replicachePokeStub = vi.fn();
    const clientID = 'c1';
    const logContext = new LogContext('error');
    const pokeHandler = new PokeHandler(
      replicachePokeStub,
      onPokeErrorStub,
      clientID,
      schema,
      logContext,
      new MutationTracker(logContext, ackMutationResponses, onFatalError),
    );
    expect(rafStub).toHaveBeenCalledTimes(0);

    pokeHandler.handlePokeStart({
      pokeID: 'poke1',
      baseCookie: '1',
    });
    const lastMutationIDChangeForSelf0 = pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c1: 4,
        c2: 2,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo1'},
        },
      ],
    });
    expect(lastMutationIDChangeForSelf0).equals(4);
    const lastMutationIDChangeForSelf1 = pokeHandler.handlePokePart({
      pokeID: 'poke1',
      lastMutationIDChanges: {
        c2: 3,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo2'},
        },
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue2', title: 'bar1'},
        },
      ],
    });
    expect(lastMutationIDChangeForSelf1).toBeUndefined();
    // error wrong pokeID
    const lastMutationIDChangeForSelf2 = pokeHandler.handlePokePart({
      pokeID: 'poke2',
      lastMutationIDChanges: {
        c1: 5,
      },
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue1', title: 'foo2'},
        },
        {
          op: 'put',
          tableName: 'issues',
          value: {['issue_id']: 'issue2', title: 'bar1'},
        },
      ],
    });
    expect(lastMutationIDChangeForSelf2).toBeUndefined();
  });

  test('mergePokes with empty array returns undefined', () => {
    const merged = mergePokes([], schema, serverToClient(schema.tables));
    expect(merged).toBeUndefined();
  });

  test('mergePokes with all optionals defined', () => {
    const result = mergePokes(
      [
        {
          pokeStart: {
            pokeID: 'poke1',
            baseCookie: '3',
          },
          parts: [
            {
              pokeID: 'poke1',
              lastMutationIDChanges: {c1: 1, c2: 2},
              desiredQueriesPatches: {
                c1: [
                  {
                    op: 'put',
                    hash: 'h1',
                  },
                ],
              },
              gotQueriesPatch: [
                {
                  op: 'put',
                  hash: 'h1',
                },
              ],
              rowsPatch: [
                {
                  op: 'put',
                  tableName: 'issues',

                  value: {['issue_id']: 'issue1', title: 'foo1'},
                },
                {
                  op: 'update',
                  tableName: 'issues',
                  id: {['issue_id']: 'issue2'},
                  merge: {['issue_id']: 'issue2', title: 'bar1'},
                  constrain: ['issue_id', 'title'],
                },
              ],
            },

            {
              pokeID: 'poke1',
              lastMutationIDChanges: {c2: 3, c3: 4},
              desiredQueriesPatches: {
                c1: [
                  {
                    op: 'put',
                    hash: 'h2',
                  },
                ],
              },
              gotQueriesPatch: [
                {
                  op: 'put',
                  hash: 'h2',
                },
              ],
              rowsPatch: [
                {
                  op: 'put',
                  tableName: 'issues',
                  value: {['issue_id']: 'issue3', title: 'baz1'},
                },
              ],
            },
          ],
          pokeEnd: {
            pokeID: 'poke1',
            cookie: '4',
          },
        },
        {
          pokeStart: {
            pokeID: 'poke2',
            baseCookie: '4',
          },
          parts: [
            {
              pokeID: 'poke2',
              lastMutationIDChanges: {c4: 3},
              desiredQueriesPatches: {
                c1: [
                  {
                    op: 'del',
                    hash: 'h1',
                  },
                ],
              },
              gotQueriesPatch: [
                {
                  op: 'del',
                  hash: 'h1',
                },
              ],
              rowsPatch: [
                {op: 'del', tableName: 'issues', id: {['issue_id']: 'issue3'}},
              ],
            },
          ],
          pokeEnd: {
            pokeID: 'poke2',
            cookie: '5',
          },
        },
      ],
      schema,
      serverToClient(schema.tables),
    );

    expect(result).toEqual({
      baseCookie: '3',
      pullResponse: {
        cookie: '5',
        lastMutationIDChanges: {
          c1: 1,
          c2: 3,
          c3: 4,
          c4: 3,
        },
        patch: [
          {
            op: 'put',
            key: 'd/c1/h1',
            value: null,
          },
          {
            op: 'put',
            key: 'g/h1',
            value: null,
          },
          {
            op: 'put',
            key: 'e/issue/issue1',
            value: {id: 'issue1', title: 'foo1'},
          },
          {
            op: 'update',
            key: 'e/issue/issue2',
            merge: {id: 'issue2', title: 'bar1'},
            constrain: ['id', 'title'],
          },
          {
            op: 'put',
            key: 'd/c1/h2',
            value: null,
          },
          {
            op: 'put',
            key: 'g/h2',
            value: null,
          },
          {
            op: 'put',
            key: 'e/issue/issue3',
            value: {id: 'issue3', title: 'baz1'},
          },
          {
            op: 'del',
            key: 'd/c1/h1',
          },
          {
            op: 'del',
            key: 'g/h1',
          },
          {
            op: 'del',
            key: 'e/issue/issue3',
          },
        ],
      },
    });
  });

  test('mergePokes sparse', () => {
    const result = mergePokes(
      [
        {
          pokeStart: {
            pokeID: 'poke1',
            baseCookie: '3',
          },
          parts: [
            {
              pokeID: 'poke1',
              lastMutationIDChanges: {c1: 1, c2: 2},
              gotQueriesPatch: [
                {
                  op: 'put',
                  hash: 'h1',
                },
              ],
              rowsPatch: [
                {
                  op: 'put',
                  tableName: 'issues',

                  value: {['issue_id']: 'issue1', title: 'foo1'},
                },
                {
                  op: 'update',
                  tableName: 'issues',
                  id: {['issue_id']: 'issue2'},
                  merge: {['issue_id']: 'issue2', title: 'bar1'},
                  constrain: ['issue_id', 'title'],
                },
              ],
            },

            {
              pokeID: 'poke1',
              desiredQueriesPatches: {
                c1: [
                  {
                    op: 'put',
                    hash: 'h2',
                  },
                ],
              },
            },
          ],
          pokeEnd: {
            pokeID: 'poke1',
            cookie: '4',
          },
        },
        {
          pokeStart: {
            pokeID: 'poke2',
            baseCookie: '4',
          },
          parts: [
            {
              pokeID: 'poke2',
              desiredQueriesPatches: {
                c1: [
                  {
                    op: 'del',
                    hash: 'h1',
                  },
                ],
              },
              rowsPatch: [
                {op: 'del', tableName: 'issues', id: {['issue_id']: 'issue3'}},
              ],
            },
          ],
          pokeEnd: {
            pokeID: 'poke2',
            cookie: '5',
          },
        },
      ],
      schema,
      serverToClient(schema.tables),
    );
    expect(result).toEqual({
      baseCookie: '3',
      pullResponse: {
        lastMutationIDChanges: {
          c1: 1,
          c2: 2,
        },
        patch: [
          {
            op: 'put',
            key: 'g/h1',
            value: null,
          },
          {
            op: 'put',
            key: 'e/issue/issue1',
            value: {id: 'issue1', title: 'foo1'},
          },
          {
            op: 'update',
            key: 'e/issue/issue2',
            merge: {id: 'issue2', title: 'bar1'},
            constrain: ['id', 'title'],
          },
          {
            op: 'put',
            key: 'd/c1/h2',
            value: null,
          },
          {
            op: 'del',
            key: 'd/c1/h1',
          },
          {
            op: 'del',
            key: 'e/issue/issue3',
          },
        ],
        cookie: '5',
      },
    });
  });

  test('mergePokes throws error on cookie gaps', () => {
    expect(() => {
      mergePokes(
        [
          {
            pokeStart: {
              pokeID: 'poke1',
              baseCookie: '3',
            },
            parts: [
              {
                pokeID: 'poke1',
                lastMutationIDChanges: {c1: 1, c2: 2},
              },
            ],
            pokeEnd: {
              pokeID: 'poke1',
              cookie: '4',
            },
          },
          {
            pokeStart: {
              pokeID: 'poke2',
              baseCookie: '5', // gap, should be 4
            },
            parts: [
              {
                pokeID: 'poke2',
                lastMutationIDChanges: {c4: 3},
              },
            ],
            pokeEnd: {
              pokeID: 'poke2',
              cookie: '6',
            },
          },
        ],
        schema,
        serverToClient(schema.tables),
      );
    }).toThrow();
  });
});

describe('mutation tracker interactions', () => {
  beforeEach(() => {
    rafStub = vi
      .spyOn(globalThis, 'requestAnimationFrame')
      .mockImplementation(c => {
        setTimeout(() => {
          c(UNUSED_RAF_ARG);
        }, 0);
        return 1;
      });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  test("poke handler advances mutation tracker's lmid", async () => {
    const onPokeErrorStub = vi.fn();
    const replicachePokeStub = vi.fn();
    const clientID = 'c1';
    const logContext = new LogContext('error');
    const tracker = new MutationTracker(
      logContext,
      ackMutationResponses,
      onFatalError,
    );
    const spy = vi.spyOn(tracker, 'lmidAdvanced');
    tracker.setClientIDAndWatch(clientID, () => () => {});
    const pokeHandler = new PokeHandler(
      replicachePokeStub,
      onPokeErrorStub,
      clientID,
      schema,
      logContext,
      tracker,
    );

    let id = 0;
    function doPoke(mids: Record<string, number> | undefined) {
      ++id;
      pokeHandler.handlePokeStart({
        pokeID: 'poke' + id,
        baseCookie: '1',
      });
      pokeHandler.handlePokePart({
        pokeID: 'poke' + id,
        lastMutationIDChanges: mids,
      });
      pokeHandler.handlePokeEnd({pokeID: 'poke' + id, cookie: '2'});
    }

    doPoke(undefined);
    expect(spy).not.toHaveBeenCalled();

    doPoke({c2: 2});
    expect(spy).not.toHaveBeenCalled();

    doPoke({c1: 2, c2: 3, c3: 4});
    await vi.waitFor(() => {
      expect(spy).toHaveBeenCalledTimes(1);
      expect(spy).toHaveBeenCalledWith(2);
    });
  });

  test('poke handler pokes replicache with mutation results', async () => {
    const onPokeErrorStub = vi.fn();
    const replicachePokeStub = vi.fn();
    const clientID = 'c1';
    const logContext = new LogContext('error');
    const tracker = new MutationTracker(
      logContext,
      ackMutationResponses,
      onFatalError,
    );

    tracker.setClientIDAndWatch(clientID, () => () => {});
    const pokeHandler = new PokeHandler(
      replicachePokeStub,
      onPokeErrorStub,
      clientID,
      schema,
      logContext,
      tracker,
    );

    let id = 0;
    function doPoke(
      mids: Record<string, number> | undefined,
      mutationsPatch: MutationPatch[] | undefined,
    ) {
      ++id;
      pokeHandler.handlePokeStart({
        pokeID: 'poke' + id,
        baseCookie: '1',
      });
      pokeHandler.handlePokePart({
        pokeID: 'poke' + id,
        lastMutationIDChanges: mids,
        mutationsPatch,
      });
      pokeHandler.handlePokeEnd({pokeID: 'poke' + id, cookie: '2'});
    }

    doPoke(undefined, undefined);

    doPoke({c2: 2}, undefined);

    doPoke({c2: 2}, [
      {
        mutation: {
          id: {id: 1, clientID: 'c1'},
          result: {},
        },
        op: 'put',
      },
    ]);
    await vi.waitFor(() => {
      expect(replicachePokeStub).toHaveBeenCalledTimes(1);
      expect(replicachePokeStub).toHaveBeenCalledWith({
        baseCookie: '1',
        pullResponse: {
          cookie: '2',
          lastMutationIDChanges: {
            c2: 2,
          },
          patch: [
            {
              key: 'm/c1/1',
              op: 'put',
              value: {},
            },
          ],
        },
      });
    });
  });
});
