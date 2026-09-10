import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../../shared/src/must.ts';
import {Queue} from '../../../../../shared/src/queue.ts';
import {sleep} from '../../../../../shared/src/sleep.ts';
import {JSON_PARSED} from '../../../types/lite.ts';
import {
  majorVersionFromString,
  majorVersionToString,
} from '../../../types/state-version.ts';
import type {
  BackfillCompleted,
  BackfillRequest,
  BackfillRequestMessage,
  BackfillStarted,
  ChangeStreamMessage,
  Mark,
  MessageBackfill,
} from '../protocol/current.ts';
import {
  BackfillManager,
  type BackfillMessage,
  type RowsExist,
} from './backfill-manager.ts';
import {ChangeStreamMultiplexer} from './change-stream-multiplexer.ts';

type TestStreamItem =
  | BackfillStarted
  | MessageBackfill
  | BackfillCompleted
  | BackfillMessage
  // Holds the stream open, so that a test can act on a run that is still
  // running rather than one that has already finished and been replaced.
  | {hold: Promise<void>}
  // Fails the stream here, after everything it has already yielded.
  | Error;

describe('backfill-manager', () => {
  let backfillManager: BackfillManager;
  let changeStream: ChangeStreamMultiplexer;
  let backfillRequests: BackfillRequest[];
  let testStreams: (TestStreamItem[] | Error)[];
  let changes: Queue<ChangeStreamMessage>;
  let finalizedStreams: number;
  let lc: LogContext;

  function initBackfillManager(commitThresholdBytes?: number) {
    changeStream = new ChangeStreamMultiplexer(lc, '123');
    backfillManager = new BackfillManager(
      lc,
      changeStream,
      backfillStreamer,
      rowsExist,
      JSON_PARSED,
      10,
      50,
      commitThresholdBytes,
    );
    changeStream.addProducers(backfillManager).addListeners(backfillManager);
    changes = new Queue<ChangeStreamMessage>();

    // Drain ChangeStreamMessages to the changes queue.
    void (async () => {
      for await (const msg of changeStream.asSource()) {
        changes.enqueue(msg);
      }
    })();
  }

  /**
   * Stands in for the `rowsExist` query. The default answer, "yes", is the
   * conservative one: it restarts a run rather than claiming a subscriber is
   * covered.
   */
  let rowsExistAnswers: boolean[];
  const rowsExistCalls: {from: Mark | null; to: Mark}[] = [];

  const rowsExist: RowsExist = (_req, from, to) => {
    rowsExistCalls.push({from, to});
    return Promise.resolve(rowsExistAnswers.shift() ?? true);
  };

  beforeEach(() => {
    lc = createSilentLogContext();
    backfillRequests = [];
    testStreams = [];
    finalizedStreams = 0;
    rowsExistAnswers = [];
    rowsExistCalls.length = 0;
    initBackfillManager();
  });

  async function* backfillStreamer(
    req: BackfillRequest,
  ): AsyncGenerator<BackfillMessage> {
    lc.debug?.(`starting test backfill stream for`, req);
    backfillRequests.push(req);
    const stream = must(
      testStreams.shift(),
      `No more testStreams configured by test`,
    );

    if (stream instanceof Error) {
      throw stream; // For testing backfill errors
    }

    try {
      for (const item of stream) {
        if (item instanceof Error) {
          throw item;
        }
        if ('hold' in item) {
          await item.hold;
          continue;
        }
        yield 'message' in item ? item : {message: item, byteSize: 0};
      }
    } finally {
      // Tracks that the stream was finalized, i.e. that the consumer either
      // exhausted it or exited early (via `return()`), which is what releases
      // the upstream resources held by real backfill streams.
      finalizedStreams++;
    }
  }

  async function drainChanges(n: number): Promise<ChangeStreamMessage[]> {
    const c: ChangeStreamMessage[] = [];
    for (let i = 0; i < n; i++) {
      c.push(await changes.dequeue());
    }
    return c;
  }

  async function expectChanges(changes: ChangeStreamMessage[]) {
    expect(await drainChanges(changes.length)).toMatchObject(changes);
  }

  test('backfill initiated by change-streamer request', async () => {
    testStreams.push([
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [
          [1, 2],
          [3, 4],
        ],
      },
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [
          [5, 6],
          [7, 8],
        ],
      },
      {
        tag: 'backfill-completed',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        columns: ['b'],
        watermark: '130',
      },
    ]);

    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);

    changeStream.pushStatus(['status', {ack: false}, {watermark: '130'}]);

    await expectChanges([
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '123.01'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          rowValues: [
            [1, 2],
            [3, 4],
          ],
          tag: 'backfill',
          watermark: '130',
        },
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          rowValues: [
            [5, 6],
            [7, 8],
          ],
          tag: 'backfill',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '123.01'}],
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '130'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          tag: 'backfill-completed',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '130'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);
  });

  test('commits a transaction when the byte threshold is reached', async () => {
    // Small threshold so that two 60-byte messages (120 bytes) cross it, but a
    // single one (60 bytes) does not.
    initBackfillManager(100);

    const relation = {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}};
    testStreams.push([
      {
        message: {
          tag: 'backfill',
          relation,
          watermark: '130',
          columns: ['b'],
          rowValues: [[1, 2]],
        },
        byteSize: 60,
      },
      {
        message: {
          tag: 'backfill',
          relation,
          watermark: '130',
          columns: ['b'],
          rowValues: [[3, 4]],
        },
        byteSize: 60,
      },
      {
        message: {
          tag: 'backfill',
          relation,
          watermark: '130',
          columns: ['b'],
          rowValues: [[5, 6]],
        },
        byteSize: 60,
      },
      {
        tag: 'backfill-completed',
        relation,
        columns: ['b'],
        watermark: '130',
      },
    ]);

    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {metadata: {rowKey: {a: 123}}, name: 'bar', schema: 'foo'},
      },
    ]);

    changeStream.pushStatus(['status', {ack: false}, {watermark: '130'}]);

    const data = (rowValues: number[][]) =>
      [
        'data',
        {
          tag: 'backfill',
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          watermark: '130',
          rowValues,
        },
      ] satisfies ChangeStreamMessage;

    await expectChanges([
      // First transaction: accumulates two messages (120 bytes >= 100)...
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '123.01'},
      ],
      data([[1, 2]]),
      data([[3, 4]]),
      // ...then the threshold forces a commit before the third message.
      ['commit', {tag: 'commit'}, {watermark: '123.01'}],
      // Second transaction: the third message opens a fresh transaction.
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '123.02'},
      ],
      data([[5, 6]]),
      // Committed to reach the backfill watermark before completing.
      ['commit', {tag: 'commit'}, {watermark: '123.02'}],
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '130'},
      ],
      [
        'data',
        {
          tag: 'backfill-completed',
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '130'}],
    ] satisfies ChangeStreamMessage[]);
  });

  test('table backfill initiated by create-table', async () => {
    testStreams.push([
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [
          [1, 2],
          [3, 4],
        ],
      },
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [
          [5, 6],
          [7, 8],
        ],
      },
      {
        tag: 'backfill-completed',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        columns: ['b'],
        watermark: '130',
      },
    ]);

    backfillManager.run('123', []);

    await changeStream.reserve('main');
    for (const msg of [
      ['begin', {tag: 'begin'}, {commitWatermark: '125'}],
      [
        'data',
        {
          tag: 'create-table',
          spec: {
            schema: 'foo',
            name: 'bar',
            columns: {
              a: {dataType: 'text', pos: 0},
              b: {dataType: 'text', pos: 1},
            },
          },
          metadata: {rowKey: {a: 123}},
          backfill: {
            a: {id: '123'},
            b: {id: '234'},
          },
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125'}],
    ] satisfies ChangeStreamMessage[]) {
      void changeStream.push(msg);
    }
    changeStream.release('125');

    changeStream.pushStatus(['status', {ack: false}, {watermark: '130'}]);

    await expectChanges([
      ['begin', {tag: 'begin'}, {commitWatermark: '125'}],
      [
        'data',
        {
          backfill: {a: {id: '123'}, b: {id: '234'}},
          metadata: {rowKey: {a: 123}},
          spec: {
            columns: {
              a: {dataType: 'text', pos: 0},
              b: {dataType: 'text', pos: 1},
            },
            name: 'bar',
            schema: 'foo',
          },
          tag: 'create-table',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125'}],
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '125.01'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          rowValues: [
            [1, 2],
            [3, 4],
          ],
          tag: 'backfill',
          watermark: '130',
        },
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          rowValues: [
            [5, 6],
            [7, 8],
          ],
          tag: 'backfill',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125.01'}],
      ['begin', {tag: 'begin'}, {commitWatermark: '130'}],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          tag: 'backfill-completed',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '130'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);
  });

  test('column backfill initiated by add-column', async () => {
    testStreams.push([
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [
          [1, 2],
          [3, 4],
        ],
      },
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [
          [5, 6],
          [7, 8],
        ],
      },
      {
        tag: 'backfill-completed',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        columns: ['b'],
        watermark: '130',
      },
    ]);

    backfillManager.run('123', []);

    await changeStream.reserve('main');
    for (const msg of [
      ['begin', {tag: 'begin'}, {commitWatermark: '130'}],
      [
        'data',
        {
          tag: 'add-column',
          table: {
            schema: 'foo',
            name: 'bar',
          },
          column: {name: 'b', spec: {dataType: 'text', pos: 1}},
          tableMetadata: {rowKey: {a: 123}},
          backfill: {id: '789'},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '130'}],
    ] satisfies ChangeStreamMessage[]) {
      void changeStream.push(msg);
    }
    changeStream.release('130');

    expect(await drainChanges(8)).toMatchObject([
      ['begin', {tag: 'begin'}, {commitWatermark: '130'}],
      [
        'data',
        {
          backfill: {id: '789'},
          tableMetadata: {rowKey: {a: 123}},
          table: {
            name: 'bar',
            schema: 'foo',
          },
          column: {name: 'b', spec: {dataType: 'text', pos: 1}},
          tag: 'add-column',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '130'}],
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '130.01'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          rowValues: [
            [1, 2],
            [3, 4],
          ],
          tag: 'backfill',
          watermark: '130',
        },
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          rowValues: [
            [5, 6],
            [7, 8],
          ],
          tag: 'backfill',
          watermark: '130',
        },
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          tag: 'backfill-completed',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '130.01'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      {
        columns: {b: {id: '789'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);
  });

  test('backfill metadata updated by schema changes', async () => {
    testStreams.push([
      {
        tag: 'backfill-completed',
        relation: {schema: 'boo', name: 'far', rowKey: {columns: ['z']}},
        columns: ['d', 'c'],
        watermark: '130',
      },
    ]);
    backfillManager.run('123', []);

    await changeStream.reserve('main');
    for (const msg of [
      ['begin', {tag: 'begin'}, {commitWatermark: '125'}],
      [
        'data',
        {
          tag: 'create-table',
          spec: {
            schema: 'zoo',
            name: 'dar',
            columns: {
              a: {dataType: 'text', pos: 0},
              b: {dataType: 'text', pos: 1},
            },
          },
          metadata: {rowKey: {a: 123}},
          backfill: {
            a: {id: '123'},
            b: {id: '234'},
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-table',
          spec: {
            schema: 'foo',
            name: 'bar',
            columns: {
              a: {dataType: 'text', pos: 0},
              b: {dataType: 'text', pos: 1},
            },
          },
          metadata: {rowKey: {a: 123}},
          backfill: {
            a: {id: '123'},
            b: {id: '234'},
          },
        },
      ],
      [
        'data',
        {
          tag: 'add-column',
          table: {schema: 'foo', name: 'bar'},
          column: {name: 'e', spec: {dataType: 'text', pos: 1}},
          tableMetadata: {rowKey: {a: 123}},
          backfill: {id: '999'},
        },
      ],
      [
        'data',
        {
          tag: 'update-column',
          table: {schema: 'foo', name: 'bar'},
          old: {name: 'b', spec: {dataType: 'text', pos: 1}},
          new: {name: 'd', spec: {dataType: 'text', pos: 1}},
        },
      ],
      [
        'data',
        {
          tag: 'add-column',
          table: {schema: 'foo', name: 'bar'},
          column: {name: 'c', spec: {dataType: 'text', pos: 1}},
          tableMetadata: {rowKey: {a: 123}},
          backfill: {id: '765'},
        },
      ],
      [
        'data',
        {
          tag: 'drop-column',
          table: {schema: 'foo', name: 'bar'},
          column: 'e',
        },
      ],
      [
        'data',
        {
          tag: 'update-column',
          table: {schema: 'foo', name: 'bar'},
          old: {name: 'a', spec: {dataType: 'text', pos: 1}},
          new: {name: 'z', spec: {dataType: 'text', pos: 1}},
        },
      ],
      [
        'data',
        {
          tag: 'update-table-metadata',
          table: {schema: 'foo', name: 'bar'},
          old: {rowKey: {a: 123}},
          new: {rowKey: {z: 123}},
        },
      ],
      [
        'data',
        {
          tag: 'rename-table',
          old: {schema: 'foo', name: 'bar'},
          new: {schema: 'boo', name: 'far'},
        },
      ],
      [
        'data',
        {
          tag: 'drop-table',
          id: {schema: 'zoo', name: 'dar'},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125'}],
    ] satisfies ChangeStreamMessage[]) {
      void changeStream.push(msg);
    }
    changeStream.release('125');
    changeStream.pushStatus(['status', {ack: false}, {watermark: '130'}]);

    expect((await drainChanges(15)).slice(-3)).toMatchObject([
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '130'},
      ],
      [
        'data',
        {
          columns: ['d', 'c'],
          relation: {
            name: 'far',
            rowKey: {columns: ['z']},
            schema: 'boo',
          },
          tag: 'backfill-completed',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '130'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      {
        columns: {
          c: {id: '765'},
          d: {id: '234'},
          z: {id: '123'},
        },
        table: {
          metadata: {rowKey: {z: 123}},
          name: 'far',
          schema: 'boo',
        },
      },
    ]);
  });

  test('backfill canceled and retried because of column drop', async () => {
    testStreams.push(
      [
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['b'],
          watermark: '120',
        },
      ],
      [
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: [],
          watermark: '130',
        },
      ],
    );
    await changeStream.reserve('main');

    // Backfill manager will start the first request and block on the
    // 'main' change-stream reservation.
    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
      },
    ]);

    // In the meantime, a column gets dropped on the main stream.
    for (const msg of [
      ['begin', {tag: 'begin'}, {commitWatermark: '125'}],
      [
        'data',
        {
          tag: 'drop-column',
          table: {schema: 'foo', name: 'bar'},
          column: 'b',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125'}],
    ] satisfies ChangeStreamMessage[]) {
      void changeStream.push(msg);
    }
    changeStream.release('125');
    changeStream.pushStatus(['status', {ack: false}, {watermark: '130'}]);

    // The first request is canceled and only the changes from
    // the updated request are streamed.
    await expectChanges([
      ['begin', {tag: 'begin'}, {commitWatermark: '125'}],
      [
        'data',
        {
          tag: 'drop-column',
          table: {schema: 'foo', name: 'bar'},
          column: 'b',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125'}],
      ['begin', {tag: 'begin'}, {commitWatermark: '130'}],
      [
        'data',
        {
          tag: 'backfill-completed',
          relation: {
            schema: 'foo',
            name: 'bar',
            rowKey: {columns: ['a']},
          },
          columns: [],
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '130'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      // Canceled request
      {
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
        columns: {a: {id: '123'}, b: {id: '234'}},
      },
      // Updated request
      {
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
        columns: {a: {id: '123'}},
      },
    ]);
  });

  test('backfill canceled and retried because of table rename', async () => {
    testStreams.push(
      [
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['a', 'b'],
          watermark: '120',
        },
      ],
      [
        {
          tag: 'backfill-completed',
          relation: {schema: 'boo', name: 'far', rowKey: {columns: ['a']}},
          columns: ['a', 'b'],
          watermark: '130',
        },
      ],
    );
    await changeStream.reserve('main');

    // Backfill manager will start the first request and block on the
    // 'main' change-stream reservation.
    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
      },
    ]);

    // In the meantime, the table gets renamed on the main stream.
    for (const msg of [
      ['begin', {tag: 'begin'}, {commitWatermark: '125'}],
      [
        'data',
        {
          tag: 'rename-table',
          old: {schema: 'foo', name: 'bar'},
          new: {schema: 'boo', name: 'far'},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125'}],
    ] satisfies ChangeStreamMessage[]) {
      void changeStream.push(msg);
    }
    changeStream.release('125');
    changeStream.pushStatus(['status', {ack: false}, {watermark: '130'}]);

    // The first request is canceled and only the changes from
    // the updated request are streamed.
    await expectChanges([
      ['begin', {tag: 'begin'}, {commitWatermark: '125'}],
      [
        'data',
        {
          tag: 'rename-table',
          old: {schema: 'foo', name: 'bar'},
          new: {schema: 'boo', name: 'far'},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125'}],
      ['begin', {tag: 'begin'}, {commitWatermark: '130'}],
      [
        'data',
        {
          tag: 'backfill-completed',
          columns: ['a', 'b'],
          relation: {
            schema: 'boo',
            name: 'far',
            rowKey: {columns: ['a']},
          },
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '130'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      // Canceled request
      {
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
        columns: {a: {id: '123'}, b: {id: '234'}},
      },
      // Updated request
      {
        table: {
          schema: 'boo',
          name: 'far',
          metadata: {rowKey: {a: 123}},
        },
        columns: {a: {id: '123'}, b: {id: '234'}},
      },
    ]);
  });

  test('backfill canceled and retried because of table metadata update', async () => {
    testStreams.push(
      [
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['a', 'b'],
          watermark: '120',
        },
      ],
      [
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['b']}},
          columns: ['a', 'b'],
          watermark: '130',
        },
      ],
    );
    await changeStream.reserve('main');

    // Backfill manager will start the first request and block on the
    // 'main' change-stream reservation.
    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
      },
    ]);

    // In the meantime, the the row key in table metadata is changed
    // on the main stream.
    for (const msg of [
      ['begin', {tag: 'begin'}, {commitWatermark: '125'}],
      [
        'data',
        {
          tag: 'update-table-metadata',
          table: {schema: 'foo', name: 'bar'},
          old: {rowKey: {a: 123}},
          new: {rowKey: {b: 234}},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125'}],
    ] satisfies ChangeStreamMessage[]) {
      void changeStream.push(msg);
    }
    changeStream.release('125');
    changeStream.pushStatus(['status', {ack: false}, {watermark: '130'}]);

    // The first request is canceled and only the changes from
    // the updated request are streamed.
    await expectChanges([
      ['begin', {tag: 'begin'}, {commitWatermark: '125'}],
      [
        'data',
        {
          tag: 'update-table-metadata',
          table: {schema: 'foo', name: 'bar'},
          old: {rowKey: {a: 123}},
          new: {rowKey: {b: 234}},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125'}],
      ['begin', {tag: 'begin'}, {commitWatermark: '130'}],
      [
        'data',
        {
          tag: 'backfill-completed',
          columns: ['a', 'b'],
          relation: {
            schema: 'foo',
            name: 'bar',
            rowKey: {columns: ['b']},
          },
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '130'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      // Canceled request
      {
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
        columns: {a: {id: '123'}, b: {id: '234'}},
      },
      // Updated request
      {
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {b: 234}},
        },
        columns: {a: {id: '123'}, b: {id: '234'}},
      },
    ]);
  });

  test("backfill canceled and retried because a row's key is updated", async () => {
    testStreams.push(
      [
        {
          tag: 'backfill',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['a', 'b'],
          watermark: '130',
          rowValues: [
            [
              [1, 2],
              [3, 4],
            ],
          ],
        },
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['a', 'b'],
          watermark: '120',
        },
      ],
      [
        {
          tag: 'backfill',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['a', 'b'],
          watermark: '150',
          rowValues: [
            [
              [5, 6],
              [3, 4],
            ],
          ],
        },
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['a', 'b'],
          watermark: '130',
        },
      ],
    );
    await changeStream.reserve('main');

    // Backfill manager will start the first request block on the
    // 'main' change-stream reservation.
    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
      },
    ]);

    // In the meantime, the one of the rows updates its key.
    for (const msg of [
      ['begin', {tag: 'begin'}, {commitWatermark: '140'}],
      [
        'data',
        {
          tag: 'update',
          relation: {
            schema: 'foo',
            name: 'bar',
            rowKey: {columns: ['a']},
          },
          key: {a: 1},
          new: {a: 5},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '140'}],
    ] satisfies ChangeStreamMessage[]) {
      void changeStream.push(msg);
    }
    changeStream.release('140');

    // The first request is canceled and only the changes from
    // the updated request are streamed.
    expect(await drainChanges(7)).toMatchObject([
      ['begin', {tag: 'begin'}, {commitWatermark: '140'}],
      [
        'data',
        {
          tag: 'update',
          relation: {
            schema: 'foo',
            name: 'bar',
            rowKey: {columns: ['a']},
          },
          key: {a: 1},
          new: {a: 5},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '140'}],
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '140.01'},
      ],
      [
        'data',
        {
          tag: 'backfill',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['a', 'b'],
          watermark: '150',
          rowValues: [
            [
              [5, 6],
              [3, 4],
            ],
          ],
        },
      ],
      [
        'data',
        {
          tag: 'backfill-completed',
          columns: ['a', 'b'],
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '140.01'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      // Canceled request
      {
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
        columns: {a: {id: '123'}, b: {id: '234'}},
      },
      // Retry
      {
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
        columns: {a: {id: '123'}, b: {id: '234'}},
      },
    ]);
  });

  test('backfill canceled because of table drop', async () => {
    testStreams.push([
      {
        tag: 'backfill-completed',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        columns: ['a', 'b'],
        watermark: '120',
      },
    ]);
    await changeStream.reserve('main');

    // Backfill manager will start the first request and block on the
    // 'main' change-stream reservation.
    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
      },
    ]);

    // In the meantime, the table gets renamed on the main stream.
    for (const msg of [
      ['begin', {tag: 'begin'}, {commitWatermark: '125'}],
      [
        'data',
        {
          tag: 'drop-table',
          id: {schema: 'foo', name: 'bar'},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125'}],
    ] satisfies ChangeStreamMessage[]) {
      void changeStream.push(msg);
    }
    changeStream.release('125');

    // The backfill request is canceled
    expect(await drainChanges(3)).toMatchObject([
      ['begin', {tag: 'begin'}, {commitWatermark: '125'}],
      [
        'data',
        {
          tag: 'drop-table',
          id: {schema: 'foo', name: 'bar'},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      // Canceled request
      {
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
        columns: {a: {id: '123'}, b: {id: '234'}},
      },
    ]);
  });

  test('backfill canceled because of last column drop', async () => {
    testStreams.push([
      {
        tag: 'backfill-completed',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        columns: ['b'],
        watermark: '120',
      },
    ]);
    await changeStream.reserve('main');

    // Backfill manager will start the first request and block on the
    // 'main' change-stream reservation.
    backfillManager.run('123', [
      {
        columns: {b: {id: '234'}},
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
      },
    ]);

    // In the meantime, the table gets renamed on the main stream.
    for (const msg of [
      ['begin', {tag: 'begin'}, {commitWatermark: '125'}],
      [
        'data',
        {
          tag: 'drop-column',
          table: {schema: 'foo', name: 'bar'},
          column: 'b',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125'}],
    ] satisfies ChangeStreamMessage[]) {
      void changeStream.push(msg);
    }
    changeStream.release('125');

    // The backfill request is canceled
    expect(await drainChanges(3)).toMatchObject([
      ['begin', {tag: 'begin'}, {commitWatermark: '125'}],
      [
        'data',
        {
          tag: 'drop-column',
          table: {schema: 'foo', name: 'bar'},
          column: 'b',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '125'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      // Canceled request, and no subsequent attempts
      {
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
        columns: {b: {id: '234'}},
      },
    ]);
  });

  test('column added to backfilling table', async () => {
    testStreams.push(
      [
        {
          tag: 'backfill',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['a', 'b'],
          watermark: '130',
          rowValues: [
            [
              [1, 2],
              [3, 4],
            ],
          ],
        },
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['a', 'b'],
          watermark: '130',
        },
      ],
      [
        {
          tag: 'backfill',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['c', 'd'],
          watermark: '150',
          rowValues: [
            [
              [5, 6],
              [3, 4],
            ],
          ],
        },
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['c', 'd'],
          watermark: '150',
        },
      ],
    );
    await changeStream.reserve('main');

    // Backfill manager will start the first request block on the
    // 'main' change-stream reservation.
    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
      },
    ]);

    // In the meantime, more columns get added to the table.
    for (const msg of [
      ['begin', {tag: 'begin'}, {commitWatermark: '140'}],
      [
        'data',
        {
          tag: 'add-column',
          table: {schema: 'foo', name: 'bar'},
          column: {name: 'c', spec: {dataType: 'text', pos: 2}},
          tableMetadata: {rowKey: {a: 123}},
          backfill: {id: '777'},
        },
      ],
      [
        'data',
        {
          tag: 'add-column',
          table: {schema: 'foo', name: 'bar'},
          column: {name: 'd', spec: {dataType: 'text', pos: 2}},
          tableMetadata: {rowKey: {a: 123}},
          backfill: {id: '888'},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '140'}],
    ] satisfies ChangeStreamMessage[]) {
      void changeStream.push(msg);
    }
    changeStream.release('140');
    changeStream.pushStatus(['status', {ack: false}, {watermark: '150'}]);

    await expectChanges([
      ['begin', {tag: 'begin'}, {commitWatermark: '140'}],
      [
        'data',
        {
          tag: 'add-column',
          table: {schema: 'foo', name: 'bar'},
          column: {name: 'c', spec: {dataType: 'text', pos: 2}},
          tableMetadata: {rowKey: {a: 123}},
          backfill: {id: '777'},
        },
      ],
      [
        'data',
        {
          tag: 'add-column',
          table: {schema: 'foo', name: 'bar'},
          column: {name: 'd', spec: {dataType: 'text', pos: 2}},
          tableMetadata: {rowKey: {a: 123}},
          backfill: {id: '888'},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '140'}],
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '140.01'},
      ],
      [
        'data',
        {
          tag: 'backfill',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['a', 'b'],
          watermark: '130',
          rowValues: [
            [
              [1, 2],
              [3, 4],
            ],
          ],
        },
      ],
      [
        'data',
        {
          tag: 'backfill-completed',
          columns: ['a', 'b'],
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '140.01'}],
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '140.02'},
      ],
      [
        'data',
        {
          tag: 'backfill',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['c', 'd'],
          watermark: '150',
          rowValues: [
            [
              [5, 6],
              [3, 4],
            ],
          ],
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '140.02'}],
      ['begin', {tag: 'begin'}, {commitWatermark: '150'}],
      [
        'data',
        {
          tag: 'backfill-completed',
          columns: ['c', 'd'],
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          watermark: '150',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '150'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      // First request
      {
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
        columns: {a: {id: '123'}, b: {id: '234'}},
      },
      // More columns
      {
        table: {
          schema: 'foo',
          name: 'bar',
          metadata: {rowKey: {a: 123}},
        },
        columns: {c: {id: '777'}, d: {id: '888'}},
      },
    ]);
  });

  test('backfill retried on stream error', async () => {
    testStreams.push(
      new Error('failure 1'),
      new Error('failure 2'),
      new Error('failure 3'),
      [
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          columns: ['b'],
          watermark: '130',
        },
      ],
    );

    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);

    changeStream.pushStatus(['status', {ack: false}, {watermark: '130'}]);

    await expectChanges([
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '130'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          tag: 'backfill-completed',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '130'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);
  });

  // A stream that fails mid-transaction held the change stream's reservation
  // with its transaction open, so no producer could ever reserve it again.
  test('a stream that fails mid-transaction rolls back and releases the change stream', async () => {
    const relation = {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}};
    testStreams.push(
      [
        {
          tag: 'backfill-started',
          relation,
          columns: ['b'],
          watermark: '130',
          runID: 'run-1',
          resumeFrom: null,
        },
        {
          tag: 'backfill',
          relation,
          columns: ['b'],
          watermark: '130',
          rowValues: [[1, 'x']],
          runID: 'run-1',
        },
        new Error('lost the COPY connection'),
      ],
      [
        {
          tag: 'backfill-completed',
          relation,
          columns: ['b'],
          watermark: '130',
        },
      ],
    );

    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {metadata: {rowKey: {a: 123}}, name: 'bar', schema: 'foo'},
      },
    ]);
    changeStream.pushStatus(['status', {ack: false}, {watermark: '130'}]);

    // The failed run's transaction is rolled back, and the retry can reserve
    // the change stream for its own.
    await expectChanges([
      ['begin', {tag: 'begin', backfill: true}, expect.anything()],
      ['data', {tag: 'backfill-started'}],
      ['data', {tag: 'backfill'}],
      ['rollback', {tag: 'rollback'}],
      ['begin', {tag: 'begin', backfill: true}, expect.anything()],
      ['data', {tag: 'backfill-completed'}],
      ['commit', {tag: 'commit'}, expect.anything()],
    ] as ChangeStreamMessage[]);
  });

  test('backfill retried for non-empty table without row key', async () => {
    testStreams.push(
      [
        {
          tag: 'backfill',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: []}},
          columns: ['id'],
          watermark: '150',
          rowValues: [[1]],
        },
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: []}},
          columns: ['id'],
          watermark: '150',
        },
      ],
      [
        {
          tag: 'backfill',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: []}},
          columns: ['id'],
          watermark: '150',
          rowValues: [[1]],
        },
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: []}},
          columns: ['id'],
          watermark: '150',
        },
      ],
      // This time the table has a row key.
      [
        {
          tag: 'backfill',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['id']}},
          columns: [],
          watermark: '188',
          rowValues: [[1]],
        },
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['id']}},
          columns: [],
          watermark: '188',
        },
      ],
    );

    backfillManager.run('123', [
      {
        columns: {id: {id: '123'}},
        table: {
          metadata: {rowKey: {}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);

    changeStream.pushStatus(['status', {ack: false}, {watermark: '188'}]);

    await expectChanges([
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '123.01'},
      ],
      [
        'data',
        {
          tag: 'backfill',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['id']}},
          columns: [],
          watermark: '188',
          rowValues: [[1]],
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '123.01'}],
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '188'},
      ],
      [
        'data',
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['id']}},
          columns: [],
          watermark: '188',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '188'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      {
        columns: {id: {id: '123'}},
        table: {
          metadata: {rowKey: {}},
          name: 'bar',
          schema: 'foo',
        },
      },
      {
        columns: {id: {id: '123'}},
        table: {
          metadata: {rowKey: {}},
          name: 'bar',
          schema: 'foo',
        },
      },
      {
        columns: {id: {id: '123'}},
        table: {
          metadata: {rowKey: {}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);
  });

  test('backfill stream yields to other stream reservations', async () => {
    testStreams.push([
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [[1, 2]],
      },
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [[2, 3]],
      },
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [[3, 4]],
      },
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [[5, 6]],
      },
      {
        tag: 'backfill-completed',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        columns: ['b'],
        watermark: '130',
      },
    ]);
    await changeStream.reserve('main');

    // Start the backfill with the table already reserved.
    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);

    changeStream.pushStatus(['status', {ack: false}, {watermark: '130'}]);

    // Repeatedly reserve and hold the reservation for 2 ms.
    void (async function () {
      let ver = majorVersionFromString('140');
      for (let i = 0; i < 6; i++, ver++) {
        changeStream.release(majorVersionToString(ver));
        await changeStream.reserve('main');
        await sleep(50);
      }
    })();

    // Each 'backfill' message is wrapped in a separate transaction because
    // the backfill-manager yielded the stream between every message.
    expect(await drainChanges(15)).toMatchObject([
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '141.01'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {
            name: 'bar',
            rowKey: {columns: ['a']},
            schema: 'foo',
          },
          rowValues: [[1, 2]],
          tag: 'backfill',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '141.01'}],
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '142.01'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {
            name: 'bar',
            rowKey: {columns: ['a']},
            schema: 'foo',
          },
          rowValues: [[2, 3]],
          tag: 'backfill',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '142.01'}],
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '143.01'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {
            name: 'bar',
            rowKey: {columns: ['a']},
            schema: 'foo',
          },
          rowValues: [[3, 4]],
          tag: 'backfill',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '143.01'}],
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '144.01'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {
            name: 'bar',
            rowKey: {columns: ['a']},
            schema: 'foo',
          },
          rowValues: [[5, 6]],
          tag: 'backfill',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '144.01'}],
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '145.01'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {
            name: 'bar',
            rowKey: {columns: ['a']},
            schema: 'foo',
          },
          tag: 'backfill-completed',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '145.01'}],
    ]);
  });

  test('backfill-completed waits for commit to exceed backfill watermark', async () => {
    testStreams.push([
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [
          [5, 6],
          [7, 8],
        ],
      },
      {
        tag: 'backfill-completed',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        columns: ['b'],
        watermark: '130',
      },
    ]);

    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);

    // Let the backfill start streaming before acquiring a reservation
    // for the main stream.
    await sleep(100);

    // Move the main replication stream past the backfill LSN
    await changeStream.reserve('main');
    for (const msg of [
      ['begin', {tag: 'begin'}, {commitWatermark: '131'}],
      ['commit', {tag: 'commit'}, {watermark: '131'}],
    ] satisfies ChangeStreamMessage[]) {
      void changeStream.push(msg);
    }
    changeStream.release('131');

    expect(await drainChanges(8)).toMatchObject([
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '123.01'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          rowValues: [
            [5, 6],
            [7, 8],
          ],
          tag: 'backfill',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '123.01'}],

      ['begin', {tag: 'begin'}, {commitWatermark: '131'}],
      ['commit', {tag: 'commit'}, {watermark: '131'}],

      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '131.01'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          tag: 'backfill-completed',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '131.01'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);
  });

  test('backfill-completed waits for stream status to reach backfill watermark', async () => {
    testStreams.push([
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [
          [1, 2],
          [3, 4],
        ],
      },
      {
        tag: 'backfill-completed',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        columns: ['b'],
        watermark: '130',
      },
    ]);

    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);

    // Let the backfill start streaming before acquiring a reservation
    // for the main stream. It should end its transaction and release
    // the reservation before flushing the backfill-completed message.
    await sleep(100);
    changeStream.pushStatus(['status', {ack: false}, {watermark: '130'}]);

    await expectChanges([
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '123.01'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          rowValues: [
            [1, 2],
            [3, 4],
          ],
          tag: 'backfill',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '123.01'}],
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '130'},
      ],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          tag: 'backfill-completed',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '130'}],
    ] satisfies ChangeStreamMessage[]);

    expect(backfillRequests).toMatchObject([
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);
  });

  test('change stream cancelation unblocks a backfill awaiting a reservation', async () => {
    testStreams.push([
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [[1, 2]],
      },
      {
        tag: 'backfill-completed',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        columns: ['b'],
        watermark: '130',
      },
    ]);

    // The main stream holds the reservation for the rest of the test,
    // simulating a stream that was canceled in the middle of a transaction.
    await changeStream.reserve('main');

    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);

    // Let the backfill start and block on the reservation.
    await sleep(10);
    expect(backfillRequests).toHaveLength(1);
    expect(finalizedStreams).toBe(0);

    // Canceling the change stream must unblock the backfill so that its
    // stream (and the upstream resources it holds) is finalized.
    changeStream.asSource().cancel();
    await vi.waitFor(() => expect(finalizedStreams).toBe(1));

    // The backfill must not be retried after cancelation.
    await sleep(100);
    expect(backfillRequests).toHaveLength(1);
  });

  test('change stream cancelation unblocks a backfill awaiting the stream watermark', async () => {
    testStreams.push([
      {
        tag: 'backfill',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        watermark: '130',
        columns: ['b'],
        rowValues: [[1, 2]],
      },
      {
        tag: 'backfill-completed',
        relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
        columns: ['b'],
        watermark: '130',
      },
    ]);

    backfillManager.run('123', [
      {
        columns: {a: {id: '123'}, b: {id: '234'}},
        table: {
          metadata: {rowKey: {a: 123}},
          name: 'bar',
          schema: 'foo',
        },
      },
    ]);

    // The first message is streamed, after which the backfill waits for
    // the change stream (at '123') to reach the backfill watermark ('130')
    // before sending the `backfill-completed` message.
    await expectChanges([
      [
        'begin',
        {tag: 'begin', json: 'p', skipAck: true, backfill: true},
        {commitWatermark: '123.01'},
      ],
      [
        'data',
        {
          tag: 'backfill',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          watermark: '130',
          columns: ['b'],
          rowValues: [[1, 2]],
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '123.01'}],
    ]);
    expect(finalizedStreams).toBe(0);

    // The change stream never reaches the watermark. Canceling it must
    // unblock the backfill so that its stream is finalized.
    changeStream.asSource().cancel();
    await vi.waitFor(() => expect(finalizedStreams).toBe(1));

    // The backfill must not be retried after cancelation.
    await sleep(100);
    expect(backfillRequests).toHaveLength(1);
  });

  describe('backfill requests from subscribers', () => {
    const RELATION: BackfillStarted['relation'] = {
      schema: 'foo',
      name: 'bar',
      rowKey: {columns: ['a']},
    };

    const REQUEST: BackfillRequest = {
      columns: {b: {id: '234'}},
      table: {metadata: {rowKey: {a: 123}}, name: 'bar', schema: 'foo'},
    };

    /**
     * Releases every held stream at the end of a test, so that the generators
     * finish rather than being left suspended.
     */
    let releases: (() => void)[];

    beforeEach(() => {
      releases = [];
      return () => releases.forEach(release => release());
    });

    /**
     * Drains `n` changes and lets the manager's own continuations run, so
     * that the run's recorded position reflects what was drained. (`lastMark`
     * is recorded after the push, so that it is never ahead of what
     * subscribers have been sent.)
     */
    async function drainAndSettle(n: number) {
      await drainChanges(n);
      await sleep(1);
    }

    /** A stream whose run stays running until the test ends. */
    function running(...items: TestStreamItem[]): TestStreamItem[] {
      return [...items, hold()];
    }

    /** A hold that is released when the test ends. */
    function hold(): {hold: Promise<void>} {
      const {promise, resolve} = resolver<void>();
      releases.push(resolve);
      return {hold: promise};
    }

    /**
     * A hold the test releases itself, to let the run produce its next
     * message. A re-announcement is pushed before that message, which is what
     * keeps it ordered with the run's rows.
     */
    function pausedRun(
      before: TestStreamItem[],
      after: TestStreamItem[],
    ): {stream: TestStreamItem[]; resume: () => void} {
      const {promise, resolve} = resolver<void>();
      releases.push(resolve);
      return {
        stream: [...before, {hold: promise}, ...after, hold()],
        resume: resolve,
      };
    }

    function announcement(
      runID: string,
      resumeFrom: Mark | null = null,
    ): BackfillStarted {
      return {
        tag: 'backfill-started',
        relation: RELATION,
        columns: ['b'],
        watermark: '130',
        runID,
        resumeFrom,
      };
    }

    function rows(lastKey: Mark | undefined): MessageBackfill {
      return {
        tag: 'backfill',
        relation: RELATION,
        columns: ['b'],
        watermark: '130',
        rowValues: [[1, 2]],
        ...(lastKey === undefined ? {} : {lastKey}),
      };
    }

    function declaration(
      mark: Mark | null,
      runID: string | null = null,
      markWatermark: string | null = mark === null ? null : '130',
    ): BackfillRequestMessage {
      return [
        'backfill-request',
        {
          table: REQUEST.table,
          columns: REQUEST.columns,
          mark,
          markWatermark,
          runID,
        },
      ];
    }

    test('a subscriber already following the run is a no-op', async () => {
      testStreams.push(running(announcement('run-1'), rows(['5'])));
      backfillManager.run('123', [REQUEST]);
      await drainAndSettle(3); // begin, backfill-started, backfill

      await backfillManager.onBackfillRequest(declaration(['1'], 'run-1'));
      expect(rowsExistCalls).toEqual([]);
      expect(backfillRequests).toHaveLength(1); // no restart
    });

    test('a subscriber the run has passed nothing for is re-announced to', async () => {
      const {stream, resume} = pausedRun(
        [announcement('run-1'), rows(['5'])],
        [rows(['9'])],
      );
      testStreams.push(stream);
      backfillManager.run('123', [REQUEST]);
      await drainAndSettle(3); // begin, backfill-started, backfill

      rowsExistAnswers = [false]; // nothing in (['1'], ['5']]
      await backfillManager.onBackfillRequest(declaration(['1'], 'other-run'));
      expect(rowsExistCalls).toEqual([{from: ['1'], to: ['5']}]);
      // No restart: the run keeps going...
      expect(backfillRequests).toHaveLength(1);

      // ...and the announcement goes out ahead of the run's next message,
      // which is what makes "anyone at this mark is covered from here" true.
      resume();
      expect(await changes.dequeue()).toMatchObject([
        'data',
        {tag: 'backfill-started', runID: 'run-1', resumeFrom: ['1']},
      ]);
      expect(await changes.dequeue()).toMatchObject([
        'data',
        {tag: 'backfill', lastKey: ['9']},
      ]);
    });

    test('a subscriber the run has passed rows for restarts it from the mark', async () => {
      testStreams.push(running(announcement('run-1'), rows(['5'])));
      testStreams.push(running(announcement('run-2', ['1']), rows(['9'])));
      backfillManager.run('123', [REQUEST]);
      await drainAndSettle(3);

      rowsExistAnswers = [true]; // rows exist in (['1'], ['5']]
      await backfillManager.onBackfillRequest(declaration(['1'], 'other-run'));
      expect(rowsExistCalls).toEqual([{from: ['1'], to: ['5']}]);

      await vi.waitFor(() => expect(backfillRequests).toHaveLength(2));
      expect(backfillRequests[1].resumeFrom).toEqual(['1']);
    });

    test('a declaration adds columns the manager finished while another column is running', async () => {
      testStreams.push(running(announcement('run-1'), rows(['5'])));
      testStreams.push(running(announcement('run-2'), rows(['9'])));
      backfillManager.run('123', [REQUEST]);
      await drainAndSettle(3);
      const [, request] = declaration(['1']);
      await backfillManager.onBackfillRequest([
        'backfill-request',
        {
          ...request,
          columns: {...request.columns, c: {id: '345'}},
        },
      ]);
      await vi.waitFor(() => expect(backfillRequests).toHaveLength(2));
      expect(backfillRequests[1]).toMatchObject({
        columns: {...REQUEST.columns, c: {id: '345'}},
        resumeFrom: null,
      });
    });

    test('an unordered run restarts from the beginning', async () => {
      // No `lastKey` anywhere: the run is not ordered, so there is no mark to
      // say "anyone here is covered from this point".
      testStreams.push(running(announcement('run-1'), rows(undefined)));
      testStreams.push(running(announcement('run-2'), rows(undefined)));
      backfillManager.run('123', [REQUEST]);
      await drainAndSettle(3);

      await backfillManager.onBackfillRequest(declaration(['1'], 'other-run'));
      expect(rowsExistCalls).toEqual([]); // nothing to compare against
      await vi.waitFor(() => expect(backfillRequests).toHaveLength(2));
      expect(backfillRequests[1].resumeFrom).toBe(null);
    });

    test('a mark older than a key change is dropped', async () => {
      testStreams.push(running(announcement('run-1'), rows(['5'])));
      testStreams.push(running(announcement('run-2'), rows(['9'])));
      backfillManager.run('123', [REQUEST]);
      await drainAndSettle(3);

      // A key change at '140' on the table.
      backfillManager.onChange([
        'begin',
        {tag: 'begin'},
        {commitWatermark: '140'},
      ]);
      backfillManager.onChange([
        'data',
        {
          tag: 'update',
          relation: RELATION,
          key: {a: 1},
          new: {a: 2},
        },
      ]);
      backfillManager.onChange(['commit', {tag: 'commit'}, {watermark: '140'}]);

      // A mark from a snapshot that predates it cannot be resumed from.
      await backfillManager.onBackfillRequest(
        declaration(['1'], 'other-run', '130'),
      );
      expect(rowsExistCalls).toEqual([{from: null, to: ['5']}]);
    });

    test('a table this session already finished is added, from the beginning', async () => {
      testStreams.push(running(announcement('run-1'), rows(['5'])));
      backfillManager.run('123', []); // nothing required
      expect(backfillRequests).toHaveLength(0);

      await backfillManager.onBackfillRequest(declaration(['1'], 'run-1'));
      await vi.waitFor(() => expect(backfillRequests).toHaveLength(1));
      // Scenario B: this session has no `minSnapshot` for a table it
      // finished, so the declared mark is dropped.
      expect(backfillRequests[0].resumeFrom).toBe(null);
      expect(backfillRequests[0].columns).toEqual(REQUEST.columns);
    });

    describe('with no run active', () => {
      test('the first declared mark is what the next run resumes from', async () => {
        testStreams.push(running(announcement('run-1', ['1']), rows(['5'])));
        // An initial request carrying the manager's own replica's mark.
        backfillManager.run('123', [{...REQUEST, resumeFrom: ['1']}]);
        await drainAndSettle(3);
        expect(backfillRequests[0].resumeFrom).toEqual(['1']);
      });

      test('a matching mark leaves the resume point alone', async () => {
        backfillManager.run('123', []);
        await backfillManager.onBackfillRequest(declaration(['1']));
        // Added from the declaration, so its mark was dropped (Scenario B).
        testStreams.push(running(announcement('run-1'), rows(['5'])));
        await vi.waitFor(() => expect(backfillRequests).toHaveLength(1));
        expect(backfillRequests[0].resumeFrom).toBe(null);
      });
    });
  });
});
