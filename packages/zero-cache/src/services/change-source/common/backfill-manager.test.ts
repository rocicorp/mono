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
import {BackfillDeclarations} from '../../change-streamer/backfill-declarations.ts';
import type {BackfillDeclaration} from '../../change-streamer/change-streamer.ts';
import type {
  BackfillCompleted,
  BackfillRequest,
  BackfillRequestMessage,
  BackfillStarted,
  ChangeStreamMessage,
  MessageBackfill,
} from '../protocol/current.ts';
import {BackfillManager, type BackfillMessage} from './backfill-manager.ts';
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
      JSON_PARSED,
      10,
      50,
      commitThresholdBytes,
    );
    changeStream.addProducers(backfillManager).addListeners(backfillManager);

    // Drain ChangeStreamMessages to this manager's own queue. A run that a
    // previous test left behind -- one that finishes once its holds are
    // released at teardown -- must not deliver into the next test's queue.
    const stream = changeStream;
    const queue = (changes = new Queue<ChangeStreamMessage>());
    void (async () => {
      for await (const msg of stream.asSource()) {
        queue.enqueue(msg);
      }
    })();
  }

  beforeEach(() => {
    lc = createSilentLogContext();
    backfillRequests = [];
    testStreams = [];
    finalizedStreams = 0;
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
          resumes: null,
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

    /** An initial request carrying the manager's own replica's progress. */
    const RESUMED: BackfillRequest = {
      ...REQUEST,
      resumeFrom: ['3'],
      resumeFromWatermark: '120',
      resumeRunID: 'run-0',
      resumeSeq: 2,
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
     * that the run's recorded announcement reflects what was drained.
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

    /** A hold the test opens itself, to let the run produce its next message. */
    function gate(): {hold: Promise<void>; open: () => void} {
      const {promise, resolve} = resolver<void>();
      releases.push(resolve);
      return {hold: promise, open: resolve};
    }

    function announcement(
      runID: string,
      resumes: {runID: string; seq: number} | null = null,
    ): BackfillStarted {
      return {
        tag: 'backfill-started',
        relation: RELATION,
        columns: ['b'],
        watermark: '130',
        runID,
        resumes,
      };
    }

    function rows(seq: number, lastKey?: string[]): MessageBackfill {
      return {
        tag: 'backfill',
        relation: RELATION,
        columns: ['b'],
        watermark: '130',
        rowValues: [[1, 2]],
        seq,
        ...(lastKey === undefined ? {} : {lastKey}),
      };
    }

    function completion(): BackfillCompleted {
      return {
        tag: 'backfill-completed',
        relation: RELATION,
        columns: ['b'],
        watermark: '130',
      };
    }

    function declaration(
      runID: string | null,
      runSeq: number | null = runID === null ? null : 1,
    ): BackfillRequestMessage {
      return [
        'backfill-request',
        {
          table: REQUEST.table,
          columns: REQUEST.columns,
          runID,
          runSeq,
        },
      ];
    }

    /** The manager's `n`th run, once it starts. */
    async function nthRun(n: number): Promise<BackfillRequest> {
      await vi.waitFor(() => expect(backfillRequests).toHaveLength(n));
      return backfillRequests[n - 1];
    }

    /** Lets a held run finish and checks that nothing runs after it. */
    async function expectNoRunAfter(done: {open: () => void}, runs: number) {
      done.open();
      await vi.waitFor(() => expect(finalizedStreams).toBe(runs));
      await sleep(20);
      expect(backfillRequests).toHaveLength(runs);
    }

    function statusAt(watermark: string) {
      changeStream.pushStatus(['status', {ack: false}, {watermark}]);
    }

    test('a subscriber already following the run is a no-op', async () => {
      const done = gate();
      testStreams.push([
        announcement('run-1'),
        rows(1, ['5']),
        done,
        completion(),
      ]);
      backfillManager.run('123', [REQUEST]);
      statusAt('130');
      await drainAndSettle(3); // begin, backfill-started, backfill

      backfillManager.onBackfillRequest(declaration('run-1', 1));
      await expectNoRunAfter(done, 1);
    });

    test('subscribers not following the run get one run from the beginning after it', async () => {
      const done = gate();
      testStreams.push([
        announcement('run-1', {runID: 'run-0', seq: 2}),
        rows(1, ['5']),
        done,
        completion(),
      ]);
      testStreams.push(running(announcement('run-2'), rows(1, ['9'])));
      backfillManager.run('123', [RESUMED]);
      statusAt('130');
      await drainAndSettle(3);

      // From another manager's run, following nothing, and behind the point
      // this run resumed from: none of them has what this run has sent.
      backfillManager.onBackfillRequest(declaration('elsewhere', 4));
      backfillManager.onBackfillRequest(declaration(null));
      backfillManager.onBackfillRequest(declaration('run-0', 1));
      // The running run is not restarted ...
      expect(backfillRequests).toHaveLength(1);
      done.open();
      // ... and once it completes, the table runs again from the beginning,
      // once for all of them.
      expect(await nthRun(2)).toMatchObject({
        columns: REQUEST.columns,
        resumeFrom: null,
        resumeRunID: null,
      });
      await sleep(20);
      expect(backfillRequests).toHaveLength(2);
    });

    test('a subscriber following the resumed run from a batch it applied follows the run, even before it announces', async () => {
      const start = gate();
      const done = gate();
      testStreams.push([
        start,
        announcement('run-1', {runID: 'run-0', seq: 2}),
        rows(1, ['5']),
        done,
        completion(),
      ]);
      backfillManager.run('123', [RESUMED]);
      statusAt('130');
      await sleep(1); // the run has started, but announced nothing yet
      expect(backfillRequests).toHaveLength(1);

      backfillManager.onBackfillRequest(declaration('run-0', 2)); // at the point
      backfillManager.onBackfillRequest(declaration('run-0', 7)); // past it
      start.open();
      await drainAndSettle(3);
      backfillManager.onBackfillRequest(declaration('run-1', 0)); // following
      await expectNoRunAfter(done, 1);
    });

    test('a subscriber behind the resume point gets a rerun, even before the run announces', async () => {
      const start = gate();
      const done = gate();
      testStreams.push([
        start,
        announcement('run-1', {runID: 'run-0', seq: 2}),
        rows(1, ['5']),
        done,
        completion(),
      ]);
      testStreams.push(running(announcement('run-2'), rows(1, ['9'])));
      backfillManager.run('123', [RESUMED]);
      statusAt('130');
      await sleep(1);

      backfillManager.onBackfillRequest(declaration('run-0', 1));
      start.open();
      done.open();
      expect(await nthRun(2)).toMatchObject({
        resumeFrom: null,
        resumeRunID: null,
      });
    });

    test('a declaration adds columns the manager finished while another column is running', async () => {
      testStreams.push(running(announcement('run-1'), rows(1, ['5'])));
      testStreams.push(running(announcement('run-2'), rows(1, ['9'])));
      backfillManager.run('123', [RESUMED]);
      await drainAndSettle(3);
      const [, request] = declaration('run-1', 1);
      backfillManager.onBackfillRequest([
        'backfill-request',
        {
          ...request,
          columns: {...request.columns, c: {id: '345'}},
        },
      ]);
      // Restarted at once, from the beginning, with every column.
      expect(await nthRun(2)).toMatchObject({
        columns: {...REQUEST.columns, c: {id: '345'}},
        resumeFrom: null,
        resumeRunID: null,
      });
    });

    test('a table this session already finished is added, from the beginning', async () => {
      testStreams.push(running(announcement('run-1'), rows(1, ['5'])));
      backfillManager.run('123', []); // nothing required
      expect(backfillRequests).toHaveLength(0);

      backfillManager.onBackfillRequest(declaration('run-1'));
      // Scenario B: this session has no `minSnapshot` for a table it
      // finished, so it starts from the beginning.
      expect(await nthRun(1)).toMatchObject({
        columns: REQUEST.columns,
        resumeFrom: null,
        resumeRunID: null,
      });
    });

    describe('with no run active', () => {
      /**
       * A first attempt that fails at once, so that the manager is between
       * runs -- backing off before the retry -- when a declaration arrives.
       */
      async function failFirst(request: BackfillRequest) {
        testStreams.push(new Error('the first attempt fails'));
        backfillManager.run('123', [request]);
        await sleep(1); // (a macrotask: the failure settles, the retry waits)
        expect(backfillRequests).toHaveLength(1);
      }

      test("the initial request's mark is what the first run resumes from", async () => {
        testStreams.push(
          running(announcement('run-1', {runID: 'run-0', seq: 2}), rows(1)),
        );
        backfillManager.run('123', [RESUMED]);
        await drainAndSettle(3);
        expect(backfillRequests[0]).toMatchObject({
          resumeFrom: ['3'],
          resumeRunID: 'run-0',
          resumeSeq: 2,
        });
      });

      test('a declaration following the resumed run from a batch applied leaves the resume point alone', async () => {
        await failFirst(RESUMED);
        testStreams.push(
          running(announcement('run-1', {runID: 'run-0', seq: 2}), rows(1)),
        );
        backfillManager.onBackfillRequest(declaration('run-0', 3));
        expect(await nthRun(2)).toMatchObject({
          resumeFrom: ['3'],
          resumeRunID: 'run-0',
          resumeSeq: 2,
        });
      });

      for (const request of [
        declaration('run-0', 1),
        declaration('elsewhere', 9),
        declaration(null),
      ]) {
        test(`a declaration the resumed run would not cover, ${JSON.stringify(
          request[1],
        )}, starts it from the beginning`, async () => {
          await failFirst(RESUMED);
          testStreams.push(running(announcement('run-1'), rows(1)));
          backfillManager.onBackfillRequest(request);
          expect(await nthRun(2)).toMatchObject({
            resumeFrom: null,
            resumeRunID: null,
            resumeSeq: null,
          });
        });
      }
    });

    describe('a run canceled mid-way is replaced for its followers', () => {
      function tx(watermark: string, ...data: ChangeStreamMessage[]) {
        backfillManager.onChange([
          'begin',
          {tag: 'begin'},
          {commitWatermark: watermark},
        ]);
        data.forEach(change => backfillManager.onChange(change));
        backfillManager.onChange(['commit', {tag: 'commit'}, {watermark}]);
      }

      const metadataUpdate: ChangeStreamMessage = [
        'data',
        {
          tag: 'update-table-metadata',
          table: {schema: 'foo', name: 'bar'},
          old: {rowKey: {a: 123}},
          new: {rowKey: {a: 456}},
        },
      ];

      test('a resumed run is replaced by one that resumes it from its start', async () => {
        testStreams.push(
          running(announcement('run-1', {runID: 'run-0', seq: 2}), rows(1)),
        );
        testStreams.push(
          running(announcement('run-2', {runID: 'run-1', seq: 0}), rows(1)),
        );
        backfillManager.run('123', [RESUMED]);
        await drainAndSettle(3);

        tx('140', metadataUpdate);
        expect(await nthRun(2)).toMatchObject({
          table: {metadata: {rowKey: {a: 456}}},
          resumeFrom: ['3'],
          resumeRunID: 'run-1',
          resumeSeq: 0,
        });
      });

      test('a run from the beginning is replaced by another from the beginning', async () => {
        // The change source found the table not resumable, and ran it from
        // the beginning despite the mark.
        testStreams.push(running(announcement('run-1'), rows(1)));
        testStreams.push(running(announcement('run-2'), rows(1)));
        backfillManager.run('123', [RESUMED]);
        await drainAndSettle(3);

        tx('140', metadataUpdate);
        expect(await nthRun(2)).toMatchObject({
          resumeFrom: null,
          resumeRunID: null,
        });
      });

      test('a run that fails is retried where it left off for its followers', async () => {
        testStreams.push([
          announcement('run-1', {runID: 'run-0', seq: 2}),
          rows(1, ['5']),
          new Error('lost the COPY'),
        ]);
        testStreams.push(
          running(announcement('run-2', {runID: 'run-1', seq: 0}), rows(1)),
        );
        backfillManager.run('123', [RESUMED]);
        expect(await nthRun(2)).toMatchObject({
          resumeFrom: ['3'],
          resumeRunID: 'run-1',
          resumeSeq: 0,
        });
      });

      test('a run that has not announced itself is replaced by what it would have been', async () => {
        const start = gate();
        testStreams.push([
          start,
          announcement('run-1', {runID: 'run-0', seq: 2}),
          rows(1),
          hold(),
        ]);
        testStreams.push(
          running(announcement('run-2', {runID: 'run-0', seq: 2}), rows(1)),
        );
        backfillManager.run('123', [RESUMED]);
        await sleep(1);

        tx('140', metadataUpdate);
        start.open();
        expect(await nthRun(2)).toMatchObject({
          resumeFrom: ['3'],
          resumeRunID: 'run-0',
          resumeSeq: 2,
        });
      });

      test('a rename carries the resume state, and a key change since, with it', async () => {
        testStreams.push(
          running(announcement('run-1', {runID: 'run-0', seq: 2}), rows(1)),
        );
        testStreams.push(running(announcement('run-2'), rows(1)));
        backfillManager.run('123', [RESUMED]);
        await drainAndSettle(3);

        // A key change on the table voids the mark ...
        tx('140', [
          'data',
          {tag: 'update', relation: RELATION, key: {a: 1}, new: {a: 2}},
        ]);
        // ... and the rename that follows must not bring it back from the
        // initial request, nor forget the key change.
        tx('141', [
          'data',
          {
            tag: 'rename-table',
            old: {schema: 'foo', name: 'bar'},
            new: {schema: 'foo', name: 'baz'},
          },
        ]);
        expect(await nthRun(2)).toMatchObject({
          table: {name: 'baz'},
          resumeFrom: null,
          resumeRunID: null,
          minSnapshot: '140',
        });
      });
    });
  });

  describe('subscribers arriving mid-run', () => {
    const KEYS = Array.from({length: 30}, (_, i) => i + 1);
    const RELATION: BackfillStarted['relation'] = {
      schema: 'foo',
      name: 'bar',
      rowKey: {columns: ['a']},
    };
    const REQUEST: BackfillRequest = {
      columns: {b: {id: '234'}},
      table: {metadata: {rowKey: {a: 123}}, name: 'bar', schema: 'foo'},
    };

    function declared(
      runID: string | null,
      runSeq: number | null,
    ): BackfillDeclaration {
      return {
        schema: 'foo',
        table: 'bar',
        columns: ['b'],
        metadata: REQUEST.table.metadata,
        backfill: REQUEST.columns,
        runID,
        runSeq,
      };
    }

    /**
     * A replication-manager over a table keyed 1..30, and the change-streamer's
     * declaration trackers for its subscribers. A tracker sees exactly what its
     * subscriber is sent, and its requests are how the subscriber's state
     * reaches the manager.
     *
     * The test decides when a run sends its next batch (`advance`) and when the
     * requests made so far arrive (`deliver`), which is what lets a run get
     * well under way before a subscriber's request is heard.
     */
    function system() {
      const runs: BackfillRequest[] = [];
      // (Not `Queue<void>`: a queued `undefined` dequeues as a rejection.)
      const tokens = new Queue<true>();

      async function* streamer(
        req: BackfillRequest,
      ): AsyncGenerator<BackfillMessage> {
        runs.push(req);
        const runID = `run-${runs.length}`;
        const from = req.resumeFrom ?? null;
        const common = {relation: RELATION, columns: ['b'], watermark: '123'};
        yield {
          message: {
            tag: 'backfill-started',
            ...common,
            runID,
            resumes:
              from === null
                ? null
                : {runID: must(req.resumeRunID), seq: must(req.resumeSeq)},
          },
          byteSize: 0,
        };
        let seq = 0;
        for (const key of KEYS) {
          if (from !== null && key <= Number(from[0])) {
            continue;
          }
          await tokens.dequeue();
          yield {
            message: {
              tag: 'backfill',
              ...common,
              rowValues: [[key, `b${key}`]],
              runID,
              seq: ++seq,
              lastKey: [String(key)],
            },
            byteSize: 1,
          };
        }
        yield {
          message: {tag: 'backfill-completed', ...common, runID},
          byteSize: 0,
        };
      }

      const stream = new ChangeStreamMultiplexer(lc, '123');
      const manager = new BackfillManager(
        lc,
        stream,
        streamer,
        JSON_PARSED,
        10,
        50,
        1, // every batch is committed before the next one is sent
      );
      stream.addProducers(manager).addListeners(manager);

      const subscribers = new Map<
        string,
        {
          tracker: BackfillDeclarations;
          sent: string;
          requests: BackfillRequestMessage[];
        }
      >();

      void (async () => {
        for await (const msg of stream.asSource()) {
          if (msg[0] === 'status' || msg[0] === 'control') {
            continue;
          }
          for (const [id, sub] of subscribers) {
            sub.tracker.apply(msg);
            // As `Subscriber` does: at each transaction boundary, a request for
            // whatever the subscriber is not covered for, when that changes.
            if (msg[0] === 'commit' || msg[0] === 'rollback') {
              const requests = sub.tracker.requests(id);
              const serialized = JSON.stringify(requests);
              if (requests.length && serialized !== sub.sent) {
                sub.requests.push(...requests);
              }
              sub.sent = serialized;
            }
          }
        }
      })();

      return {
        manager,
        runs,
        subscribe(id: string, declaration: BackfillDeclaration) {
          subscribers.set(id, {
            tracker: new BackfillDeclarations([declaration]),
            sent: '[]',
            requests: [],
          });
        },
        /** Lets the running run send `n` more batches. */
        async advance(n: number) {
          for (let i = 0; i < n; i++) {
            tokens.enqueue(true);
          }
          for (let i = 0; i < 100 && tokens.size() > 0; i++) {
            await sleep(1);
          }
          await sleep(5);
        },
        /** Delivers every request the subscribers have made so far. */
        deliver() {
          for (const sub of subscribers.values()) {
            for (const request of sub.requests.splice(0)) {
              manager.onBackfillRequest(request);
            }
          }
        },
        /** Whether every subscriber has completed the backfill. */
        done: () =>
          [...subscribers.values()].every(({tracker}) => !tracker.pending),
      };
    }

    test('subscribers arriving from elsewhere mid-run are served by one run after the current one, whose followers are never left behind', async () => {
      const {manager, runs, subscribe, advance, deliver, done} = system();
      // `f` is there from the start of the first run, and follows it.
      subscribe('f', declared(null, null));
      manager.run('123', [REQUEST]);
      await advance(6);

      // `s` and `t` arrive from another replication-manager, following a run
      // of its own, at different points in it. Neither has what this run has
      // sent, and nothing about their runs says what they have.
      subscribe('s', declared('elsewhere', 10));
      subscribe('t', declared('elsewhere', 12));
      for (let round = 0; round < 12 && !done(); round++) {
        deliver();
        await advance(6);
      }

      // The first run ran to its end for `f`; one more, from the beginning,
      // served both `s` and `t`.
      expect(runs.map(run => run.resumeFrom ?? null)).toEqual([null, null]);
      expect(done()).toBe(true);
    });
  });
});
