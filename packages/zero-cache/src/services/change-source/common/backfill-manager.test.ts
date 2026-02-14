import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test} from 'vitest';
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
  ChangeStreamMessage,
  MessageBackfill,
} from '../protocol/current.ts';
import {BackfillManager} from './backfill-manager.ts';
import {ChangeStreamMultiplexer} from './change-stream-multiplexer.ts';

describe('backfill-manager', () => {
  let backfillManager: BackfillManager;
  let changeStream: ChangeStreamMultiplexer;
  let backfillRequests: BackfillRequest[];
  let testStreams: ((MessageBackfill | BackfillCompleted)[] | Error)[];
  let changes: Queue<ChangeStreamMessage>;
  let lc: LogContext;

  beforeEach(() => {
    lc = createSilentLogContext();
    changeStream = new ChangeStreamMultiplexer(lc, '123');
    backfillManager = new BackfillManager(
      lc,
      changeStream,
      backfillStreamer,
      JSON_PARSED,
      10,
      50,
    );
    changeStream.addProducers(backfillManager).addListeners(backfillManager);
    backfillRequests = [];
    testStreams = [];
    changes = new Queue<ChangeStreamMessage>();

    // Drain ChangeStreamMessages to the changes queue.
    void (async () => {
      for await (const msg of changeStream.asSource()) {
        changes.enqueue(msg);
      }
    })();
  });

  async function* backfillStreamer(
    req: BackfillRequest,
  ): AsyncGenerator<MessageBackfill | BackfillCompleted> {
    lc.debug?.(`starting test backfill stream for`, req);
    backfillRequests.push(req);
    const stream = must(
      testStreams.shift(),
      `No more testStreams configured by test`,
    );

    if (stream instanceof Error) {
      throw stream; // For testing backfill errors
    }

    for (const msg of stream) {
      yield msg;
    }
  }

  async function drainChanges(n: number): Promise<ChangeStreamMessage[]> {
    const c: ChangeStreamMessage[] = [];
    for (let i = 0; i < n; i++) {
      c.push(await changes.dequeue());
    }
    return c;
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

    expect(await drainChanges(5)).toMatchObject([
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '123.01'}],
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
      ['commit', {tag: 'commit'}, {watermark: '123.01'}],
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

    expect(await drainChanges(8)).toMatchObject([
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '125.01'}],
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
      ['commit', {tag: 'commit'}, {watermark: '125.01'}],
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '130.01'}],
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '125.01'}],
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
      ['commit', {tag: 'commit'}, {watermark: '125.01'}],
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
    expect(await drainChanges(6)).toMatchObject([
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '125.01'}],
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
      ['commit', {tag: 'commit'}, {watermark: '125.01'}],
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
    expect(await drainChanges(6)).toMatchObject([
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '125.01'}],
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
      ['commit', {tag: 'commit'}, {watermark: '125.01'}],
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
    expect(await drainChanges(6)).toMatchObject([
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '125.01'}],
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
      ['commit', {tag: 'commit'}, {watermark: '125.01'}],
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '140.01'}],
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

    expect(await drainChanges(12)).toMatchObject([
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '140.01'}],
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '140.02'}],
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
      [
        'data',
        {
          tag: 'backfill-completed',
          columns: ['c', 'd'],
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['a']}},
          watermark: '150',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '140.02'}],
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

    expect(await drainChanges(3)).toMatchObject([
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '123.01'}],
      [
        'data',
        {
          columns: ['b'],
          relation: {name: 'bar', rowKey: {columns: ['a']}, schema: 'foo'},
          tag: 'backfill-completed',
          watermark: '130',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '123.01'}],
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

    expect(await drainChanges(4)).toMatchObject([
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '123.01'}],
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
      [
        'data',
        {
          tag: 'backfill-completed',
          relation: {schema: 'foo', name: 'bar', rowKey: {columns: ['id']}},
          columns: [],
          watermark: '188',
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '123.01'}],
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '141.01'}],
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '142.01'}],
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '143.01'}],
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '144.01'}],
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
      ['begin', {tag: 'begin', json: 'p'}, {commitWatermark: '145.01'}],
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
});
