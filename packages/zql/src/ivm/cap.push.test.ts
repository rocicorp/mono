import {expect, suite, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {SourceChange} from './source.ts';
import {Cap} from './cap.ts';
import {Catch} from './catch.ts';
import {MemoryStorage} from './memory-storage.ts';
import {Snitch} from './snitch.ts';
import {createSource} from './test/source-factory.ts';
import {consume} from './stream.ts';

const lc = createSilentLogContext();

const columns = {
  id: {type: 'string'},
  created: {type: 'number'},
  text: {type: 'string', optional: true},
} as const;
const primaryKey = ['id'] as const;

function capPushTest(t: {
  sourceRows: Row[];
  limit: number;
  pushes: SourceChange[];
}) {
  const source = createSource(lc, testLogConfig, 'table', columns, primaryKey);
  for (const row of t.sourceRows) {
    consume(source.push({type: 'add', row}));
  }
  const conn = source.connect([['id', 'asc']]);
  const snitch = new Snitch(conn, 'capSnitch');
  const storage = new MemoryStorage();
  const cap = new Cap(snitch, storage, t.limit);
  const catchOp = new Catch(cap);

  // Hydrate
  catchOp.fetch();
  catchOp.reset();

  // Push
  for (const push of t.pushes) {
    consume(source.push(push));
  }

  return {
    pushes: catchOp.pushes,
    storage: storage.cloneData(),
  };
}

suite('cap push with no partition', () => {
  suite('add', () => {
    test('limit 0', () => {
      const {pushes} = capPushTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
        ],
        limit: 0,
        pushes: [{type: 'add', row: {id: 'i4', created: 50, text: null}}],
      });
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });

    test('less than limit add row', () => {
      const {pushes, storage} = capPushTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
        ],
        limit: 5,
        pushes: [{type: 'add', row: {id: 'i4', created: 50, text: null}}],
      });
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "node": {
              "relationships": {},
              "row": {
                "created": 50,
                "id": "i4",
                "text": null,
              },
            },
            "type": "add",
          },
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["cap"]": {
            "pks": [
              "["i1"]",
              "["i2"]",
              "["i3"]",
              "["i4"]",
            ],
            "size": 4,
          },
        }
      `);
    });

    test('at limit add row is dropped', () => {
      const {pushes, storage} = capPushTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
        ],
        limit: 3,
        pushes: [{type: 'add', row: {id: 'i4', created: 50, text: null}}],
      });
      expect(pushes).toMatchInlineSnapshot(`[]`);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["cap"]": {
            "pks": [
              "["i1"]",
              "["i2"]",
              "["i3"]",
            ],
            "size": 3,
          },
        }
      `);
    });
  });

  suite('remove', () => {
    test('remove tracked row, no refill available', () => {
      const {pushes, storage} = capPushTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
        ],
        limit: 3,
        pushes: [{type: 'remove', row: {id: 'i2', created: 200, text: null}}],
      });
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "node": {
              "relationships": {},
              "row": {
                "created": 200,
                "id": "i2",
                "text": null,
              },
            },
            "type": "remove",
          },
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["cap"]": {
            "pks": [
              "["i1"]",
              "["i3"]",
            ],
            "size": 2,
          },
        }
      `);
    });

    test('remove tracked row with refill', () => {
      const {pushes, storage} = capPushTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
          {id: 'i4', created: 400, text: null},
        ],
        limit: 3,
        pushes: [{type: 'remove', row: {id: 'i2', created: 200, text: null}}],
      });
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "node": {
              "relationships": {},
              "row": {
                "created": 200,
                "id": "i2",
                "text": null,
              },
            },
            "type": "remove",
          },
          {
            "node": {
              "relationships": {},
              "row": {
                "created": 400,
                "id": "i4",
                "text": null,
              },
            },
            "type": "add",
          },
        ]
      `);
      expect(storage).toMatchInlineSnapshot(`
        {
          "["cap"]": {
            "pks": [
              "["i1"]",
              "["i3"]",
              "["i4"]",
            ],
            "size": 3,
          },
        }
      `);
    });

    test('remove untracked row is dropped', () => {
      const {pushes} = capPushTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
          {id: 'i4', created: 400, text: null},
        ],
        limit: 3,
        pushes: [{type: 'remove', row: {id: 'i4', created: 400, text: null}}],
      });
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });
  });

  suite('edit', () => {
    test('edit tracked row', () => {
      const {pushes} = capPushTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
        ],
        limit: 3,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i2', created: 200, text: null},
            row: {id: 'i2', created: 250, text: null},
          },
        ],
      });
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "oldRow": {
              "created": 200,
              "id": "i2",
              "text": null,
            },
            "row": {
              "created": 250,
              "id": "i2",
              "text": null,
            },
            "type": "edit",
          },
        ]
      `);
    });

    test('edit untracked row is dropped', () => {
      const {pushes} = capPushTest({
        sourceRows: [
          {id: 'i1', created: 100, text: null},
          {id: 'i2', created: 200, text: null},
          {id: 'i3', created: 300, text: null},
          {id: 'i4', created: 400, text: null},
        ],
        limit: 3,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i4', created: 400, text: null},
            row: {id: 'i4', created: 450, text: null},
          },
        ],
      });
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });
  });
});

suite('cap push with partition', () => {
  const partitionColumns = {
    id: {type: 'string'},
    issueID: {type: 'string'},
    created: {type: 'number'},
  } as const;
  const partitionPrimaryKey = ['id'] as const;

  function capPartitionPushTest(t: {
    sourceRows: Row[];
    limit: number;
    pushes: SourceChange[];
  }) {
    const source = createSource(
      lc,
      testLogConfig,
      'table',
      partitionColumns,
      partitionPrimaryKey,
    );
    for (const row of t.sourceRows) {
      consume(source.push({type: 'add', row}));
    }
    const conn = source.connect([['id', 'asc']]);
    const snitch = new Snitch(conn, 'capSnitch');
    const storage = new MemoryStorage();
    const cap = new Cap(snitch, storage, t.limit, ['issueID']);
    const catchOp = new Catch(cap);

    // Hydrate partition i1
    catchOp.fetch({constraint: {issueID: 'i1'}});
    // Hydrate partition i2
    catchOp.fetch({constraint: {issueID: 'i2'}});
    catchOp.reset();

    // Push
    for (const push of t.pushes) {
      consume(source.push(push));
    }

    return {
      pushes: catchOp.pushes,
      storage: storage.cloneData(),
    };
  }

  test('add below limit in partition', () => {
    const {pushes} = capPartitionPushTest({
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i2', created: 200},
      ],
      limit: 3,
      pushes: [{type: 'add', row: {id: 'c3', issueID: 'i1', created: 300}}],
    });
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {},
            "row": {
              "created": 300,
              "id": "c3",
              "issueID": "i1",
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('add at limit in partition is dropped', () => {
    const {pushes} = capPartitionPushTest({
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i2', created: 300},
      ],
      limit: 2,
      pushes: [{type: 'add', row: {id: 'c4', issueID: 'i1', created: 400}}],
    });
    expect(pushes).toMatchInlineSnapshot(`[]`);
  });

  test('remove from partition with refill', () => {
    const {pushes} = capPartitionPushTest({
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
        {id: 'c4', issueID: 'i2', created: 400},
      ],
      limit: 2,
      pushes: [{type: 'remove', row: {id: 'c1', issueID: 'i1', created: 100}}],
    });
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {},
            "row": {
              "created": 100,
              "id": "c1",
              "issueID": "i1",
            },
          },
          "type": "remove",
        },
        {
          "node": {
            "relationships": {},
            "row": {
              "created": 300,
              "id": "c3",
              "issueID": "i1",
            },
          },
          "type": "add",
        },
      ]
    `);
  });
});
