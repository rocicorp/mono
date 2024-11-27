import {describe, expect, suite, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.js';
import type {JSONValue} from '../../../shared/src/json.js';
import type {Ordering} from '../../../zero-protocol/src/ast.js';
import type {Row, Value} from '../../../zero-protocol/src/data.js';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';
import {Catch, type CaughtChange} from './catch.js';
import {MemoryStorage} from './memory-storage.js';
import {Snitch, type SnitchMessage} from './snitch.js';
import type {SourceChange} from './source.js';
import {Take, type PartitionKey} from './take.js';
import {createSource} from './test/source-factory.js';

suite('take with no partition', () => {
  const base = {
    columns: {
      id: {type: 'string'},
      created: {type: 'number'},
    },
    primaryKey: ['id'],
    sort: [
      ['created', 'asc'],
      ['id', 'asc'],
    ],
    partition: undefined,
  } as const;

  suite('add', () => {
    test('limit 0', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
        ],
        limit: 0,
        pushes: [{type: 'add', row: {id: 'i4', created: 50}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('less than limit add row at start', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
        ],
        limit: 5,
        pushes: [{type: 'add', row: {id: 'i4', created: 50}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('less than limit add row at end', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
        ],
        limit: 5,
        pushes: [{type: 'add', row: {id: 'i4', created: 350}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('at limit add row after bound', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
          {id: 'i4', created: 400},
        ],
        limit: 3,
        pushes: [{type: 'add', row: {id: 'i5', created: 350}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('at limit add row at start', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
          {id: 'i4', created: 400},
        ],
        limit: 3,
        pushes: [{type: 'add', row: {id: 'i5', created: 50}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('at limit add row at end', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
          {id: 'i4', created: 400},
        ],
        limit: 3,
        pushes: [{type: 'add', row: {id: 'i5', created: 250}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });
  });

  suite('remove', () => {
    test('limit 0', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
        ],
        limit: 0,
        pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('less than limit remove row at start', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
        ],
        limit: 5,
        pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('less than limit remove row at end', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
        ],
        limit: 5,
        pushes: [{type: 'remove', row: {id: 'i3', created: 300}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('at limit remove row after bound', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
          {id: 'i4', created: 400},
        ],
        limit: 3,
        pushes: [{type: 'remove', row: {id: 'i4', created: 400}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('at limit remove row at start with row after', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
          {id: 'i4', created: 400},
        ],
        limit: 3,
        pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('at limit remove row at start with row after, limit 2', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
          {id: 'i4', created: 400},
        ],
        limit: 2,
        pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('at limit remove row at start with row after, limit 1', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
          {id: 'i4', created: 400},
        ],
        limit: 1,
        pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('at limit remove row at start no row after', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
        ],
        limit: 3,
        pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('at limit remove row at end with row after', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
          {id: 'i4', created: 400},
        ],
        limit: 3,
        pushes: [{type: 'remove', row: {id: 'i3', created: 300}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('at limit remove row at end, no row after', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        sourceRows: [
          {id: 'i1', created: 100},
          {id: 'i2', created: 200},
          {id: 'i3', created: 300},
        ],
        limit: 3,
        pushes: [{type: 'remove', row: {id: 'i3', created: 300}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });
  });

  suite('edit', () => {
    const base = {
      columns: {
        id: {type: 'string'},
        created: {type: 'number'},
        text: {type: 'string'},
      },
      primaryKey: ['id'],
      sort: [
        ['created', 'asc'],
        ['id', 'asc'],
      ],
      sourceRows: [
        {id: 'i1', created: 100, text: 'a'},
        {id: 'i2', created: 200, text: 'b'},
        {id: 'i3', created: 300, text: 'c'},
        {id: 'i4', created: 400, text: 'd'},
      ],
      partition: undefined,
    } as const;

    test('limit 0', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        limit: 0,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i2', created: 200, text: 'b'},
            row: {id: 'i2', created: 200, text: 'c'},
          },
        ],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    describe('less than limit ', () => {
      test('edit row at start', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 5,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i1', created: 100, text: 'a'},
              row: {id: 'i1', created: 100, text: 'a2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row at end', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 5,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i4', created: 400, text: 'd'},
              row: {id: 'i4', created: 400, text: 'd2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });
    });

    describe('at limit', () => {
      test('edit row after boundary', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i4', created: 400, text: 'd'},
              row: {id: 'i4', created: 400, text: 'd2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row before boundary', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i2', created: 200, text: 'b'},
              row: {id: 'i2', created: 200, text: 'b2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row at boundary', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i3', created: 300, text: 'c'},
              row: {id: 'i3', created: 300, text: 'c2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row before boundary, changing its order', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i2', created: 200, text: 'b'},
              row: {id: 'i2', created: 50, text: 'b2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row after boundary to make it the new boundary', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i4', created: 400, text: 'd'},
              row: {id: 'i4', created: 250, text: 'd'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row before boundary to make it new boundary', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i2', created: 200, text: 'b'},
              row: {id: 'i2', created: 350, text: 'b2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row before boundary to fetch new boundary', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'i2', created: 200, text: 'b'},
              row: {id: 'i2', created: 450, text: 'b2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });
    });

    test('at limit 1', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        limit: 1,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i1', created: 100, text: 'a'},
            row: {id: 'i1', created: 50, text: 'a2'},
          },
        ],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });
  });
});

suite('take with partition', () => {
  const base = {
    columns: {
      id: {type: 'string'},
      issueID: {type: 'string'},
      created: {type: 'number'},
    },
    primaryKey: ['id'],
    sort: [
      ['created', 'asc'],
      ['id', 'asc'],
    ],
  } as const;

  suite('add', () => {
    test('limit 0', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        partition: {
          key: ['issueID'],
          values: [['i1'], ['i2']],
        },
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100},
          {id: 'c2', issueID: 'i1', created: 200},
          {id: 'c3', issueID: 'i1', created: 300},
        ],
        limit: 0,
        pushes: [{type: 'add', row: {id: 'c6', issueID: 'i2', created: 150}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('less than limit add row at start', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        partition: {
          key: ['issueID'],
          values: [['i1'], ['i2']],
        },
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100},
          {id: 'c2', issueID: 'i1', created: 200},
          {id: 'c3', issueID: 'i1', created: 300},
          {id: 'c4', issueID: 'i2', created: 400},
          {id: 'c5', issueID: 'i2', created: 500},
        ],
        limit: 5,
        pushes: [{type: 'add', row: {id: 'c6', issueID: 'i2', created: 150}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('at limit add row at end', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        partition: {
          key: ['issueID'],
          values: [['i1'], ['i2']],
        },
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100},
          {id: 'c2', issueID: 'i1', created: 200},
          // 580 to test that it constrains looking for previous
          // to constraint issueID: 'i2'
          {id: 'c3', issueID: 'i1', created: 580},
          {id: 'c4', issueID: 'i2', created: 400},
          {id: 'c5', issueID: 'i2', created: 500},
          {id: 'c6', issueID: 'i2', created: 600},
          {id: 'c7', issueID: 'i2', created: 700},
        ],
        limit: 3,
        pushes: [{type: 'add', row: {id: 'c8', issueID: 'i2', created: 550}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('add with non-fetched partition value', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        partition: {
          key: ['issueID'],
          values: [['i1'], ['i2']],
        },
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100},
          {id: 'c2', issueID: 'i1', created: 200},
          {id: 'c3', issueID: 'i1', created: 300},
          {id: 'c4', issueID: 'i2', created: 400},
          {id: 'c5', issueID: 'i2', created: 500},
        ],
        limit: 3,
        pushes: [{type: 'add', row: {id: 'c6', issueID: '3', created: 550}}],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });
  });

  suite('remove', () => {
    test('limit 0', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        partition: {
          key: ['issueID'],
          values: [['i1'], ['i2']],
        },
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100},
          {id: 'c2', issueID: 'i1', created: 200},
          {id: 'c3', issueID: 'i1', created: 300},
        ],
        limit: 0,
        pushes: [
          {type: 'remove', row: {id: 'c1', issueID: 'i1', created: 100}},
        ],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('less than limit remove row at start', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        partition: {
          key: ['issueID'],
          values: [['i1'], ['i2']],
        },
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100},
          {id: 'c2', issueID: 'i1', created: 200},
          {id: 'c3', issueID: 'i1', created: 300},
          {id: 'c4', issueID: 'i2', created: 400},
          {id: 'c5', issueID: 'i2', created: 500},
        ],
        limit: 5,
        pushes: [
          {type: 'remove', row: {id: 'c1', issueID: 'i1', created: 100}},
        ],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    test('remove row unfetched partition', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        partition: {
          key: ['issueID'],
          values: [['i1'], ['i2']],
        },
        sourceRows: [
          {id: 'c1', issueID: 'i1', created: 100},
          {id: 'c2', issueID: 'i1', created: 200},
          {id: 'c3', issueID: 'i1', created: 300},
          {id: 'c4', issueID: 'i2', created: 400},
          {id: 'c5', issueID: 'i2', created: 500},
          {id: 'c6', issueID: 'i3', created: 600},
        ],
        limit: 5,
        pushes: [
          {type: 'remove', row: {id: 'c6', issueID: 'i3', created: 600}},
        ],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });
  });

  suite('edit', () => {
    const base = {
      columns: {
        id: {type: 'string'},
        created: {type: 'number'},
        issueID: {type: 'string'},
        text: {type: 'string'},
      },
      primaryKey: ['id'],
      sort: [
        ['created', 'asc'],
        ['id', 'asc'],
      ],
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
        {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
        {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
        {id: 'c4', issueID: 'i2', created: 400, text: 'd'},
        {id: 'c5', issueID: 'i2', created: 500, text: 'e'},
      ],
      partition: {
        key: ['issueID'],
        values: [['i1'], ['i2']],
      },
    } as const;

    test('limit 0', () => {
      const {messages, storage, pushes} = takeTest({
        ...base,
        partition: {
          key: ['issueID'],
          values: [['i1'], ['i2']],
        },
        limit: 0,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
            row: {id: 'c2', issueID: 'i1', created: 200, text: 'b2'},
          },
        ],
      });
      expect(messages).toMatchInlineSnapshot();
      expect(storage).toMatchInlineSnapshot();
      expect(pushes).toMatchInlineSnapshot();
    });

    describe('less than limit ', () => {
      test('edit row at start', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 5,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
              row: {id: 'c1', issueID: 'i1', created: 100, text: 'a2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row at end', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 5,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c5', issueID: 'i2', created: 500, text: 'e'},
              row: {id: 'c5', issueID: 'i2', created: 500, text: 'e2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });
    });

    describe('at limit', () => {
      test('edit row after boundary', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 2,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              row: {id: 'c3', issueID: 'i1', created: 300, text: 'c2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row before boundary', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              row: {id: 'c2', issueID: 'i1', created: 200, text: 'b2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row at boundary', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              row: {id: 'c3', issueID: 'i1', created: 300, text: 'c2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row at boundary, making it not the boundary', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              row: {id: 'c3', issueID: 'i1', created: 150, text: 'c2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row at boundary, making it fall outside the window', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 2,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              row: {id: 'c2', issueID: 'i1', created: 350, text: 'b2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row before boundary, changing its order', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 3,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              row: {id: 'c2', issueID: 'i1', created: 50, text: 'b2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row after boundary to make it the new boundary', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 2,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              row: {id: 'c3', issueID: 'i1', created: 150, text: 'c2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row before boundary to make it new boundary', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 2,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
              row: {id: 'c1', issueID: 'i1', created: 250, text: 'a2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });

      test('edit row before boundary to fetch new boundary', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 2,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
              row: {id: 'c1', issueID: 'i1', created: 350, text: 'a2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });
    });

    describe('changing partition value', () => {
      test('move to from first partition to second', () => {
        const {messages, storage, pushes} = takeTest({
          ...base,
          limit: 2,
          pushes: [
            {
              type: 'edit',
              oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
              row: {id: 'c1', issueID: 'i2', created: 100, text: 'a2'},
            },
          ],
        });
        expect(messages).toMatchInlineSnapshot();
        expect(storage).toMatchInlineSnapshot();
        expect(pushes).toMatchInlineSnapshot();
      });
    });
  });
});

function takeTest(t: TakeTest): TakeTestReults {
  const log: SnitchMessage[] = [];
  const source = createSource('table', t.columns, t.primaryKey);
  for (const row of t.sourceRows) {
    source.push({type: 'add', row});
  }
  const snitch = new Snitch(
    source.connect(t.sort || [['id', 'asc']]),
    'takeSnitch',
    log,
  );
  const memoryStorage = new MemoryStorage();
  const partitionKey = t.partition?.key;

  const take = new Take(snitch, memoryStorage, t.limit, partitionKey);
  const c = new Catch(take);
  if (t.partition === undefined) {
    c.fetch();
  } else {
    assert(partitionKey);
    for (const partitionValue of t.partition.values) {
      c.fetch({
        constraint: Object.fromEntries(
          partitionKey.map((k, i) => [k, partitionValue[i]]),
        ),
      });
    }
  }
  expect(c.pushes).toEqual([]);
  log.length = 0;
  for (const change of t.pushes) {
    source.push(change);
  }

  return {
    messages: log,
    storage: memoryStorage.cloneData(),
    pushes: c.pushes,
  };
}

type TakeTest = {
  columns: Record<string, SchemaValue>;
  primaryKey: PrimaryKey;
  sourceRows: readonly Row[];
  sort?: Ordering | undefined;
  limit: number;
  partition:
    | {
        key: PartitionKey;
        values: readonly (readonly Value[])[];
      }
    | undefined;
  pushes: SourceChange[];
};

type TakeTestReults = {
  messages: SnitchMessage[];
  storage: Record<string, JSONValue>;
  pushes: CaughtChange[];
};
