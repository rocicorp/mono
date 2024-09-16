import {JSONValue} from 'shared/src/json.js';
import {describe, expect, suite, test} from 'vitest';
import {Ordering} from '../ast/ast.js';
import {Catch} from './catch.js';
import {Change} from './change.js';
import {Row, Value} from './data.js';
import {MemorySource} from './memory-source.js';
import {MemoryStorage} from './memory-storage.js';
import {PrimaryKey, SchemaValue} from './schema.js';
import {Snitch, SnitchMessage} from './snitch.js';
import {SourceChange} from './source.js';
import {Take} from './take.js';

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
    takeTest({
      ...base,
      name: 'limit 0',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 0,
      pushes: [{type: 'add', row: {id: 'i4', created: 50}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'add', row: {id: 'i4', created: 50}}],
      ],
      expectedStorage: {},
      expectedOutput: [],
    });

    takeTest({
      ...base,
      name: 'less than limit add row at start',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 5,
      pushes: [{type: 'add', row: {id: 'i4', created: 50}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'add', row: {id: 'i4', created: 50}}],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 300,
            id: 'i3',
          },
          size: 4,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [
        {type: 'add', node: {row: {id: 'i4', created: 50}, relationships: {}}},
      ],
    });

    takeTest({
      ...base,
      name: 'less than limit add row at end',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 5,
      pushes: [{type: 'add', row: {id: 'i4', created: 350}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'add', row: {id: 'i4', created: 350}}],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 350,
            id: 'i4',
          },
          size: 4,
        },
        'maxBound': {
          created: 350,
          id: 'i4',
        },
      },
      expectedOutput: [
        {type: 'add', node: {row: {id: 'i4', created: 350}, relationships: {}}},
      ],
    });

    takeTest({
      ...base,
      name: 'at limit add row after bound',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
      ],
      limit: 3,
      pushes: [{type: 'add', row: {id: 'i5', created: 350}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'add', row: {id: 'i5', created: 350}}],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 300,
            id: 'i3',
          },
          size: 3,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [],
    });

    takeTest({
      ...base,
      name: 'at limit add row at start',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
      ],
      limit: 3,
      pushes: [{type: 'add', row: {id: 'i5', created: 50}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'add', row: {id: 'i5', created: 50}}],
        [
          'takeSnitch',
          'fetch',
          {start: {basis: 'before', row: {id: 'i3', created: 300}}},
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 200,
            id: 'i2',
          },
          size: 3,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i3', created: 300}, relationships: {}},
        },
        {type: 'add', node: {row: {id: 'i5', created: 50}, relationships: {}}},
      ],
    });

    takeTest({
      ...base,
      name: 'at limit add row at end',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
      ],
      limit: 3,
      pushes: [{type: 'add', row: {id: 'i5', created: 250}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'add', row: {id: 'i5', created: 250}}],
        [
          'takeSnitch',
          'fetch',
          {start: {basis: 'before', row: {id: 'i3', created: 300}}},
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 250,
            id: 'i5',
          },
          size: 3,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i3', created: 300}, relationships: {}},
        },
        {type: 'add', node: {row: {id: 'i5', created: 250}, relationships: {}}},
      ],
    });
  });

  suite('remove', () => {
    takeTest({
      ...base,
      name: 'limit 0',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 0,
      pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i1', created: 100}}],
      ],
      expectedStorage: {},
      expectedOutput: [],
    });

    takeTest({
      ...base,
      name: 'less than limit remove row at start',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 5,
      pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i1', created: 100}}],
        [
          'takeSnitch',
          'fetch',
          {
            start: {
              basis: 'before',
              row: {
                created: 300,
                id: 'i3',
              },
            },
          },
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 300,
            id: 'i3',
          },
          size: 2,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i1', created: 100}, relationships: {}},
        },
      ],
    });

    takeTest({
      ...base,
      name: 'less than limit remove row at end',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 5,
      pushes: [{type: 'remove', row: {id: 'i3', created: 300}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i3', created: 300}}],
        [
          'takeSnitch',
          'fetch',
          {
            start: {
              basis: 'before',
              row: {
                created: 300,
                id: 'i3',
              },
            },
          },
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 200,
            id: 'i2',
          },
          size: 2,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i3', created: 300}, relationships: {}},
        },
      ],
    });

    takeTest({
      ...base,
      name: 'at limit remove row after bound',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
      ],
      limit: 3,
      pushes: [{type: 'remove', row: {id: 'i4', created: 400}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i4', created: 400}}],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 300,
            id: 'i3',
          },
          size: 3,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [],
    });

    takeTest({
      ...base,
      name: 'at limit remove row at start with row after',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
      ],
      limit: 3,
      pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i1', created: 100}}],
        [
          'takeSnitch',
          'fetch',
          {start: {basis: 'before', row: {id: 'i3', created: 300}}},
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 400,
            id: 'i4',
          },
          size: 3,
        },
        'maxBound': {
          created: 400,
          id: 'i4',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i1', created: 100}, relationships: {}},
        },
        {
          type: 'add',
          node: {row: {id: 'i4', created: 400}, relationships: {}},
        },
      ],
    });

    takeTest({
      ...base,
      name: 'at limit remove row at start no row after',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 3,
      pushes: [{type: 'remove', row: {id: 'i1', created: 100}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i1', created: 100}}],
        [
          'takeSnitch',
          'fetch',
          {start: {basis: 'before', row: {id: 'i3', created: 300}}},
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 300,
            id: 'i3',
          },
          size: 2,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i1', created: 100}, relationships: {}},
        },
      ],
    });

    takeTest({
      ...base,
      name: 'at limit remove row at end with row after',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
        {id: 'i4', created: 400},
      ],
      limit: 3,
      pushes: [{type: 'remove', row: {id: 'i3', created: 300}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i3', created: 300}}],
        [
          'takeSnitch',
          'fetch',
          {start: {basis: 'before', row: {id: 'i3', created: 300}}},
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 400,
            id: 'i4',
          },
          size: 3,
        },
        'maxBound': {
          created: 400,
          id: 'i4',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i3', created: 300}, relationships: {}},
        },
        {
          type: 'add',
          node: {row: {id: 'i4', created: 400}, relationships: {}},
        },
      ],
    });

    takeTest({
      ...base,
      name: 'at limit remove row at end, no row after',
      sourceRows: [
        {id: 'i1', created: 100},
        {id: 'i2', created: 200},
        {id: 'i3', created: 300},
      ],
      limit: 3,
      pushes: [{type: 'remove', row: {id: 'i3', created: 300}}],
      expectedMessages: [
        ['takeSnitch', 'push', {type: 'remove', row: {id: 'i3', created: 300}}],
        [
          'takeSnitch',
          'fetch',
          {start: {basis: 'before', row: {id: 'i3', created: 300}}},
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 200,
            id: 'i2',
          },
          size: 2,
        },
        'maxBound': {
          created: 300,
          id: 'i3',
        },
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {row: {id: 'i3', created: 300}, relationships: {}},
        },
      ],
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

    takeTest({
      ...base,
      name: 'limit 0',
      limit: 0,
      pushes: [
        {
          type: 'edit',
          oldRow: {id: 'i2', created: 200, text: 'b'},
          row: {id: 'i2', created: 200, text: 'c'},
        },
      ],
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {
            type: 'edit',
            oldRow: {id: 'i2', created: 200, text: 'b'},
            row: {id: 'i2', created: 200, text: 'c'},
          },
        ],
      ],
      expectedStorage: {},
      expectedOutput: [],
    });

    describe('less than limit ', () => {
      takeTest({
        ...base,
        name: 'edit row at start',
        limit: 5,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i1', created: 100, text: 'a'},
            row: {id: 'i1', created: 100, text: 'a2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'i1', created: 100, text: 'a'},
              row: {id: 'i1', created: 100, text: 'a2'},
            },
          ],
        ],
        expectedStorage: {
          '["take",null]': {
            bound: {
              created: 400,
              id: 'i4',
              text: 'd',
            },
            size: 4,
          },
          'maxBound': {
            created: 400,
            id: 'i4',
            text: 'd',
          },
        },
        expectedOutput: [
          {
            type: 'edit',
            oldRow: {id: 'i1', created: 100, text: 'a'},
            row: {id: 'i1', created: 100, text: 'a2'},
          },
        ],
      });

      takeTest({
        ...base,
        name: 'edit row at end',
        limit: 5,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i4', created: 400, text: 'd'},
            row: {id: 'i4', created: 400, text: 'd2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'i4', created: 400, text: 'd'},
              row: {id: 'i4', created: 400, text: 'd2'},
            },
          ],
        ],
        expectedStorage: {
          '["take",null]': {
            bound: {
              created: 400,
              id: 'i4',
              text: 'd',
            },
            size: 4,
          },
          'maxBound': {
            created: 400,
            id: 'i4',
            text: 'd',
          },
        },
        expectedOutput: [
          {
            type: 'edit',
            oldRow: {id: 'i4', created: 400, text: 'd'},
            row: {id: 'i4', created: 400, text: 'd2'},
          },
        ],
      });
    });

    describe('at limit', () => {
      takeTest({
        ...base,
        name: 'edit row after boundary',
        limit: 3,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i4', created: 400, text: 'd'},
            row: {id: 'i4', created: 400, text: 'd2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'i4', created: 400, text: 'd'},
              row: {id: 'i4', created: 400, text: 'd2'},
            },
          ],
        ],
        expectedStorage: {
          '["take",null]': {
            bound: {
              created: 300,
              id: 'i3',
              text: 'c',
            },
            size: 3,
          },
          'maxBound': {
            created: 300,
            id: 'i3',
            text: 'c',
          },
        },
        expectedOutput: [],
      });

      takeTest({
        ...base,
        name: 'edit row before boundary',
        limit: 3,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i2', created: 200, text: 'b'},
            row: {id: 'i2', created: 200, text: 'b2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'i2', created: 200, text: 'b'},
              row: {id: 'i2', created: 200, text: 'b2'},
            },
          ],
        ],
        expectedStorage: {
          '["take",null]': {
            bound: {
              created: 300,
              id: 'i3',
              text: 'c',
            },
            size: 3,
          },
          'maxBound': {
            created: 300,
            id: 'i3',
            text: 'c',
          },
        },
        expectedOutput: [
          {
            type: 'edit',
            oldRow: {id: 'i2', created: 200, text: 'b'},
            row: {id: 'i2', created: 200, text: 'b2'},
          },
        ],
      });

      takeTest({
        ...base,
        name: 'edit row at boundary',
        limit: 3,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i3', created: 300, text: 'c'},
            row: {id: 'i3', created: 300, text: 'c2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'i3', created: 300, text: 'c'},
              row: {id: 'i3', created: 300, text: 'c2'},
            },
          ],
        ],
        expectedStorage: {
          '["take",null]': {
            bound: {
              created: 300,
              id: 'i3',
              text: 'c',
            },
            size: 3,
          },
          'maxBound': {
            created: 300,
            id: 'i3',
            text: 'c',
          },
        },
        expectedOutput: [
          {
            type: 'edit',
            oldRow: {id: 'i3', created: 300, text: 'c'},
            row: {id: 'i3', created: 300, text: 'c2'},
          },
        ],
      });

      takeTest({
        ...base,
        name: 'edit row before boundary, changing its order',
        limit: 3,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i2', created: 200, text: 'b'},
            row: {id: 'i2', created: 50, text: 'b2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'i2', created: 200, text: 'b'},
              row: {id: 'i2', created: 50, text: 'b2'},
            },
          ],
        ],
        expectedStorage: {
          '["take",null]': {
            bound: {
              created: 300,
              id: 'i3',
              text: 'c',
            },
            size: 3,
          },
          'maxBound': {
            created: 300,
            id: 'i3',
            text: 'c',
          },
        },
        expectedOutput: [
          {
            type: 'edit',
            oldRow: {id: 'i2', created: 200, text: 'b'},
            row: {id: 'i2', created: 50, text: 'b2'},
          },
        ],
      });

      takeTest({
        ...base,
        name: 'edit row after boundary to make it the new boundary',
        limit: 3,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i4', created: 400, text: 'd'},
            row: {id: 'i4', created: 250, text: 'd'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'i4', created: 400, text: 'd'},
              row: {id: 'i4', created: 250, text: 'd'},
            },
          ],
          [
            'takeSnitch',
            'fetch',
            {
              constraint: undefined,
              start: {
                basis: 'before',
                row: {
                  id: 'i3',
                  created: 300,
                  text: 'c',
                },
              },
            },
          ],
        ],
        expectedStorage: {
          '["take",null]': {
            bound: {
              created: 250,
              id: 'i4',
              text: 'd',
            },
            size: 3,
          },
          'maxBound': {
            created: 300,
            id: 'i3',
            text: 'c',
          },
        },
        expectedOutput: [
          {
            type: 'remove',
            node: {row: {id: 'i3', created: 300, text: 'c'}, relationships: {}},
          },
          {
            type: 'add',
            node: {
              row: {id: 'i4', created: 250, text: 'd'},
              relationships: {},
            },
          },
        ],
      });

      takeTest({
        ...base,
        name: 'edit row before boundary to make it new boundary',
        limit: 3,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i2', created: 200, text: 'b'},
            row: {id: 'i2', created: 350, text: 'b2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'i2', created: 200, text: 'b'},
              row: {id: 'i2', created: 350, text: 'b2'},
            },
          ],
          [
            'takeSnitch',
            'fetch',
            {
              constraint: undefined,
              start: {
                basis: 'after',
                row: {
                  id: 'i3',
                  created: 300,
                  text: 'c',
                },
              },
            },
          ],
        ],
        expectedStorage: {
          '["take",null]': {
            bound: {
              created: 350,
              id: 'i2',
              text: 'b2',
            },
            size: 3,
          },
          'maxBound': {
            created: 350,
            id: 'i2',
            text: 'b2',
          },
        },
        expectedOutput: [
          {
            type: 'edit',
            oldRow: {id: 'i2', created: 200, text: 'b'},
            row: {id: 'i2', created: 350, text: 'b2'},
          },
        ],
      });

      takeTest({
        ...base,
        name: 'edit row before boundary to fetch new boundary',
        limit: 3,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'i2', created: 200, text: 'b'},
            row: {id: 'i2', created: 450, text: 'b2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'i2', created: 200, text: 'b'},
              row: {id: 'i2', created: 450, text: 'b2'},
            },
          ],
          [
            'takeSnitch',
            'fetch',
            {
              constraint: undefined,
              start: {
                basis: 'after',
                row: {
                  id: 'i3',
                  created: 300,
                  text: 'c',
                },
              },
            },
          ],
        ],
        expectedStorage: {
          '["take",null]': {
            bound: {
              created: 400,
              id: 'i4',
              text: 'd',
            },
            size: 3,
          },
          'maxBound': {
            created: 400,
            id: 'i4',
            text: 'd',
          },
        },
        expectedOutput: [
          {
            type: 'remove',
            node: {
              row: {id: 'i2', created: 200, text: 'b'},
              relationships: {},
            },
          },
          {
            type: 'add',
            node: {
              row: {id: 'i4', created: 400, text: 'd'},
              relationships: {},
            },
          },
        ],
      });
    });

    takeTest({
      ...base,
      name: 'at limit 1',
      limit: 1,
      pushes: [
        {
          type: 'edit',
          oldRow: {id: 'i1', created: 100, text: 'a'},
          row: {id: 'i1', created: 50, text: 'a2'},
        },
      ],
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {
            type: 'edit',
            oldRow: {id: 'i1', created: 100, text: 'a'},
            row: {id: 'i1', created: 50, text: 'a2'},
          },
        ],
      ],
      expectedStorage: {
        '["take",null]': {
          bound: {
            created: 50,
            id: 'i1',
            text: 'a2',
          },
          size: 1,
        },
        'maxBound': {
          created: 100,
          id: 'i1',
          text: 'a',
        },
      },
      expectedOutput: [
        {
          oldRow: {
            created: 100,
            id: 'i1',
            text: 'a',
          },
          row: {
            created: 50,
            id: 'i1',
            text: 'a2',
          },
          type: 'edit',
        },
      ],
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
    takeTest({
      ...base,
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
      },
      name: 'limit 0',
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
      ],
      limit: 0,
      pushes: [{type: 'add', row: {id: 'c6', issueID: 'i2', created: 150}}],
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {type: 'add', row: {id: 'c6', issueID: 'i2', created: 150}},
        ],
      ],
      expectedStorage: {},
      expectedOutput: [],
    });

    takeTest({
      ...base,
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
      },
      name: 'less than limit add row at start',
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
        {id: 'c4', issueID: 'i2', created: 400},
        {id: 'c5', issueID: 'i2', created: 500},
      ],
      limit: 5,
      pushes: [{type: 'add', row: {id: 'c6', issueID: 'i2', created: 150}}],
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {type: 'add', row: {id: 'c6', issueID: 'i2', created: 150}},
        ],
      ],
      expectedStorage: {
        '["take","i1"]': {
          bound: {id: 'c3', issueID: 'i1', created: 300},
          size: 3,
        },
        '["take","i2"]': {
          bound: {id: 'c5', issueID: 'i2', created: 500},
          size: 3,
        },
        'maxBound': {id: 'c5', issueID: 'i2', created: 500},
      },
      expectedOutput: [
        {
          type: 'add',
          node: {
            row: {id: 'c6', issueID: 'i2', created: 150},
            relationships: {},
          },
        },
      ],
    });

    takeTest({
      ...base,
      name: 'at limit add row at end',
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
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
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {type: 'add', row: {id: 'c8', issueID: 'i2', created: 550}},
        ],
        [
          'takeSnitch',
          'fetch',
          {
            constraint: {
              key: 'issueID',
              value: 'i2',
            },
            start: {
              basis: 'before',
              row: {id: 'c6', issueID: 'i2', created: 600},
            },
          },
        ],
      ],
      expectedStorage: {
        '["take","i1"]': {
          bound: {id: 'c3', issueID: 'i1', created: 580},
          size: 3,
        },
        '["take","i2"]': {
          bound: {id: 'c8', issueID: 'i2', created: 550},
          size: 3,
        },
        'maxBound': {id: 'c6', issueID: 'i2', created: 600},
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {
            row: {id: 'c6', issueID: 'i2', created: 600},
            relationships: {},
          },
        },
        {
          type: 'add',
          node: {
            row: {id: 'c8', issueID: 'i2', created: 550},
            relationships: {},
          },
        },
      ],
    });

    takeTest({
      ...base,
      name: 'add with non-fetched partition value',
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
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
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {type: 'add', row: {id: 'c6', issueID: '3', created: 550}},
        ],
      ],
      expectedStorage: {
        '["take","i1"]': {
          bound: {id: 'c3', issueID: 'i1', created: 300},
          size: 3,
        },
        '["take","i2"]': {
          bound: {id: 'c5', issueID: 'i2', created: 500},
          size: 2,
        },
        'maxBound': {id: 'c5', issueID: 'i2', created: 500},
      },
      expectedOutput: [],
    });
  });

  suite('remove', () => {
    takeTest({
      ...base,
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
      },
      name: 'limit 0',
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
      ],
      limit: 0,
      pushes: [{type: 'remove', row: {id: 'c1', issueID: 'i1', created: 100}}],
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {type: 'remove', row: {id: 'c1', issueID: 'i1', created: 100}},
        ],
      ],
      expectedStorage: {},
      expectedOutput: [],
    });

    takeTest({
      ...base,
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
      },
      name: 'less than limit remove row at start',
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
        {id: 'c4', issueID: 'i2', created: 400},
        {id: 'c5', issueID: 'i2', created: 500},
      ],
      limit: 5,
      pushes: [{type: 'remove', row: {id: 'c1', issueID: 'i1', created: 100}}],
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {type: 'remove', row: {id: 'c1', issueID: 'i1', created: 100}},
        ],
        [
          'takeSnitch',
          'fetch',
          {
            constraint: {
              key: 'issueID',
              value: 'i1',
            },
            start: {
              basis: 'before',
              row: {id: 'c3', issueID: 'i1', created: 300},
            },
          },
        ],
      ],
      expectedStorage: {
        '["take","i1"]': {
          bound: {id: 'c3', issueID: 'i1', created: 300},
          size: 2,
        },
        '["take","i2"]': {
          bound: {id: 'c5', issueID: 'i2', created: 500},
          size: 2,
        },
        'maxBound': {id: 'c5', issueID: 'i2', created: 500},
      },
      expectedOutput: [
        {
          type: 'remove',
          node: {
            row: {id: 'c1', issueID: 'i1', created: 100},
            relationships: {},
          },
        },
      ],
    });

    takeTest({
      ...base,
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
      },
      name: 'remove row unfetched partition',
      sourceRows: [
        {id: 'c1', issueID: 'i1', created: 100},
        {id: 'c2', issueID: 'i1', created: 200},
        {id: 'c3', issueID: 'i1', created: 300},
        {id: 'c4', issueID: 'i2', created: 400},
        {id: 'c5', issueID: 'i2', created: 500},
        {id: 'c6', issueID: 'i3', created: 600},
      ],
      limit: 5,
      pushes: [{type: 'remove', row: {id: 'c6', issueID: 'i3', created: 600}}],
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {type: 'remove', row: {id: 'c6', issueID: 'i3', created: 600}},
        ],
      ],
      expectedStorage: {
        '["take","i1"]': {
          bound: {id: 'c3', issueID: 'i1', created: 300},
          size: 3,
        },
        '["take","i2"]': {
          bound: {id: 'c5', issueID: 'i2', created: 500},
          size: 2,
        },
        'maxBound': {id: 'c5', issueID: 'i2', created: 500},
      },
      expectedOutput: [],
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
        {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
        {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
        {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
        {id: 'c4', issueID: 'i2', created: 400, text: 'd'},
        {id: 'c5', issueID: 'i2', created: 500, text: 'e'},
      ],
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
      },
    } as const;

    takeTest({
      ...base,
      name: 'limit 0',
      partition: {
        key: 'issueID',
        values: ['i1', 'i2'],
      },
      limit: 0,
      pushes: [
        {
          type: 'edit',
          oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
          row: {id: 'c2', issueID: 'i1', created: 200, text: 'b2'},
        },
      ],
      expectedMessages: [
        [
          'takeSnitch',
          'push',
          {
            type: 'edit',
            oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
            row: {id: 'c2', issueID: 'i1', created: 200, text: 'b2'},
          },
        ],
      ],
      expectedStorage: {},
      expectedOutput: [],
    });

    describe('less than limit ', () => {
      takeTest({
        ...base,
        name: 'edit row at start',
        limit: 5,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
            row: {id: 'c1', issueID: 'i1', created: 100, text: 'a2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
              row: {id: 'c1', issueID: 'i1', created: 100, text: 'a2'},
            },
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {
              created: 300,
              id: 'c3',
              issueID: 'i1',
              text: 'c',
            },
            size: 3,
          },
          '["take","i2"]': {
            bound: {
              created: 500,
              id: 'c5',
              issueID: 'i2',
              text: 'e',
            },
            size: 2,
          },
          'maxBound': {
            created: 500,
            id: 'c5',
            issueID: 'i2',
            text: 'e',
          },
        },
        expectedOutput: [
          {
            type: 'edit',
            oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
            row: {id: 'c1', issueID: 'i1', created: 100, text: 'a2'},
          },
        ],
      });

      takeTest({
        ...base,
        name: 'edit row at end',
        limit: 5,
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c5', issueID: 'i2', created: 500, text: 'e'},
            row: {id: 'c5', issueID: 'i2', created: 500, text: 'e2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'c5', issueID: 'i2', created: 500, text: 'e'},
              row: {id: 'c5', issueID: 'i2', created: 500, text: 'e2'},
            },
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {
              created: 300,
              id: 'c3',
              issueID: 'i1',
              text: 'c',
            },
            size: 3,
          },
          '["take","i2"]': {
            bound: {
              created: 500,
              id: 'c5',
              issueID: 'i2',
              text: 'e',
            },
            size: 2,
          },
          'maxBound': {
            created: 500,
            id: 'c5',
            issueID: 'i2',
            text: 'e',
          },
        },
        expectedOutput: [
          {
            type: 'edit',
            oldRow: {id: 'c5', issueID: 'i2', created: 500, text: 'e'},
            row: {id: 'c5', issueID: 'i2', created: 500, text: 'e2'},
          },
        ],
      });
    });

    describe('at limit', () => {
      takeTest({
        ...base,
        limit: 2,
        name: 'edit row after boundary',
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
            row: {id: 'c3', issueID: 'i1', created: 300, text: 'c2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              row: {id: 'c3', issueID: 'i1', created: 300, text: 'c2'},
            },
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {
              created: 200,
              id: 'c2',
              issueID: 'i1',
              text: 'b',
            },
            size: 2,
          },
          '["take","i2"]': {
            bound: {
              created: 500,
              id: 'c5',
              issueID: 'i2',
              text: 'e',
            },
            size: 2,
          },
          'maxBound': {
            created: 500,
            id: 'c5',
            issueID: 'i2',
            text: 'e',
          },
        },
        expectedOutput: [],
      });

      takeTest({
        ...base,
        limit: 3,
        name: 'edit row before boundary',
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
            row: {id: 'c2', issueID: 'i1', created: 200, text: 'b2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              row: {id: 'c2', issueID: 'i1', created: 200, text: 'b2'},
            },
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {
              created: 300,
              id: 'c3',
              issueID: 'i1',
              text: 'c',
            },
            size: 3,
          },
          '["take","i2"]': {
            bound: {
              created: 500,
              id: 'c5',
              issueID: 'i2',
              text: 'e',
            },
            size: 2,
          },
          'maxBound': {
            created: 500,
            id: 'c5',
            issueID: 'i2',
            text: 'e',
          },
        },
        expectedOutput: [
          {
            type: 'edit',
            oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
            row: {id: 'c2', issueID: 'i1', created: 200, text: 'b2'},
          },
        ],
      });

      takeTest({
        ...base,
        limit: 3,
        name: 'edit row at boundary',
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
            row: {id: 'c3', issueID: 'i1', created: 300, text: 'c2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              row: {id: 'c3', issueID: 'i1', created: 300, text: 'c2'},
            },
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {
              created: 300,
              id: 'c3',
              issueID: 'i1',
              text: 'c',
            },
            size: 3,
          },
          '["take","i2"]': {
            bound: {
              created: 500,
              id: 'c5',
              issueID: 'i2',
              text: 'e',
            },
            size: 2,
          },
          'maxBound': {
            created: 500,
            id: 'c5',
            issueID: 'i2',
            text: 'e',
          },
        },
        expectedOutput: [
          {
            type: 'edit',
            oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
            row: {id: 'c3', issueID: 'i1', created: 300, text: 'c2'},
          },
        ],
      });

      takeTest({
        ...base,
        limit: 3,
        name: 'edit row at boundary, making it not the boundary',
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
            row: {id: 'c3', issueID: 'i1', created: 150, text: 'c2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              row: {id: 'c3', issueID: 'i1', created: 150, text: 'c2'},
            },
          ],
          [
            'takeSnitch',
            'fetch',
            {
              constraint: {
                key: 'issueID',
                value: 'i1',
              },
              start: {
                basis: 'before',
                row: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              },
            },
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {
              created: 200,
              id: 'c2',
              issueID: 'i1',
              text: 'b',
            },
            size: 3,
          },
          '["take","i2"]': {
            bound: {
              created: 500,
              id: 'c5',
              issueID: 'i2',
              text: 'e',
            },
            size: 2,
          },
          'maxBound': {
            created: 500,
            id: 'c5',
            issueID: 'i2',
            text: 'e',
          },
        },
        expectedOutput: [
          {
            type: 'edit',
            oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
            row: {id: 'c3', issueID: 'i1', created: 150, text: 'c2'},
          },
        ],
      });

      takeTest({
        ...base,
        limit: 2,
        name: 'edit row at boundary, making it fall outside the window',
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
            row: {id: 'c2', issueID: 'i1', created: 350, text: 'b2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              row: {id: 'c2', issueID: 'i1', created: 350, text: 'b2'},
            },
          ],
          [
            'takeSnitch',
            'fetch',
            {
              constraint: {
                key: 'issueID',
                value: 'i1',
              },
              start: {
                basis: 'at',
                row: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              },
            },
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {
              created: 300,
              id: 'c3',
              issueID: 'i1',
              text: 'c',
            },
            size: 2,
          },
          '["take","i2"]': {
            bound: {
              created: 500,
              id: 'c5',
              issueID: 'i2',
              text: 'e',
            },
            size: 2,
          },
          'maxBound': {
            created: 500,
            id: 'c5',
            issueID: 'i2',
            text: 'e',
          },
        },
        expectedOutput: [
          {
            type: 'remove',
            node: {
              row: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              relationships: {},
            },
          },
          {
            type: 'add',
            node: {
              row: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              relationships: {},
            },
          },
        ],
      });

      takeTest({
        ...base,
        limit: 3,
        name: 'edit row before boundary, changing its order',
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
            row: {id: 'c2', issueID: 'i1', created: 50, text: 'b2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              row: {id: 'c2', issueID: 'i1', created: 50, text: 'b2'},
            },
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {
              created: 300,
              id: 'c3',
              issueID: 'i1',
              text: 'c',
            },
            size: 3,
          },
          '["take","i2"]': {
            bound: {
              created: 500,
              id: 'c5',
              issueID: 'i2',
              text: 'e',
            },
            size: 2,
          },
          'maxBound': {
            created: 500,
            id: 'c5',
            issueID: 'i2',
            text: 'e',
          },
        },
        expectedOutput: [
          {
            type: 'edit',
            oldRow: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
            row: {id: 'c2', issueID: 'i1', created: 50, text: 'b2'},
          },
        ],
      });

      takeTest({
        ...base,
        limit: 2,
        name: 'edit row after boundary to make it the new boundary',
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
            row: {id: 'c3', issueID: 'i1', created: 150, text: 'c2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              row: {id: 'c3', issueID: 'i1', created: 150, text: 'c2'},
            },
          ],
          [
            'takeSnitch',
            'fetch',
            {
              constraint: {
                key: 'issueID',
                value: 'i1',
              },
              start: {
                basis: 'before',
                row: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              },
            },
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {
              created: 150,
              id: 'c3',
              issueID: 'i1',
              text: 'c2',
            },
            size: 2,
          },
          '["take","i2"]': {
            bound: {
              created: 500,
              id: 'c5',
              issueID: 'i2',
              text: 'e',
            },
            size: 2,
          },
          'maxBound': {
            created: 500,
            id: 'c5',
            issueID: 'i2',
            text: 'e',
          },
        },
        expectedOutput: [
          {
            type: 'remove',
            node: {
              row: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              relationships: {},
            },
          },
          {
            type: 'add',
            node: {
              row: {id: 'c3', issueID: 'i1', created: 150, text: 'c2'},
              relationships: {},
            },
          },
        ],
      });

      takeTest({
        ...base,
        limit: 2,
        name: 'edit row before boundary to make it new boundary',
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
            row: {id: 'c1', issueID: 'i1', created: 250, text: 'a2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
              row: {id: 'c1', issueID: 'i1', created: 250, text: 'a2'},
            },
          ],
          [
            'takeSnitch',
            'fetch',
            {
              constraint: {
                key: 'issueID',
                value: 'i1',
              },
              start: {
                basis: 'after',
                row: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              },
            },
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {
              created: 250,
              id: 'c1',
              issueID: 'i1',
              text: 'a2',
            },
            size: 2,
          },
          '["take","i2"]': {
            bound: {
              created: 500,
              id: 'c5',
              issueID: 'i2',
              text: 'e',
            },
            size: 2,
          },
          'maxBound': {
            created: 500,
            id: 'c5',
            issueID: 'i2',
            text: 'e',
          },
        },
        expectedOutput: [
          {
            type: 'edit',
            oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
            row: {id: 'c1', issueID: 'i1', created: 250, text: 'a2'},
          },
        ],
      });

      takeTest({
        ...base,
        limit: 2,
        name: 'edit row before boundary to fetch new boundary',
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
            row: {id: 'c1', issueID: 'i1', created: 350, text: 'a2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
              row: {id: 'c1', issueID: 'i1', created: 350, text: 'a2'},
            },
          ],
          [
            'takeSnitch',
            'fetch',
            {
              constraint: {
                key: 'issueID',
                value: 'i1',
              },
              start: {
                basis: 'after',
                row: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              },
            },
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {
              created: 300,
              id: 'c3',
              issueID: 'i1',
              text: 'c',
            },
            size: 2,
          },
          '["take","i2"]': {
            bound: {
              created: 500,
              id: 'c5',
              issueID: 'i2',
              text: 'e',
            },
            size: 2,
          },
          'maxBound': {
            created: 500,
            id: 'c5',
            issueID: 'i2',
            text: 'e',
          },
        },
        expectedOutput: [
          {
            type: 'remove',
            node: {
              row: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
              relationships: {},
            },
          },
          {
            type: 'add',
            node: {
              row: {id: 'c3', issueID: 'i1', created: 300, text: 'c'},
              relationships: {},
            },
          },
        ],
      });
    });

    describe('changing partition value', () => {
      takeTest({
        ...base,
        limit: 2,
        name: 'move to from first partition to second',
        pushes: [
          {
            type: 'edit',
            oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
            row: {id: 'c1', issueID: 'i2', created: 100, text: 'a2'},
          },
        ],
        expectedMessages: [
          [
            'takeSnitch',
            'push',
            {
              type: 'edit',
              oldRow: {id: 'c1', issueID: 'i1', created: 100, text: 'a'},
              row: {id: 'c1', issueID: 'i2', created: 100, text: 'a2'},
            },
          ],
          [
            'takeSnitch',
            'fetch',
            {
              constraint: {
                key: 'issueID',
                value: 'i1',
              },
              start: {
                basis: 'before',
                row: {id: 'c2', issueID: 'i1', created: 200, text: 'b'},
              },
            },
          ],
          [
            'takeSnitch',
            'fetch',
            {
              constraint: {
                key: 'issueID',
                value: 'i2',
              },
              start: {
                basis: 'before',
                row: {id: 'c5', issueID: 'i2', created: 500, text: 'e'},
              },
            },
          ],
        ],
        expectedStorage: {
          '["take","i1"]': {
            bound: {
              created: 300,
              id: 'c3',
              issueID: 'i1',
              text: 'c',
            },
            size: 2,
          },
          '["take","i2"]': {
            bound: {
              created: 400,
              id: 'c4',
              issueID: 'i2',
              text: 'd',
            },
            size: 2,
          },
          'maxBound': {
            created: 500,
            id: 'c5',
            issueID: 'i2',
            text: 'e',
          },
        },
        expectedOutput: [
          {
            type: 'remove',
            node: {
              row: {
                created: 100,
                id: 'c1',
                issueID: 'i1',
                text: 'a',
              },
              relationships: {},
            },
          },
          {
            type: 'add',
            node: {
              row: {
                created: 300,
                id: 'c3',
                issueID: 'i1',
                text: 'c',
              },
              relationships: {},
            },
          },
          {
            type: 'remove',
            node: {
              row: {
                created: 500,
                id: 'c5',
                issueID: 'i2',
                text: 'e',
              },
              relationships: {},
            },
          },
          {
            type: 'add',
            node: {
              row: {
                created: 100,
                id: 'c1',
                issueID: 'i2',
                text: 'a2',
              },
              relationships: {},
            },
          },
        ],
      });
    });
  });
});

function takeTest(t: TakeTest) {
  test(t.name, () => {
    const log: SnitchMessage[] = [];
    const source = new MemorySource('table', t.columns, t.primaryKey);
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
    for (const partitionValue of t.partition?.values || [undefined]) {
      c.fetch(
        partitionKey && partitionValue
          ? {
              constraint: {
                key: partitionKey,
                value: partitionValue,
              },
            }
          : undefined,
      );
      expect(c.pushes).toEqual([]);
    }
    log.length = 0;
    for (const change of t.pushes) {
      source.push(change);
    }
    expect(log).toEqual(t.expectedMessages);
    expect(memoryStorage.cloneData()).toEqual(t.expectedStorage);
    expect(c.pushes).toEqual(t.expectedOutput);
  });
}

type TakeTest = {
  name: string;
  columns: Record<string, SchemaValue>;
  primaryKey: PrimaryKey;
  sourceRows: readonly Row[];
  sort?: Ordering | undefined;
  limit: number;
  partition:
    | {
        key: string;
        values: readonly Value[];
      }
    | undefined;
  pushes: SourceChange[];
  expectedMessages: SnitchMessage[];
  expectedStorage: Record<string, JSONValue>;
  expectedOutput: Change[];
};
