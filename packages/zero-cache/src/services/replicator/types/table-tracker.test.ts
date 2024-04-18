import {describe, expect, test} from 'vitest';
import {
  EffectiveRowChange,
  TableTracker,
  type RowChange,
} from './table-tracker.js';

describe('types/table-tracker', () => {
  type Case = {
    name: string;
    changes: (RowChange | 'truncate')[];
    expectTruncated?: boolean;
    expected: EffectiveRowChange[];
  };

  const cases: Case[] = [
    {
      name: 'insert',
      changes: [
        {preValue: 'none', postRowKey: {id: 1}, postValue: {id: 1, foo: 'bar'}},
      ],
      expected: [
        {rowKey: {id: 1}, preValue: 'none', postValue: {id: 1, foo: 'bar'}},
      ],
    },
    {
      name: 'update',
      changes: [
        {
          preValue: 'unknown',
          postRowKey: {id: 1},
          postValue: {id: 1, foo: 'bar'},
        },
      ],
      expected: [
        {rowKey: {id: 1}, preValue: 'unknown', postValue: {id: 1, foo: 'bar'}},
      ],
    },
    {
      name: 'insert, update',
      changes: [
        {preValue: 'none', postRowKey: {id: 1}, postValue: {id: 1, foo: 'bar'}},
        {
          preValue: 'unknown',
          postRowKey: {id: 1},
          postValue: {id: 1, foo: 'bar'}, // Doesn't matter that it's an equivalent value.
        },
      ],
      expected: [
        {rowKey: {id: 1}, preValue: 'none', postValue: {id: 1, foo: 'bar'}},
      ],
    },
    {
      name: 'delete',
      changes: [{preValue: 'unknown', postRowKey: {id: 1}, postValue: 'none'}],
      expected: [{rowKey: {id: 1}, preValue: 'unknown', postValue: 'none'}],
    },
    {
      name: 'insert, delete',
      changes: [
        {preValue: 'none', postRowKey: {id: 1}, postValue: {id: 1, foo: 'bar'}},
        {preValue: 'unknown', postRowKey: {id: 1}, postValue: 'none'},
      ],
      expected: [], // No effective changes.
    },
    {
      name: 'insert, update, delete',
      changes: [
        {preValue: 'none', postRowKey: {id: 1}, postValue: {id: 1, foo: 'bar'}},
        {
          preValue: 'unknown',
          postRowKey: {id: 1},
          postValue: {id: 1, foo: 'bonk'},
        },
        {preValue: 'unknown', postRowKey: {id: 1}, postValue: 'none'},
      ],
      expected: [], // No effective changes.
    },
    {
      name: 'insert, delete, insert',
      changes: [
        {preValue: 'none', postRowKey: {id: 1}, postValue: {id: 1, foo: 'bar'}},
        {preValue: 'unknown', postRowKey: {id: 1}, postValue: 'none'},
        {
          preValue: 'none',
          postRowKey: {id: 1},
          postValue: {id: 1, foo: 'bonk'},
        },
      ],
      expected: [
        {rowKey: {id: 1}, preValue: 'none', postValue: {id: 1, foo: 'bonk'}},
      ],
    },
    {
      name: 'delete, insert',
      changes: [
        {preValue: 'unknown', postRowKey: {id: 1}, postValue: 'none'},
        {preValue: 'none', postRowKey: {id: 1}, postValue: {id: 1, foo: 'bar'}},
      ],
      expected: [
        {rowKey: {id: 1}, preValue: 'unknown', postValue: {id: 1, foo: 'bar'}},
      ],
    },
    {
      name: 'update, delete',
      changes: [
        {
          preValue: 'unknown',
          postRowKey: {id: 1},
          postValue: {id: 1, foo: 'bar'},
        },
        {preValue: 'unknown', postRowKey: {id: 1}, postValue: 'none'},
      ],
      expected: [{rowKey: {id: 1}, preValue: 'unknown', postValue: 'none'}],
    },
    {
      name: 'update with row key change',
      changes: [
        {
          preValue: 'unknown',
          preRowKey: {id: 2},
          postRowKey: {id: 1},
          postValue: {id: 1, foo: 'bar'},
        },
      ],
      expected: [
        // Effectively an INSERT of the new row.
        {rowKey: {id: 1}, preValue: 'none', postValue: {id: 1, foo: 'bar'}},
        // Effectively a DELETE of the old row.
        {rowKey: {id: 2}, preValue: 'unknown', postValue: 'none'},
      ],
    },
    {
      name: 'update to new row key, then back to the old one',
      changes: [
        {
          preValue: 'unknown',
          preRowKey: {id: 2},
          postRowKey: {id: 1},
          postValue: {id: 1, foo: 'bar'},
        },
        {
          preValue: 'unknown',
          preRowKey: {id: 1},
          postRowKey: {id: 2},
          postValue: {id: 2, foo: 'bar'},
        },
      ],
      expected: [
        // Effectively an UPDATE of the old row. The new row was ephemeral.
        {rowKey: {id: 2}, preValue: 'unknown', postValue: {id: 2, foo: 'bar'}},
      ],
    },
    {
      name: 'update with row key change, re-insert of old row, delete of new row',
      changes: [
        {
          preValue: 'unknown',
          preRowKey: {id: 2},
          postRowKey: {id: 1},
          postValue: {id: 1, foo: 'bar'},
        },
        {
          preValue: 'none',
          postRowKey: {id: 2},
          postValue: {id: 2, foo: 'boo'},
        },
        {
          preValue: 'unknown',
          postRowKey: {id: 1},
          postValue: 'none',
        },
      ],
      expected: [
        // Effectively an UPDATE of the old row.
        {rowKey: {id: 2}, preValue: 'unknown', postValue: {id: 2, foo: 'boo'}},
      ],
    },
    {
      name: 'truncate with subsequent actions',
      changes: [
        {preValue: 'none', postRowKey: {id: 1}, postValue: {id: 1, foo: 'bar'}},
        {preValue: 'none', postRowKey: {id: 2}, postValue: {id: 2, foo: 'boo'}},
        {
          preValue: 'unknown',
          preRowKey: {id: 2},
          postRowKey: {id: 3},
          postValue: {id: 3, foo: 'boo'},
        },
        'truncate',
        {
          preValue: 'none',
          postRowKey: {id: 1},
          postValue: {id: 1, foo: 'bonk'},
        },
        {
          preValue: 'unknown',
          postRowKey: {id: 4},
          postValue: {id: 4, foo: 'foo'},
        },
      ],
      expectTruncated: true,
      expected: [
        {rowKey: {id: 1}, preValue: 'none', postValue: {id: 1, foo: 'bonk'}},
        {
          rowKey: {id: 4},
          preValue: 'unknown',
          postValue: {id: 4, foo: 'foo'},
        },
      ],
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      const tracker = new TableTracker('public', 'foo', {});
      for (const change of c.changes) {
        if (change === 'truncate') {
          tracker.truncate();
        } else {
          tracker.add(change);
        }
      }
      const {truncated, changes} = tracker.getEffectiveRowChanges();
      expect(truncated).toBe(c.expectTruncated ?? false);
      expect([...changes.values()]).toEqual(c.expected);
    });
  }
});
