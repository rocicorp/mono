import {expect, test} from 'vitest';
import {Catch} from './catch.js';
import {Row} from './data.js';
import {MemorySource} from './memory-source.js';
import {Start} from './operator.js';
import {Bound, Skip} from './skip.js';
import {SourceChange} from './source.js';

test('fetch', () => {
  const cases: {
    skipBound: Bound;
    fetchBound: Start | undefined;
    expected: Row[];
  }[] = [
    {
      skipBound: {row: {startDate: '2023-03-31', id: 5}, exclusive: false},
      fetchBound: undefined,
      expected: [
        {id: 5, name: 'Alex', startDate: '2023-04-01'},
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-03-31', id: 5}, exclusive: true},
      fetchBound: undefined,
      expected: [
        {id: 5, name: 'Alex', startDate: '2023-04-01'},
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
      fetchBound: undefined,
      expected: [
        {id: 5, name: 'Alex', startDate: '2023-04-01'},
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: true},
      fetchBound: undefined,
      expected: [
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-02', id: 4}, exclusive: false},
      fetchBound: undefined,
      expected: [
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-02', id: 4}, exclusive: true},
      fetchBound: undefined,
      expected: [
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
      fetchBound: {row: {startDate: '2023-03-30', id: 5}, basis: 'before'},
      expected: [
        {id: 5, name: 'Alex', startDate: '2023-04-01'},
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
      fetchBound: {row: {startDate: '2023-03-30', id: 5}, basis: 'at'},
      expected: [
        {id: 5, name: 'Alex', startDate: '2023-04-01'},
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
      fetchBound: {row: {startDate: '2023-03-30', id: 5}, basis: 'after'},
      expected: [
        {id: 5, name: 'Alex', startDate: '2023-04-01'},
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
      fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'before'},
      expected: [
        {id: 5, name: 'Alex', startDate: '2023-04-01'},
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
      fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
      expected: [
        {id: 5, name: 'Alex', startDate: '2023-04-01'},
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
      fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'after'},
      expected: [
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: true},
      fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
      expected: [
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-02', id: 5}, exclusive: false},
      fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
      expected: [
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-02', id: 5}, exclusive: true},
      fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
      expected: [
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-02', id: 5}, exclusive: true},
      fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'after'},
      expected: [
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-02', id: 5}, exclusive: true},
      fetchBound: {row: {startDate: '2023-04-02', id: 5}, basis: 'after'},
      expected: [
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
      fetchBound: {row: {startDate: '2023-04-02', id: 5}, basis: 'before'},
      expected: [
        {id: 5, name: 'Alex', startDate: '2023-04-01'},
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
      fetchBound: {row: {startDate: '2023-09-02', id: 6}, basis: 'before'},
      expected: [
        {id: 6, name: 'Darick', startDate: '2023-09-01'},
        {id: 7, name: 'Matt', startDate: '2024-06-01'},
      ],
    },
    {
      skipBound: {row: {startDate: '2023-04-02', id: 5}, exclusive: true},
      fetchBound: {row: {startDate: '2030-04-02', id: 5}, basis: 'after'},
      expected: [],
    },
  ];

  for (const c of cases) {
    const ms = new MemorySource(
      'users',
      {
        id: {type: 'number'},
        name: {type: 'string'},
        startDate: {type: 'string'},
      },
      ['id'],
    );

    ms.push({
      type: 'add',
      row: {id: 1, name: 'Aaron', startDate: '2019-06-18'},
    });
    ms.push({
      type: 'add',
      row: {id: 2, name: 'Erik', startDate: '2020-08-01'},
    });
    ms.push({
      type: 'add',
      row: {id: 3, name: 'Greg', startDate: '2021-12-07'},
    });
    ms.push({
      type: 'add',
      row: {id: 4, name: 'Cesar', startDate: '2022-12-01'},
    });
    ms.push({
      type: 'add',
      row: {id: 5, name: 'Alex', startDate: '2023-04-01'},
    });
    ms.push({
      type: 'add',
      row: {id: 6, name: 'Darick', startDate: '2023-09-01'},
    });
    ms.push({
      type: 'add',
      row: {id: 7, name: 'Matt', startDate: '2024-06-01'},
    });

    const conn = ms.connect([
      ['startDate', 'asc'],
      ['id', 'asc'],
    ]);

    const skip = new Skip(conn, c.skipBound);
    const out = new Catch(skip);
    expect(out.fetch({start: c.fetchBound})).toEqual(
      c.expected.map(row => ({row, relationships: {}})),
    );
  }
});

test('push', () => {
  const cases: {
    skipBound: Bound;
    push: SourceChange;
    expectPush: boolean;
  }[] = [
    {
      skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
      push: {type: 'add', row: {id: 1, date: '2014-01-23'}},
      expectPush: false,
    },
    {
      skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
      push: {type: 'add', row: {id: 2, date: '2014-01-23'}},
      expectPush: false,
    },
    {
      skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
      push: {type: 'add', row: {id: 1, date: '2014-01-24'}},
      expectPush: true,
    },
    {
      skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
      push: {type: 'add', row: {id: 2, date: '2014-01-24'}},
      expectPush: true,
    },
    {
      skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
      push: {type: 'add', row: {id: 1, date: '2014-01-25'}},
      expectPush: true,
    },
    {
      skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
      push: {type: 'add', row: {id: 2, date: '2014-01-25'}},
      expectPush: true,
    },

    {
      skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
      push: {type: 'add', row: {id: 1, date: '2014-01-23'}},
      expectPush: false,
    },
    {
      skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
      push: {type: 'add', row: {id: 2, date: '2014-01-23'}},
      expectPush: false,
    },
    {
      skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
      push: {type: 'add', row: {id: 1, date: '2014-01-24'}},
      expectPush: false,
    },
    {
      skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
      push: {type: 'add', row: {id: 2, date: '2014-01-24'}},
      expectPush: true,
    },
    {
      skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
      push: {type: 'add', row: {id: 1, date: '2014-01-25'}},
      expectPush: true,
    },
    {
      skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
      push: {type: 'add', row: {id: 2, date: '2014-01-25'}},
      expectPush: true,
    },
  ];

  for (const c of cases) {
    const ms = new MemorySource(
      'users',
      {
        id: {type: 'number'},
        date: {type: 'string'},
      },
      ['id'],
    );

    const conn = ms.connect([
      ['date', 'asc'],
      ['id', 'asc'],
    ]);
    const skip = new Skip(conn, c.skipBound);
    const out = new Catch(skip);

    ms.push(c.push);
    expect(out.pushes).toEqual(
      c.expectPush
        ? [{type: c.push.type, node: {row: c.push.row, relationships: {}}}]
        : [],
    );
  }
});
