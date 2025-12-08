import {expect, suite, test} from 'vitest';
import {Catch} from './catch.ts';
import type {FetchRequest} from './operator.ts';
import {type Bound, Skip} from './skip.ts';
import type {SourceChange} from './source.ts';
import {createSource} from './test/source-factory.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {consume} from './stream.ts';

const lc = createSilentLogContext();

suite('fetch', () => {
  function t(c: {skipBound: Bound; fetchRequest: FetchRequest}) {
    const ms = createSource(
      lc,
      testLogConfig,
      'users',
      {
        id: {type: 'number'},
        name: {type: 'string'},
        startDate: {type: 'string'},
      },
      ['id'],
    );

    consume(
      ms.push({
        type: 'add',
        row: {id: 1, name: 'Aaron', startDate: '2019-06-18'},
      }),
    );
    consume(
      ms.push({
        type: 'add',
        row: {id: 2, name: 'Erik', startDate: '2020-08-01'},
      }),
    );
    consume(
      ms.push({
        type: 'add',
        row: {id: 3, name: 'Greg', startDate: '2021-12-07'},
      }),
    );
    consume(
      ms.push({
        type: 'add',
        row: {id: 4, name: 'Cesar', startDate: '2022-12-01'},
      }),
    );
    consume(
      ms.push({
        type: 'add',
        row: {id: 5, name: 'Alex', startDate: '2023-04-01'},
      }),
    );
    consume(
      ms.push({
        type: 'add',
        row: {id: 6, name: 'Darick', startDate: '2023-09-01'},
      }),
    );
    consume(
      ms.push({
        type: 'add',
        row: {id: 7, name: 'Matt', startDate: '2024-06-01'},
      }),
    );

    const conn = ms.connect([
      ['startDate', 'asc'],
      ['id', 'asc'],
    ]);

    const skip = new Skip(conn, c.skipBound);
    const out = new Catch(skip);
    return out.fetch(c.fetchRequest);
  }

  test('c1', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-03-31', id: 5}, exclusive: false},
        fetchRequest: {},
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 5,
            "name": "Alex",
            "startDate": "2023-04-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c2', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-03-31', id: 5}, exclusive: true},
        fetchRequest: {},
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 5,
            "name": "Alex",
            "startDate": "2023-04-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c3', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
        fetchRequest: {},
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 5,
            "name": "Alex",
            "startDate": "2023-04-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c4', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: true},
        fetchRequest: {},
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c5', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-02', id: 4}, exclusive: false},
        fetchRequest: {},
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c6', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-02', id: 4}, exclusive: true},
        fetchRequest: {},
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c7', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
        fetchRequest: {
          start: {row: {startDate: '2023-03-30', id: 5}, basis: 'at'},
        },
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 5,
            "name": "Alex",
            "startDate": "2023-04-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c8', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
        fetchRequest: {
          start: {row: {startDate: '2023-03-30', id: 5}, basis: 'after'},
        },
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 5,
            "name": "Alex",
            "startDate": "2023-04-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c10', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
        fetchRequest: {
          start: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
        },
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 5,
            "name": "Alex",
            "startDate": "2023-04-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c11', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
        fetchRequest: {
          start: {row: {startDate: '2023-04-01', id: 5}, basis: 'after'},
        },
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c12', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: true},
        fetchRequest: {
          start: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
        },
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c13', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-02', id: 5}, exclusive: false},
        fetchRequest: {
          start: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
        },
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c14', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-02', id: 5}, exclusive: true},
        fetchRequest: {
          start: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
        },
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });
  test('c15', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-02', id: 5}, exclusive: true},
        fetchRequest: {
          start: {row: {startDate: '2023-04-01', id: 5}, basis: 'after'},
        },
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c16', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-02', id: 5}, exclusive: true},
        fetchRequest: {
          start: {row: {startDate: '2023-04-02', id: 5}, basis: 'after'},
        },
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
        {
          "relationships": {},
          "row": {
            "id": 7,
            "name": "Matt",
            "startDate": "2024-06-01",
          },
        },
      ]
    `);
  });

  test('c19', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-02', id: 5}, exclusive: true},
        fetchRequest: {
          start: {row: {startDate: '2030-04-02', id: 5}, basis: 'after'},
        },
      }),
    ).toMatchInlineSnapshot(`[]`);
  });

  test('fetch request constraint does not match bounds', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
        fetchRequest: {constraint: {id: 6}},
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "id": 6,
            "name": "Darick",
            "startDate": "2023-09-01",
          },
        },
      ]
    `);
  });

  test('fetch request constraint does not match bounds, only row matches constraint is before bounds', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: false},
        fetchRequest: {constraint: {id: 3}},
      }),
    ).toMatchInlineSnapshot(`
      []
    `);
  });

  suite('reverse', () => {
    test('inclusive bound, no start', () => {
      expect(
        t({
          skipBound: {
            row: {startDate: '2023-03-31', id: 5},
            exclusive: false,
          },
          fetchRequest: {reverse: true},
        }),
      ).toMatchInlineSnapshot(`
        [
          {
            "relationships": {},
            "row": {
              "id": 7,
              "name": "Matt",
              "startDate": "2024-06-01",
            },
          },
          {
            "relationships": {},
            "row": {
              "id": 6,
              "name": "Darick",
              "startDate": "2023-09-01",
            },
          },
          {
            "relationships": {},
            "row": {
              "id": 5,
              "name": "Alex",
              "startDate": "2023-04-01",
            },
          },
        ]
      `);
    });

    test('exclusive bound, no start', () => {
      expect(
        t({
          skipBound: {row: {startDate: '2023-03-31', id: 5}, exclusive: true},
          fetchRequest: {reverse: true},
        }),
      ).toMatchInlineSnapshot(`
        [
          {
            "relationships": {},
            "row": {
              "id": 7,
              "name": "Matt",
              "startDate": "2024-06-01",
            },
          },
          {
            "relationships": {},
            "row": {
              "id": 6,
              "name": "Darick",
              "startDate": "2023-09-01",
            },
          },
          {
            "relationships": {},
            "row": {
              "id": 5,
              "name": "Alex",
              "startDate": "2023-04-01",
            },
          },
        ]
      `);
    });

    test('inclusive bound, no start, bound equals a row', () => {
      expect(
        t({
          skipBound: {
            row: {startDate: '2023-04-01', id: 5},
            exclusive: false,
          },
          fetchRequest: {reverse: true},
        }),
      ).toMatchInlineSnapshot(`
        [
          {
            "relationships": {},
            "row": {
              "id": 7,
              "name": "Matt",
              "startDate": "2024-06-01",
            },
          },
          {
            "relationships": {},
            "row": {
              "id": 6,
              "name": "Darick",
              "startDate": "2023-09-01",
            },
          },
          {
            "relationships": {},
            "row": {
              "id": 5,
              "name": "Alex",
              "startDate": "2023-04-01",
            },
          },
        ]
      `);
    });

    test('exclusive bound, no start, bound equals a row', () => {
      expect(
        t({
          skipBound: {
            row: {startDate: '2023-04-01', id: 5},
            exclusive: true,
          },
          fetchRequest: {reverse: true},
        }),
      ).toMatchInlineSnapshot(`
        [
          {
            "relationships": {},
            "row": {
              "id": 7,
              "name": "Matt",
              "startDate": "2024-06-01",
            },
          },
          {
            "relationships": {},
            "row": {
              "id": 6,
              "name": "Darick",
              "startDate": "2023-09-01",
            },
          },
        ]
      `);
    });

    test('inclusive bound, inclusive start', () => {
      expect(
        t({
          skipBound: {
            row: {startDate: '2023-04-01', id: 5},
            exclusive: false,
          },
          fetchRequest: {
            start: {row: {startDate: '2023-03-30', id: 5}, basis: 'at'},
            reverse: true,
          },
        }),
      ).toMatchInlineSnapshot(`[]`);
    });

    test('inclusive bound, inclusive start, bound and start equal and equal to row', () => {
      expect(
        t({
          skipBound: {
            row: {startDate: '2023-04-01', id: 5},
            exclusive: false,
          },
          fetchRequest: {
            start: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
            reverse: true,
          },
        }),
      ).toMatchInlineSnapshot(`
        [
          {
            "relationships": {},
            "row": {
              "id": 5,
              "name": "Alex",
              "startDate": "2023-04-01",
            },
          },
        ]
      `);
    });

    test('inclusive bound, exclusive start', () => {
      expect(
        t({
          skipBound: {
            row: {startDate: '2023-04-01', id: 5},
            exclusive: false,
          },
          fetchRequest: {
            start: {row: {startDate: '2023-03-30', id: 5}, basis: 'after'},
            reverse: true,
          },
        }),
      ).toMatchInlineSnapshot(`[]`);
    });

    test('exclusive bound, inclusive start, bound and start equal', () => {
      expect(
        t({
          skipBound: {row: {startDate: '2023-04-01', id: 5}, exclusive: true},
          fetchRequest: {
            start: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
            reverse: true,
          },
        }),
      ).toMatchInlineSnapshot(`[]`);
    });

    test('exclusive bound, exclusive start', () => {
      expect(
        t({
          skipBound: {row: {startDate: '2023-04-00', id: 5}, exclusive: true},
          fetchRequest: {
            start: {row: {startDate: '2023-09-01', id: 5}, basis: 'after'},
            reverse: true,
          },
        }),
      ).toMatchInlineSnapshot(`
        [
          {
            "relationships": {},
            "row": {
              "id": 5,
              "name": "Alex",
              "startDate": "2023-04-01",
            },
          },
        ]
      `);
    });

    test('exclusive bound, exclusive start, start and bound equal', () => {
      expect(
        t({
          skipBound: {row: {startDate: '2023-04-02', id: 5}, exclusive: true},
          fetchRequest: {
            start: {row: {startDate: '2023-04-02', id: 5}, basis: 'after'},
            reverse: true,
          },
        }),
      ).toMatchInlineSnapshot(`[]`);
    });

    test('exclusive bound, exclusive start, start and bound equal, fairly apart', () => {
      expect(
        t({
          skipBound: {row: {startDate: '2020-08-01', id: 2}, exclusive: true},
          fetchRequest: {
            start: {row: {startDate: '2023-06-01', id: 7}, basis: 'after'},
            reverse: true,
          },
        }),
      ).toMatchInlineSnapshot(`
        [
          {
            "relationships": {},
            "row": {
              "id": 5,
              "name": "Alex",
              "startDate": "2023-04-01",
            },
          },
          {
            "relationships": {},
            "row": {
              "id": 4,
              "name": "Cesar",
              "startDate": "2022-12-01",
            },
          },
          {
            "relationships": {},
            "row": {
              "id": 3,
              "name": "Greg",
              "startDate": "2021-12-07",
            },
          },
        ]
      `);
    });
  });
});

suite('push', () => {
  function t(c: {name?: string; skipBound: Bound; pushes: SourceChange[]}) {
    const ms = createSource(
      lc,
      testLogConfig,
      'users',
      {
        id: {type: 'number'},
        date: {type: 'string'},
        x: {type: 'number', optional: true},
      },
      ['id'],
    );

    const conn = ms.connect([
      ['date', 'asc'],
      ['id', 'asc'],
    ]);
    const skip = new Skip(conn, c.skipBound);
    const out = new Catch(skip);

    for (const push of c.pushes) {
      consume(ms.push(push));
    }

    return out.pushes;
  }

  test('c1', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
        pushes: [{type: 'add', row: {id: 1, date: '2014-01-23'}}],
      }),
    ).toMatchInlineSnapshot(`[]`);
  });

  test('c2', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
        pushes: [{type: 'add', row: {id: 2, date: '2014-01-23'}}],
      }),
    ).toMatchInlineSnapshot(`[]`);
  });

  test('c3', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
        pushes: [{type: 'add', row: {id: 1, date: '2014-01-24'}}],
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {},
            "row": {
              "date": "2014-01-24",
              "id": 1,
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('c4', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
        pushes: [{type: 'add', row: {id: 2, date: '2014-01-24'}}],
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {},
            "row": {
              "date": "2014-01-24",
              "id": 2,
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('c5', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
        pushes: [{type: 'add', row: {id: 1, date: '2014-01-25'}}],
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {},
            "row": {
              "date": "2014-01-25",
              "id": 1,
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('c6', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
        pushes: [{type: 'add', row: {id: 2, date: '2014-01-25'}}],
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {},
            "row": {
              "date": "2014-01-25",
              "id": 2,
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('c7', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
        pushes: [{type: 'add', row: {id: 1, date: '2014-01-23'}}],
      }),
    ).toMatchInlineSnapshot(`[]`);
  });

  test('c8', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
        pushes: [{type: 'add', row: {id: 2, date: '2014-01-23'}}],
      }),
    ).toMatchInlineSnapshot(`[]`);
  });

  test('c9', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
        pushes: [{type: 'add', row: {id: 1, date: '2014-01-24'}}],
      }),
    ).toMatchInlineSnapshot(`[]`);
  });

  test('c10', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
        pushes: [{type: 'add', row: {id: 2, date: '2014-01-24'}}],
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {},
            "row": {
              "date": "2014-01-24",
              "id": 2,
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('c10', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
        pushes: [{type: 'add', row: {id: 1, date: '2014-01-25'}}],
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {},
            "row": {
              "date": "2014-01-25",
              "id": 1,
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('c11', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
        pushes: [{type: 'add', row: {id: 2, date: '2014-01-25'}}],
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {},
            "row": {
              "date": "2014-01-25",
              "id": 2,
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('Edit - Old and new before bound', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
        pushes: [
          {type: 'add', row: {id: 1, date: '2014-01-22'}},
          {
            type: 'edit',
            oldRow: {id: 1, date: '2014-01-22'},
            row: {id: 1, date: '2014-01-23'},
          },
        ],
      }),
    ).toMatchInlineSnapshot(`[]`);
  });

  test('Edit - Old and new at bound. Inclusive', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: false},
        pushes: [
          {type: 'add', row: {id: 1, date: '2014-01-24', x: 1}},
          {
            type: 'edit',
            oldRow: {id: 1, date: '2014-01-24', x: 1},
            row: {id: 1, date: '2014-01-24', x: 2},
          },
        ],
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {},
            "row": {
              "date": "2014-01-24",
              "id": 1,
              "x": 1,
            },
          },
          "type": "add",
        },
        {
          "oldRow": {
            "date": "2014-01-24",
            "id": 1,
            "x": 1,
          },
          "row": {
            "date": "2014-01-24",
            "id": 1,
            "x": 2,
          },
          "type": "edit",
        },
      ]
    `);
  });

  test('Edit - Old and new at bound. Exclusive', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
        pushes: [
          {type: 'add', row: {id: 1, date: '2014-01-24', x: 1}},
          {
            type: 'edit',
            oldRow: {id: 1, date: '2014-01-24', x: 1},
            row: {id: 1, date: '2014-01-24', x: 2},
          },
        ],
      }),
    ).toMatchInlineSnapshot(`[]`);
  });

  test('Edit - Old and new after bound', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
        pushes: [
          {type: 'add', row: {id: 1, date: '2014-01-25'}},
          {
            type: 'edit',
            oldRow: {id: 1, date: '2014-01-25'},
            row: {id: 1, date: '2014-01-26'},
          },
        ],
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {},
            "row": {
              "date": "2014-01-25",
              "id": 1,
            },
          },
          "type": "add",
        },
        {
          "oldRow": {
            "date": "2014-01-25",
            "id": 1,
          },
          "row": {
            "date": "2014-01-26",
            "id": 1,
          },
          "type": "edit",
        },
      ]
    `);
  });

  test('Edit - Old before bound, new after bound', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
        pushes: [
          {type: 'add', row: {id: 1, date: '2014-01-23'}},
          {
            type: 'edit',
            oldRow: {id: 1, date: '2014-01-23'},
            row: {id: 1, date: '2014-01-25'},
          },
        ],
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {},
            "row": {
              "date": "2014-01-25",
              "id": 1,
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('Edit - Old after bound, new before bound', () => {
    expect(
      t({
        skipBound: {row: {id: 1, date: '2014-01-24'}, exclusive: true},
        pushes: [
          {type: 'add', row: {id: 1, date: '2014-01-25'}},
          {
            type: 'edit',
            oldRow: {id: 1, date: '2014-01-25'},
            row: {id: 1, date: '2014-01-23'},
          },
        ],
      }),
    ).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {},
            "row": {
              "date": "2014-01-25",
              "id": 1,
            },
          },
          "type": "add",
        },
        {
          "node": {
            "relationships": {},
            "row": {
              "date": "2014-01-25",
              "id": 1,
            },
          },
          "type": "remove",
        },
      ]
    `);
  });
});
