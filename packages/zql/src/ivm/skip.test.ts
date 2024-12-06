import {expect, suite, test} from 'vitest';
import {Catch} from './catch.js';
import type {Start} from './operator.js';
import {type Bound, Skip} from './skip.js';
import type {SourceChange} from './source.js';
import {createSource} from './test/source-factory.js';

suite('fetch', () => {
  function t(c: {
    skipBound: Bound;
    fetchBound: Start | undefined;
    fetchReverse?: boolean | undefined;
  }) {
    const ms = createSource(
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
    return out.fetch({start: c.fetchBound});
  }

  test('c1', () => {
    expect(
      t({
        skipBound: {row: {startDate: '2023-03-31', id: 5}, exclusive: false},
        fetchBound: undefined,
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
        fetchBound: undefined,
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
        fetchBound: undefined,
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
        fetchBound: undefined,
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
        fetchBound: undefined,
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
        fetchBound: undefined,
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
        fetchBound: {row: {startDate: '2023-03-30', id: 5}, basis: 'at'},
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
        fetchBound: {row: {startDate: '2023-03-30', id: 5}, basis: 'after'},
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
        fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
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
        fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'after'},
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
        fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
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
        fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
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
        fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
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
        fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'after'},
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
        fetchBound: {row: {startDate: '2023-04-02', id: 5}, basis: 'after'},
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
        fetchBound: {row: {startDate: '2030-04-02', id: 5}, basis: 'after'},
      }),
    ).toMatchInlineSnapshot(`[]`);
  });

  suite('reverse', () => {
    test('inclusive bound, no start', () => {
      expect(
        t({
          skipBound: {
            row: {startDate: '2023-03-31', id: 5},
            exclusive: false,
          },
          fetchBound: undefined,
          fetchReverse: true,
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

    test('exclusive bound, no start', () => {
      expect(
        t({
          skipBound: {row: {startDate: '2023-03-31', id: 5}, exclusive: true},
          fetchBound: undefined,
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

    test('c7', () => {
      expect(
        t({
          skipBound: {
            row: {startDate: '2023-04-01', id: 5},
            exclusive: false,
          },
          fetchBound: {row: {startDate: '2023-03-30', id: 5}, basis: 'at'},
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
          skipBound: {
            row: {startDate: '2023-04-01', id: 5},
            exclusive: false,
          },
          fetchBound: {row: {startDate: '2023-03-30', id: 5}, basis: 'after'},
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
          skipBound: {
            row: {startDate: '2023-04-01', id: 5},
            exclusive: false,
          },
          fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
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
          skipBound: {
            row: {startDate: '2023-04-01', id: 5},
            exclusive: false,
          },
          fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'after'},
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
          fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
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
          skipBound: {
            row: {startDate: '2023-04-02', id: 5},
            exclusive: false,
          },
          fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
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
          fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'at'},
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
          fetchBound: {row: {startDate: '2023-04-01', id: 5}, basis: 'after'},
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
          fetchBound: {row: {startDate: '2023-04-02', id: 5}, basis: 'after'},
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
          fetchBound: {row: {startDate: '2030-04-02', id: 5}, basis: 'after'},
        }),
      ).toMatchInlineSnapshot(`[]`);
    });
  });
});

suite('push', () => {
  function t(c: {name?: string; skipBound: Bound; pushes: SourceChange[]}) {
    const ms = createSource(
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
      ms.push(push);
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
