import {describe, expect, test} from 'vitest';
import {makeComparator} from './data.ts';
import type {SourceSchema} from './schema.ts';
import {
  applyChange,
  refCountSymbol,
  type ViewChange,
} from './view-apply-change.ts';
import type {Entry, Format} from './view.ts';

// Test helpers for cleaner entry access (casts are safe in tests)
const entries = (e: Entry, key: string): Entry[] => e[key] as Entry[];
const at = (e: Entry, key: string, i: number): Entry => entries(e, key)[i];

describe('applyChange', () => {
  const relationship = '';
  const schema: SourceSchema = {
    tableName: 'event',
    columns: {
      id: {type: 'string'},
      name: {type: 'string'},
    },
    primaryKey: ['id'],
    sort: [['id', 'asc']],
    system: 'client',
    relationships: {
      athletes: {
        tableName: 'matchup',
        columns: {
          eventID: {type: 'string'},
          athleteCountry: {type: 'string'},
          athleteID: {type: 'string'},
          disciplineID: {type: 'string'},
        },
        primaryKey: ['eventID', 'athleteCountry', 'athleteID', 'disciplineID'],
        sort: [
          ['eventID', 'asc'],
          ['athleteCountry', 'asc'],
          ['athleteID', 'asc'],
          ['disciplineID', 'asc'],
        ],
        system: 'client',
        relationships: {
          athletes: {
            tableName: 'athlete',
            columns: {
              id: {type: 'string'},
              country: {type: 'string'},
              name: {type: 'string'},
            },
            primaryKey: ['country', 'id'],
            sort: [
              ['country', 'asc'],
              ['id', 'asc'],
            ],
            system: 'client',
            relationships: {},
            isHidden: false,
            compareRows: makeComparator([
              ['country', 'asc'],
              ['id', 'asc'],
            ]),
          },
        },
        isHidden: true,
        compareRows: makeComparator([
          ['eventID', 'asc'],
          ['athleteCountry', 'asc'],
          ['athleteID', 'asc'],
          ['disciplineID', 'asc'],
        ]),
      },
    },
    isHidden: false,
    compareRows: makeComparator([['id', 'asc']]),
  } as const;

  describe('Multiple entries', () => {
    test('singular: false', () => {
      // This should really be a WeakMap but for testing purposes we use a Map.

      let root: Entry = {'': []};
      const format: Format = {
        singular: false,
        relationships: {
          athletes: {
            relationships: {},
            singular: false,
          },
        },
      };

      {
        const changes: ViewChange[] = [
          {
            type: 'add',
            node: {
              row: {
                id: 'e1',
                name: 'Buffalo Big Board Classic',
              },
              relationships: {
                athletes: () => [],
              },
            },
          },
          {
            type: 'child',
            node: {
              row: {
                id: 'e1',
                name: 'Buffalo Big Board Classic',
              },
            },
            child: {
              relationshipName: 'athletes',
              change: {
                type: 'add',
                node: {
                  row: {
                    eventID: 'e1',
                    athleteCountry: 'USA',
                    athleteID: 'a1',
                    disciplineID: 'd1',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
                          country: 'USA',
                          id: 'a1',
                          name: 'Mason Ho',
                        },
                        relationships: {},
                      },
                    ],
                  },
                },
              },
            },
          },
          {
            type: 'child',
            node: {
              row: {
                id: 'e1',
                name: 'Buffalo Big Board Classic',
              },
            },
            child: {
              relationshipName: 'athletes',
              change: {
                type: 'add',
                node: {
                  row: {
                    eventID: 'e1',
                    athleteCountry: 'USA',
                    athleteID: 'a1',
                    disciplineID: 'd2',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
                          country: 'USA',
                          id: 'a1',
                          name: 'Mason Ho',
                        },
                        relationships: {},
                      },
                    ],
                  },
                },
              },
            },
          },
        ];

        for (const change of changes) {
          root = applyChange(root, change, schema, relationship, format, true);
        }

        expect(root).toMatchInlineSnapshot(`
          {
            "": [
              {
                "athletes": [
                  {
                    "country": "USA",
                    "id": "a1",
                    "name": "Mason Ho",
                    Symbol(rc): 2,
                    Symbol(id): "["USA","a1"]",
                  },
                ],
                "id": "e1",
                "name": "Buffalo Big Board Classic",
                Symbol(rc): 1,
                Symbol(id): ""e1"",
              },
            ],
          }
        `);
      }

      {
        const changes: ViewChange[] = [
          {
            type: 'child',
            node: {
              row: {
                id: 'e1',
                name: 'Buffalo Big Board Classic',
              },
            },
            child: {
              relationshipName: 'athletes',
              change: {
                type: 'remove',
                node: {
                  row: {
                    eventID: 'e1',
                    athleteCountry: 'USA',
                    athleteID: 'a1',
                    disciplineID: 'd1',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
                          country: 'USA',
                          id: 'a1',
                          name: 'Mason Ho',
                        },
                        relationships: {},
                      },
                    ],
                  },
                },
              },
            },
          },
        ];

        for (const change of changes) {
          root = applyChange(root, change, schema, relationship, format, true);
        }

        expect(root).toMatchInlineSnapshot(`
          {
            "": [
              {
                "athletes": [
                  {
                    "country": "USA",
                    "id": "a1",
                    "name": "Mason Ho",
                    Symbol(rc): 1,
                    Symbol(id): "["USA","a1"]",
                  },
                ],
                "id": "e1",
                "name": "Buffalo Big Board Classic",
                Symbol(rc): 1,
                Symbol(id): ""e1"",
              },
            ],
          }
        `);
      }

      {
        const changes: ViewChange[] = [
          {
            type: 'child',
            node: {
              row: {
                id: 'e1',
                name: 'Buffalo Big Board Classic',
              },
            },
            child: {
              relationshipName: 'athletes',
              change: {
                type: 'remove',
                node: {
                  row: {
                    eventID: 'e1',
                    athleteCountry: 'USA',
                    athleteID: 'a1',
                    disciplineID: 'd2',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
                          country: 'USA',
                          id: 'a1',
                          name: 'Mason Ho',
                        },
                        relationships: {},
                      },
                    ],
                  },
                },
              },
            },
          },
        ];

        for (const change of changes) {
          root = applyChange(root, change, schema, relationship, format, true);
        }

        expect(root).toMatchInlineSnapshot(`
          {
            "": [
              {
                "athletes": [],
                "id": "e1",
                "name": "Buffalo Big Board Classic",
                Symbol(rc): 1,
                Symbol(id): ""e1"",
              },
            ],
          }
        `);
      }
    });

    test('singular: true', () => {
      // This should really be a WeakMap but for testing purposes we use a Map.

      let root: Entry = {'': []};
      const format: Format = {
        singular: false,
        relationships: {
          athletes: {
            relationships: {},
            singular: true,
          },
        },
      };

      {
        const changes: ViewChange[] = [
          {
            type: 'add',
            node: {
              row: {
                id: 'e1',
                name: 'Buffalo Big Board Classic',
              },
              relationships: {
                athletes: () => [],
              },
            },
          },
          {
            type: 'child',
            node: {
              row: {
                id: 'e1',
                name: 'Buffalo Big Board Classic',
              },
            },
            child: {
              relationshipName: 'athletes',
              change: {
                type: 'add',
                node: {
                  row: {
                    eventID: 'e1',
                    athleteCountry: 'USA',
                    athleteID: 'a1',
                    disciplineID: 'd1',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
                          country: 'USA',
                          id: 'a1',
                          name: 'Mason Ho',
                        },
                        relationships: {},
                      },
                    ],
                  },
                },
              },
            },
          },
          {
            type: 'child',
            node: {
              row: {
                id: 'e1',
                name: 'Buffalo Big Board Classic',
              },
            },
            child: {
              relationshipName: 'athletes',
              change: {
                type: 'add',
                node: {
                  row: {
                    eventID: 'e1',
                    athleteCountry: 'USA',
                    athleteID: 'a1',
                    disciplineID: 'd2',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
                          country: 'USA',
                          id: 'a1',
                          name: 'Mason Ho',
                        },
                        relationships: {},
                      },
                    ],
                  },
                },
              },
            },
          },
        ];

        for (const change of changes) {
          root = applyChange(root, change, schema, relationship, format, true);
        }

        expect(root).toMatchInlineSnapshot(`
          {
            "": [
              {
                "athletes": {
                  "country": "USA",
                  "id": "a1",
                  "name": "Mason Ho",
                  Symbol(rc): 2,
                  Symbol(id): "["USA","a1"]",
                },
                "id": "e1",
                "name": "Buffalo Big Board Classic",
                Symbol(rc): 1,
                Symbol(id): ""e1"",
              },
            ],
          }
        `);
      }

      {
        const changes: ViewChange[] = [
          {
            type: 'child',
            node: {
              row: {
                id: 'e1',
                name: 'Buffalo Big Board Classic',
              },
            },
            child: {
              relationshipName: 'athletes',
              change: {
                type: 'remove',
                node: {
                  row: {
                    eventID: 'e1',
                    athleteCountry: 'USA',
                    athleteID: 'a1',
                    disciplineID: 'd1',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
                          country: 'USA',
                          id: 'a1',
                          name: 'Mason Ho',
                        },
                        relationships: {},
                      },
                    ],
                  },
                },
              },
            },
          },
        ];

        for (const change of changes) {
          root = applyChange(root, change, schema, relationship, format, true);
        }

        expect(root).toMatchInlineSnapshot(`
          {
            "": [
              {
                "athletes": {
                  "country": "USA",
                  "id": "a1",
                  "name": "Mason Ho",
                  Symbol(rc): 1,
                  Symbol(id): "["USA","a1"]",
                },
                "id": "e1",
                "name": "Buffalo Big Board Classic",
                Symbol(rc): 1,
                Symbol(id): ""e1"",
              },
            ],
          }
        `);
      }

      {
        const changes: ViewChange[] = [
          {
            type: 'child',
            node: {
              row: {
                id: 'e1',
                name: 'Buffalo Big Board Classic',
              },
            },
            child: {
              relationshipName: 'athletes',
              change: {
                type: 'remove',
                node: {
                  row: {
                    eventID: 'e1',
                    athleteCountry: 'USA',
                    athleteID: 'a1',
                    disciplineID: 'd2',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
                          country: 'USA',
                          id: 'a1',
                          name: 'Mason Ho',
                        },
                        relationships: {},
                      },
                    ],
                  },
                },
              },
            },
          },
        ];

        for (const change of changes) {
          root = applyChange(root, change, schema, relationship, format, true);
        }

        expect(root).toMatchInlineSnapshot(`
          {
            "": [
              {
                "athletes": undefined,
                "id": "e1",
                "name": "Buffalo Big Board Classic",
                Symbol(rc): 1,
                Symbol(id): ""e1"",
              },
            ],
          }
        `);
      }
    });
  });

  describe('Simple', () => {
    test('singular: false', () => {
      const schema = {
        tableName: 'event',
        columns: {
          id: {type: 'string'},
          name: {type: 'string'},
        },
        primaryKey: ['id'],
        sort: [['id', 'asc']],
        system: 'client',
        relationships: {},
        isHidden: false,
        compareRows: makeComparator([['id', 'asc']]),
      } as const;
      let root: Entry = {'': []};
      const format = {
        singular: false,
        relationships: {},
      };

      const apply = (change: ViewChange) => {
        root = applyChange(root, change, schema, '', format, true);
      };

      apply({
        type: 'add',
        node: {
          row: {
            id: '1',
            name: 'Aaron',
          },
          relationships: {},
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "id": "1",
              "name": "Aaron",
              Symbol(rc): 1,
              Symbol(id): ""1"",
            },
          ],
        }
      `);

      for (let i = 0; i < 5; i++) {
        apply({
          type: 'add',
          node: {
            row: {
              id: '2',
              name: 'Greg',
            },
            relationships: {},
          },
        });
      }
      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "id": "1",
              "name": "Aaron",
              Symbol(rc): 1,
              Symbol(id): ""1"",
            },
            {
              "id": "2",
              "name": "Greg",
              Symbol(rc): 5,
              Symbol(id): ""2"",
            },
          ],
        }
      `);

      for (let i = 0; i < 4; i++) {
        apply({
          type: 'remove',
          node: {
            row: {
              id: '2',
              name: 'Greg',
            },
            relationships: {},
          },
        });
      }
      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "id": "1",
              "name": "Aaron",
              Symbol(rc): 1,
              Symbol(id): ""1"",
            },
            {
              "id": "2",
              "name": "Greg",
              Symbol(rc): 1,
              Symbol(id): ""2"",
            },
          ],
        }
      `);

      apply({
        type: 'remove',
        node: {
          row: {
            id: '2',
            name: 'Greg',
          },
          relationships: {},
        },
      });

      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "id": "1",
              "name": "Aaron",
              Symbol(rc): 1,
              Symbol(id): ""1"",
            },
          ],
        }
      `);

      expect(() =>
        apply({
          type: 'remove',
          node: {
            row: {
              id: '2',
              name: 'Greg',
            },
            relationships: {},
          },
        }),
      ).toThrowError(new Error('node does not exist'));
    });

    test('singular: true', () => {
      const schema = {
        tableName: 'event',
        columns: {
          id: {type: 'string'},
          name: {type: 'string'},
        },
        primaryKey: ['id'],
        sort: [['id', 'asc']],
        system: 'client',
        relationships: {},
        isHidden: false,
        compareRows: makeComparator([['id', 'asc']]),
      } as const;
      let root: Entry = {'': undefined};
      const format = {
        singular: true,
        relationships: {},
      };

      const apply = (change: ViewChange) => {
        root = applyChange(root, change, schema, relationship, format, true);
      };

      apply({
        type: 'add',
        node: {
          row: {
            id: '1',
            name: 'Aaron',
          },
          relationships: {},
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": {
            "id": "1",
            "name": "Aaron",
            Symbol(rc): 1,
            Symbol(id): ""1"",
          },
        }
      `);

      expect(() =>
        apply({
          type: 'add',
          node: {
            row: {
              id: '2',
              name: 'Greg',
            },
            relationships: {},
          },
        }),
      ).toThrowError(
        new Error(
          "Singular relationship '' should not have multiple rows. You may need to declare this relationship with the `many` helper instead of the `one` helper in your schema.",
        ),
      );

      apply({
        type: 'add',
        node: {
          row: {
            id: '1',
            name: 'Aaron',
          },
          relationships: {},
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": {
            "id": "1",
            "name": "Aaron",
            Symbol(rc): 2,
            Symbol(id): ""1"",
          },
        }
      `);

      for (let i = 0; i < 3; i++) {
        apply({
          type: 'add',
          node: {
            row: {
              id: '1',
              name: 'Aaron',
            },
            relationships: {},
          },
        });
      }
      expect(root).toMatchInlineSnapshot(`
        {
          "": {
            "id": "1",
            "name": "Aaron",
            Symbol(rc): 5,
            Symbol(id): ""1"",
          },
        }
      `);

      for (let i = 0; i < 4; i++) {
        apply({
          type: 'remove',
          node: {
            row: {
              id: '1',
              name: 'Aaron',
            },
            relationships: {},
          },
        });
      }
      expect(root).toMatchInlineSnapshot(`
        {
          "": {
            "id": "1",
            "name": "Aaron",
            Symbol(rc): 1,
            Symbol(id): ""1"",
          },
        }
      `);

      apply({
        type: 'remove',
        node: {
          row: {
            id: '1',
            name: 'Aaron',
          },
          relationships: {},
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": undefined,
        }
      `);

      expect(() =>
        apply({
          type: 'remove',
          node: {
            row: {
              id: '1',
              name: 'Aaron',
            },
            relationships: {},
          },
        }),
      ).toThrowError(new Error('node does not exist'));
    });

    test('edit, singular: false', () => {
      const schema = {
        tableName: 'event',
        columns: {
          id: {type: 'string'},
          name: {type: 'string'},
        },
        primaryKey: ['id'],
        sort: [['id', 'asc']],
        system: 'client',
        relationships: {},
        isHidden: false,
        compareRows: makeComparator([['id', 'asc']]),
      } as const;
      let root: Entry = {'': []};
      const format = {
        singular: false,
        relationships: {},
      };

      const apply = (change: ViewChange) => {
        root = applyChange(root, change, schema, '', format, true);
      };

      apply({
        type: 'add',
        node: {
          row: {
            id: '1',
            name: 'Aaron',
          },
          relationships: {},
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "id": "1",
              "name": "Aaron",
              Symbol(rc): 1,
              Symbol(id): ""1"",
            },
          ],
        }
      `);

      apply({
        type: 'edit',
        oldNode: {
          row: {
            id: '1',
            name: 'Aaron',
          },
        },
        node: {
          row: {
            id: '1',
            name: 'Greg',
          },
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "id": "1",
              "name": "Greg",
              Symbol(rc): 1,
              Symbol(id): ""1"",
            },
          ],
        }
      `);

      for (let i = 0; i < 2; i++) {
        apply({
          type: 'add',
          node: {
            row: {
              id: '1',
              name: 'Greg',
            },
            relationships: {},
          },
        });
      }
      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "id": "1",
              "name": "Greg",
              Symbol(rc): 3,
              Symbol(id): ""1"",
            },
          ],
        }
      `);

      apply({
        type: 'edit',
        oldNode: {
          row: {
            id: '1',
            name: 'Greg',
          },
        },
        node: {
          row: {
            id: '1',
            name: 'Aaron',
          },
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "id": "1",
              "name": "Aaron",
              Symbol(rc): 3,
              Symbol(id): ""1"",
            },
          ],
        }
      `);
    });

    test('edit primary key, singular: false', () => {
      const schema = {
        tableName: 'event',
        columns: {
          id: {type: 'string'},
          name: {type: 'string'},
        },
        primaryKey: ['id'],
        sort: [['id', 'asc']],
        system: 'client',
        relationships: {},
        isHidden: false,
        compareRows: makeComparator([['id', 'asc']]),
      } as const;
      let root: Entry = {'': []};
      const format = {
        singular: false,
        relationships: {},
      };

      const apply = (change: ViewChange) => {
        root = applyChange(root, change, schema, '', format, true);
      };

      apply({
        type: 'add',
        node: {
          row: {
            id: '1',
            name: 'Aaron',
          },
          relationships: {},
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "id": "1",
              "name": "Aaron",
              Symbol(rc): 1,
              Symbol(id): ""1"",
            },
          ],
        }
      `);

      apply({
        type: 'edit',
        oldNode: {
          row: {
            id: '1',
            name: 'Aaron',
          },
        },
        node: {
          row: {
            id: '2',
            name: 'Greg',
          },
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "id": "2",
              "name": "Greg",
              Symbol(rc): 1,
              Symbol(id): ""2"",
            },
          ],
        }
      `);

      apply({
        type: 'remove',
        node: {
          row: {
            id: '2',
            name: 'Greg',
          },
          relationships: {},
        },
      });

      for (let i = 0; i < 2; i++) {
        apply({
          type: 'add',
          node: {
            row: {
              id: '1',
              name: 'Aaron',
            },
            relationships: {},
          },
        });
      }
      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "id": "1",
              "name": "Aaron",
              Symbol(rc): 2,
              Symbol(id): ""1"",
            },
          ],
        }
      `);

      for (let i = 0; i < 2; i++) {
        apply({
          type: 'add',
          node: {
            row: {
              id: '2',
              name: 'Greg',
            },
            relationships: {},
          },
        });
      }

      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "id": "1",
              "name": "Aaron",
              Symbol(rc): 2,
              Symbol(id): ""1"",
            },
            {
              "id": "2",
              "name": "Greg",
              Symbol(rc): 2,
              Symbol(id): ""2"",
            },
          ],
        }
      `);

      apply({
        type: 'edit',
        oldNode: {
          row: {
            id: '1',
            name: 'Aaron',
          },
        },
        node: {
          row: {
            id: '2',
            name: 'Greg',
          },
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "id": "1",
              "name": "Aaron",
              Symbol(rc): 1,
              Symbol(id): ""1"",
            },
            {
              "id": "2",
              "name": "Greg",
              Symbol(rc): 3,
              Symbol(id): ""2"",
            },
          ],
        }
      `);
    });

    test('edit, singular: true', () => {
      const schema = {
        tableName: 'event',
        columns: {
          id: {type: 'string'},
          name: {type: 'string'},
        },
        primaryKey: ['id'],
        sort: [['id', 'asc']],
        system: 'client',
        relationships: {},
        isHidden: false,
        compareRows: makeComparator([['id', 'asc']]),
      } as const;
      let root: Entry = {'': undefined};
      const format = {
        singular: true,
        relationships: {},
      };

      const apply = (change: ViewChange) => {
        root = applyChange(root, change, schema, '', format, true);
      };

      apply({
        type: 'add',
        node: {
          row: {
            id: '1',
            name: 'Aaron',
          },
          relationships: {},
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": {
            "id": "1",
            "name": "Aaron",
            Symbol(rc): 1,
            Symbol(id): ""1"",
          },
        }
      `);

      apply({
        type: 'edit',
        oldNode: {
          row: {
            id: '1',
            name: 'Aaron',
          },
        },
        node: {
          row: {
            id: '1',
            name: 'Greg',
          },
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": {
            "id": "1",
            "name": "Greg",
            Symbol(rc): 1,
            Symbol(id): ""1"",
          },
        }
      `);

      apply({
        type: 'add',
        node: {
          row: {
            id: '1',
            name: 'Greg',
          },
          relationships: {},
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": {
            "id": "1",
            "name": "Greg",
            Symbol(rc): 2,
            Symbol(id): ""1"",
          },
        }
      `);

      apply({
        type: 'edit',
        oldNode: {
          row: {
            id: '2',
            name: 'Greg',
          },
        },
        node: {
          row: {
            id: '1',
            name: 'Aaron',
          },
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": {
            "id": "1",
            "name": "Aaron",
            Symbol(rc): 2,
            Symbol(id): ""1"",
          },
        }
      `);
    });

    test('edit primary key, singular: true', () => {
      const schema = {
        tableName: 'event',
        columns: {
          id: {type: 'string'},
          name: {type: 'string'},
        },
        primaryKey: ['id'],
        sort: [['id', 'asc']],
        system: 'client',
        relationships: {},
        isHidden: false,
        compareRows: makeComparator([['id', 'asc']]),
      } as const;
      let root: Entry = {'': undefined};
      const format = {
        singular: true,
        relationships: {},
      };

      const apply = (change: ViewChange) => {
        root = applyChange(root, change, schema, '', format, true);
      };

      apply({
        type: 'add',
        node: {
          row: {
            id: '1',
            name: 'Aaron',
          },
          relationships: {},
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": {
            "id": "1",
            "name": "Aaron",
            Symbol(rc): 1,
            Symbol(id): ""1"",
          },
        }
      `);

      apply({
        type: 'edit',
        oldNode: {
          row: {
            id: '1',
            name: 'Aaron',
          },
        },
        node: {
          row: {
            id: '2',
            name: 'Greg',
          },
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": {
            "id": "2",
            "name": "Greg",
            Symbol(rc): 1,
            Symbol(id): ""2"",
          },
        }
      `);

      apply({
        type: 'add',
        node: {
          row: {
            id: '2',
            name: 'Greg',
          },
          relationships: {},
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": {
            "id": "2",
            "name": "Greg",
            Symbol(rc): 2,
            Symbol(id): ""2"",
          },
        }
      `);

      apply({
        type: 'edit',
        oldNode: {
          row: {
            id: '2',
            name: 'Greg',
          },
        },
        node: {
          row: {
            id: '1',
            name: 'Aaron',
          },
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": {
            "id": "1",
            "name": "Aaron",
            Symbol(rc): 2,
            Symbol(id): ""1"",
          },
        }
      `);
    });
  });

  describe('Object identity preservation (immutability)', () => {
    const simpleSchema = {
      tableName: 'item',
      columns: {
        id: {type: 'string'},
        name: {type: 'string'},
      },
      primaryKey: ['id'],
      sort: [['id', 'asc']],
      system: 'client',
      relationships: {},
      isHidden: false,
      compareRows: makeComparator([['id', 'asc']]),
    } as const;

    const format: Format = {
      singular: false,
      relationships: {},
    };

    test('unchanged rows keep their reference when adding a new row', () => {
      let root: Entry = {'': []};

      // Add first row
      root = applyChange(
        root,
        {
          type: 'add',
          node: {row: {id: '1', name: 'Alice'}, relationships: {}},
        },
        simpleSchema,
        '',
        format,
        true,
      );

      const firstRowRef = at(root, '', 0);

      // Add second row
      root = applyChange(
        root,
        {
          type: 'add',
          node: {row: {id: '2', name: 'Bob'}, relationships: {}},
        },
        simpleSchema,
        '',
        format,
        true,
      );

      // First row should keep same reference (toBe checks reference equality)
      expect(at(root, '', 0)).toBe(firstRowRef);
      expect(entries(root, '')).toHaveLength(2);
    });

    test('unchanged rows keep their reference when removing another row', () => {
      let root: Entry = {'': []};

      // Add two rows
      root = applyChange(
        root,
        {
          type: 'add',
          node: {row: {id: '1', name: 'Alice'}, relationships: {}},
        },
        simpleSchema,
        '',
        format,
        true,
      );
      root = applyChange(
        root,
        {
          type: 'add',
          node: {row: {id: '2', name: 'Bob'}, relationships: {}},
        },
        simpleSchema,
        '',
        format,
        true,
      );

      const firstRowRef = at(root, '', 0);

      // Remove second row
      root = applyChange(
        root,
        {
          type: 'remove',
          node: {row: {id: '2', name: 'Bob'}, relationships: {}},
        },
        simpleSchema,
        '',
        format,
        true,
      );

      // First row should keep same reference
      expect(at(root, '', 0)).toBe(firstRowRef);
      expect(entries(root, '')).toHaveLength(1);
    });

    test('unchanged rows keep their reference when editing another row', () => {
      let root: Entry = {'': []};

      // Add two rows
      root = applyChange(
        root,
        {
          type: 'add',
          node: {row: {id: '1', name: 'Alice'}, relationships: {}},
        },
        simpleSchema,
        '',
        format,
        true,
      );
      root = applyChange(
        root,
        {
          type: 'add',
          node: {row: {id: '2', name: 'Bob'}, relationships: {}},
        },
        simpleSchema,
        '',
        format,
        true,
      );

      const firstRowRef = at(root, '', 0);

      // Edit second row
      root = applyChange(
        root,
        {
          type: 'edit',
          oldNode: {row: {id: '2', name: 'Bob'}},
          node: {row: {id: '2', name: 'Bobby'}},
        },
        simpleSchema,
        '',
        format,
        true,
      );

      // First row should keep same reference
      expect(at(root, '', 0)).toBe(firstRowRef);
      // Second row should have new reference with updated data
      expect(at(root, '', 1)).not.toBe(firstRowRef);
      expect(at(root, '', 1)).toEqual(
        expect.objectContaining({id: '2', name: 'Bobby'}),
      );
    });

    test('edited row gets new reference', () => {
      let root: Entry = {'': []};

      root = applyChange(
        root,
        {
          type: 'add',
          node: {row: {id: '1', name: 'Alice'}, relationships: {}},
        },
        simpleSchema,
        '',
        format,
        true,
      );

      const originalRef = at(root, '', 0);

      root = applyChange(
        root,
        {
          type: 'edit',
          oldNode: {row: {id: '1', name: 'Alice'}},
          node: {row: {id: '1', name: 'Alicia'}},
        },
        simpleSchema,
        '',
        format,
        true,
      );

      // Edited row should have a new reference
      expect(at(root, '', 0)).not.toBe(originalRef);
      expect(at(root, '', 0)).toEqual(
        expect.objectContaining({id: '1', name: 'Alicia'}),
      );
    });

    test('root object changes when any modification occurs', () => {
      let root: Entry = {'': []};

      root = applyChange(
        root,
        {
          type: 'add',
          node: {row: {id: '1', name: 'Alice'}, relationships: {}},
        },
        simpleSchema,
        '',
        format,
        true,
      );

      const rootRef = root;
      const listRef = entries(root, '');

      root = applyChange(
        root,
        {
          type: 'add',
          node: {row: {id: '2', name: 'Bob'}, relationships: {}},
        },
        simpleSchema,
        '',
        format,
        true,
      );

      // Root and list should have new references
      expect(root).not.toBe(rootRef);
      expect(root['']).not.toBe(listRef);
    });

    test('unchanged nested relationships keep their reference', () => {
      const schemaWithRelationship: SourceSchema = {
        tableName: 'parent',
        columns: {id: {type: 'string'}, name: {type: 'string'}},
        primaryKey: ['id'],
        sort: [['id', 'asc']],
        system: 'client',
        relationships: {
          children: {
            tableName: 'child',
            columns: {id: {type: 'string'}, parentId: {type: 'string'}},
            primaryKey: ['id'],
            sort: [['id', 'asc']],
            system: 'client',
            relationships: {},
            isHidden: false,
            compareRows: makeComparator([['id', 'asc']]),
          },
        },
        isHidden: false,
        compareRows: makeComparator([['id', 'asc']]),
      };

      const formatWithRelationship: Format = {
        singular: false,
        relationships: {
          children: {
            singular: false,
            relationships: {},
          },
        },
      };

      const apply = (entry: Entry, change: ViewChange) =>
        applyChange(entry, change, schemaWithRelationship, '', formatWithRelationship, true);

      let root: Entry = {'': []};

      // Add parent with children
      root = apply(root, {
        type: 'add',
        node: {
          row: {id: 'p1', name: 'Parent1'},
          relationships: {
            children: () => [
              {row: {id: 'c1', parentId: 'p1'}, relationships: {}},
              {row: {id: 'c2', parentId: 'p1'}, relationships: {}},
            ],
          },
        },
      });

      const parent1 = at(root, '', 0);
      const child1Ref = at(parent1, 'children', 0);
      const child2Ref = at(parent1, 'children', 1);

      // Add a new child to the parent
      root = apply(root, {
        type: 'child',
        node: {row: {id: 'p1', name: 'Parent1'}},
        child: {
          relationshipName: 'children',
          change: {
            type: 'add',
            node: {row: {id: 'c3', parentId: 'p1'}, relationships: {}},
          },
        },
      });

      const newParent1 = at(root, '', 0);
      // Parent should have new reference (its children changed)
      expect(newParent1).not.toBe(parent1);
      // But existing children should keep their references
      expect(at(newParent1, 'children', 0)).toBe(child1Ref);
      expect(at(newParent1, 'children', 1)).toBe(child2Ref);
      // New child is added
      expect(entries(newParent1, 'children')).toHaveLength(3);
    });

    test('refCount increment creates new reference but preserves data', () => {
      let root: Entry = {'': []};

      root = applyChange(
        root,
        {
          type: 'add',
          node: {row: {id: '1', name: 'Alice'}, relationships: {}},
        },
        simpleSchema,
        '',
        format,
        true,
      );

      const originalRef = at(root, '', 0);
      const originalRefCount = (originalRef as {[refCountSymbol]: number})[
        refCountSymbol
      ];

      // Add same row again (should increment refCount)
      root = applyChange(
        root,
        {
          type: 'add',
          node: {row: {id: '1', name: 'Alice'}, relationships: {}},
        },
        simpleSchema,
        '',
        format,
        true,
      );

      const newRef = at(root, '', 0);

      // Reference should change (immutability)
      expect(newRef).not.toBe(originalRef);
      // RefCount should be incremented
      expect((newRef as {[refCountSymbol]: number})[refCountSymbol]).toBe(
        originalRefCount + 1,
      );
      // Data should be the same
      expect(newRef).toEqual(expect.objectContaining({id: '1', name: 'Alice'}));
    });

    test('child change on one parent does not affect other parents', () => {
      const schemaWithRelationship: SourceSchema = {
        tableName: 'parent',
        columns: {id: {type: 'string'}, name: {type: 'string'}},
        primaryKey: ['id'],
        sort: [['id', 'asc']],
        system: 'client',
        relationships: {
          children: {
            tableName: 'child',
            columns: {id: {type: 'string'}, parentId: {type: 'string'}},
            primaryKey: ['id'],
            sort: [['id', 'asc']],
            system: 'client',
            relationships: {},
            isHidden: false,
            compareRows: makeComparator([['id', 'asc']]),
          },
        },
        isHidden: false,
        compareRows: makeComparator([['id', 'asc']]),
      };

      const formatWithRelationship: Format = {
        singular: false,
        relationships: {
          children: {
            singular: false,
            relationships: {},
          },
        },
      };

      const apply = (entry: Entry, change: ViewChange) =>
        applyChange(entry, change, schemaWithRelationship, '', formatWithRelationship, true);

      let root: Entry = {'': []};

      // Add two parents with children
      root = apply(root, {
        type: 'add',
        node: {
          row: {id: 'p1', name: 'Parent1'},
          relationships: {
            children: () => [{row: {id: 'c1', parentId: 'p1'}, relationships: {}}],
          },
        },
      });

      root = apply(root, {
        type: 'add',
        node: {
          row: {id: 'p2', name: 'Parent2'},
          relationships: {
            children: () => [{row: {id: 'c2', parentId: 'p2'}, relationships: {}}],
          },
        },
      });

      const parent1Ref = at(root, '', 0);
      const parent2Ref = at(root, '', 1);
      const parent2ChildrenRef = entries(parent2Ref, 'children');

      // Add a child to parent1 only
      root = apply(root, {
        type: 'child',
        node: {row: {id: 'p1', name: 'Parent1'}},
        child: {
          relationshipName: 'children',
          change: {
            type: 'add',
            node: {row: {id: 'c3', parentId: 'p1'}, relationships: {}},
          },
        },
      });

      // Parent1 should have new reference
      expect(at(root, '', 0)).not.toBe(parent1Ref);
      // Parent2 should keep same reference (unchanged)
      expect(at(root, '', 1)).toBe(parent2Ref);
      // Parent2's children should keep same reference
      expect(at(root, '', 1)['children']).toBe(parent2ChildrenRef);
    });
  });
});
