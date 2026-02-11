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

const NO_MUTATE = 0 as const;
const WITH_IDS = 'withIDs' as const;

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
      let root: Entry = {'': []};
      const format: Format = {
        singular: false,
        relationships: {athletes: {relationships: {}, singular: false}},
      };
      const apply = (change: ViewChange) => {
        root = applyChange(
          root,
          change,
          schema,
          relationship,
          format,
          WITH_IDS,
          NO_MUTATE,
        );
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

        changes.forEach(apply);

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

        changes.forEach(apply);

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

        changes.forEach(apply);

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
      const apply = (change: ViewChange) => {
        root = applyChange(
          root,
          change,
          schema,
          relationship,
          format,
          WITH_IDS,
          NO_MUTATE,
        );
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

        changes.forEach(apply);

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

        changes.forEach(apply);

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

        changes.forEach(apply);

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
        root = applyChange(
          root,
          change,
          schema,
          '',
          format,
          WITH_IDS,
          NO_MUTATE,
        );
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
        root = applyChange(
          root,
          change,
          schema,
          relationship,
          format,
          WITH_IDS,
          NO_MUTATE,
        );
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
        root = applyChange(
          root,
          change,
          schema,
          '',
          format,
          WITH_IDS,
          NO_MUTATE,
        );
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
        root = applyChange(
          root,
          change,
          schema,
          '',
          format,
          WITH_IDS,
          NO_MUTATE,
        );
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
        root = applyChange(
          root,
          change,
          schema,
          '',
          format,
          WITH_IDS,
          NO_MUTATE,
        );
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
        root = applyChange(
          root,
          change,
          schema,
          '',
          format,
          WITH_IDS,
          NO_MUTATE,
        );
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

  // ═══════════════════════════════════════════════════════════════════════════
  // ADD WITH CHILDREN TESTS
  // Verify entries with child relationships are correctly positioned.
  // Tests the indexOf optimization: add() returns pos, avoiding O(n) scan.
  //
  //   []  ──add B+kids──►  [B]  ──add A+kids──►  [A, B]
  //                         │                     ↑
  //                         │         binary search finds pos=0
  //                         └──────────────────────┘
  // ═══════════════════════════════════════════════════════════════════════════
  describe('add with initialized relationships', () => {
    const schemaWithChildren: SourceSchema = {
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
    } as const;

    const formatWithChildren: Format = {
      singular: false,
      relationships: {
        children: {singular: false, relationships: {}},
      },
    };

    const apply = (root: Entry, change: ViewChange) =>
      applyChange(
        root,
        change,
        schemaWithChildren,
        '',
        formatWithChildren,
        WITH_IDS,
        NO_MUTATE,
      );

    test('entry with children is placed at correct position', () => {
      let root: Entry = {'': []};

      // Add entries in non-alphabetical order to verify binary search positioning
      root = apply(root, {
        type: 'add',
        node: {
          row: {id: 'b', name: 'Bob'},
          relationships: {
            children: () => [
              {row: {id: 'c1', parentId: 'b'}, relationships: {}},
            ],
          },
        },
      });

      root = apply(root, {
        type: 'add',
        node: {
          row: {id: 'd', name: 'Dave'},
          relationships: {
            children: () => [
              {row: {id: 'c2', parentId: 'd'}, relationships: {}},
            ],
          },
        },
      });

      // Add entry that should be inserted at position 0 (before 'b')
      // This entry has children, so initializeRelationships will return a new entry
      root = apply(root, {
        type: 'add',
        node: {
          row: {id: 'a', name: 'Alice'},
          relationships: {
            children: () => [
              {row: {id: 'c3', parentId: 'a'}, relationships: {}},
            ],
          },
        },
      });

      // Verify entries are in correct sorted order with their children
      expect(root).toMatchInlineSnapshot(`
        {
          "": [
            {
              "children": [
                {
                  "id": "c3",
                  "parentId": "a",
                  Symbol(rc): 1,
                  Symbol(id): ""c3"",
                },
              ],
              "id": "a",
              "name": "Alice",
              Symbol(rc): 1,
              Symbol(id): ""a"",
            },
            {
              "children": [
                {
                  "id": "c1",
                  "parentId": "b",
                  Symbol(rc): 1,
                  Symbol(id): ""c1"",
                },
              ],
              "id": "b",
              "name": "Bob",
              Symbol(rc): 1,
              Symbol(id): ""b"",
            },
            {
              "children": [
                {
                  "id": "c2",
                  "parentId": "d",
                  Symbol(rc): 1,
                  Symbol(id): ""c2"",
                },
              ],
              "id": "d",
              "name": "Dave",
              Symbol(rc): 1,
              Symbol(id): ""d"",
            },
          ],
        }
      `);
    });

    test('entry inserted in middle position with children', () => {
      let root: Entry = {'': []};

      // Add entries with gap for middle insertion
      root = apply(root, {
        type: 'add',
        node: {
          row: {id: 'a', name: 'Alice'},
          relationships: {children: () => []},
        },
      });

      root = apply(root, {
        type: 'add',
        node: {
          row: {id: 'c', name: 'Charlie'},
          relationships: {children: () => []},
        },
      });

      // Insert entry in middle (at position 1) with multiple children
      root = apply(root, {
        type: 'add',
        node: {
          row: {id: 'b', name: 'Bob'},
          relationships: {
            children: () => [
              {row: {id: 'child1', parentId: 'b'}, relationships: {}},
              {row: {id: 'child2', parentId: 'b'}, relationships: {}},
            ],
          },
        },
      });

      // Verify middle entry has children and is at correct position
      expect(at(root, '', 0)).toEqual(
        expect.objectContaining({id: 'a', name: 'Alice'}),
      );
      expect(at(root, '', 1)).toEqual(
        expect.objectContaining({id: 'b', name: 'Bob'}),
      );
      expect(at(root, '', 2)).toEqual(
        expect.objectContaining({id: 'c', name: 'Charlie'}),
      );

      // Verify Bob's children are correctly initialized
      const bobEntry = at(root, '', 1);
      expect(entries(bobEntry, 'children')).toHaveLength(2);
      expect(at(bobEntry, 'children', 0)).toEqual(
        expect.objectContaining({id: 'child1', parentId: 'b'}),
      );
      expect(at(bobEntry, 'children', 1)).toEqual(
        expect.objectContaining({id: 'child2', parentId: 'b'}),
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // IMMUTABILITY TESTS
  // Verify unchanged rows keep their object reference (enables React.memo etc.)
  //
  //   root ─┬─ [A, B, C]     edit B      root' ─┬─ [A, B', C]
  //         │                  ────►            │
  //         └─ users:[X,Y]                      └─ users:[X,Y]  ← same ref
  //
  //   A, C: same ref (unchanged)
  //   B': new ref (edited)
  //   users: same ref (sibling relationship untouched)
  // ═══════════════════════════════════════════════════════════════════════════
  describe('Object identity preservation (immutability)', () => {
    const simpleSchema = {
      tableName: 'item',
      columns: {id: {type: 'string'}, name: {type: 'string'}},
      primaryKey: ['id'],
      sort: [['id', 'asc']],
      system: 'client',
      relationships: {},
      isHidden: false,
      compareRows: makeComparator([['id', 'asc']]),
    } as const;

    const format: Format = {singular: false, relationships: {}};

    // Helper to reduce boilerplate in tests
    const apply = (root: Entry, change: ViewChange) =>
      applyChange(root, change, simpleSchema, '', format, WITH_IDS, NO_MUTATE);

    //   [A]  ──add B──►  [A, B]
    //    ↑                 ↑
    //    └─── same ref ────┘
    test('unchanged rows keep their reference when adding a new row', () => {
      let root: Entry = {'': []};

      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });

      const firstRowRef = at(root, '', 0);

      root = apply(root, {
        type: 'add',
        node: {row: {id: '2', name: 'Bob'}, relationships: {}},
      });

      // First row should keep same reference (toBe checks reference equality)
      expect(at(root, '', 0)).toBe(firstRowRef);
      expect(entries(root, '')).toHaveLength(2);
    });

    test('unchanged rows keep their reference when removing another row', () => {
      let root: Entry = {'': []};

      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });
      root = apply(root, {
        type: 'add',
        node: {row: {id: '2', name: 'Bob'}, relationships: {}},
      });

      const firstRowRef = at(root, '', 0);

      root = apply(root, {
        type: 'remove',
        node: {row: {id: '2', name: 'Bob'}, relationships: {}},
      });

      // First row should keep same reference
      expect(at(root, '', 0)).toBe(firstRowRef);
      expect(entries(root, '')).toHaveLength(1);
    });

    test('unchanged rows keep their reference when editing another row', () => {
      let root: Entry = {'': []};

      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });
      root = apply(root, {
        type: 'add',
        node: {row: {id: '2', name: 'Bob'}, relationships: {}},
      });

      const firstRowRef = at(root, '', 0);

      root = apply(root, {
        type: 'edit',
        oldNode: {row: {id: '2', name: 'Bob'}},
        node: {row: {id: '2', name: 'Bobby'}},
      });

      // First row should keep same reference
      expect(at(root, '', 0)).toBe(firstRowRef);
      // Second row should have new reference with updated data
      expect(at(root, '', 1)).not.toBe(firstRowRef);
      expect(at(root, '', 1)).toEqual(
        expect.objectContaining({id: '2', name: 'Bobby'}),
      );
    });

    //   [A]  ──edit A──►  [A']
    //    │                  │
    //    └── different ref ─┘
    test('edited row gets new reference', () => {
      let root: Entry = {'': []};

      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });

      const originalRef = at(root, '', 0);

      root = apply(root, {
        type: 'edit',
        oldNode: {row: {id: '1', name: 'Alice'}},
        node: {row: {id: '1', name: 'Alicia'}},
      });

      // Edited row should have a new reference
      expect(at(root, '', 0)).not.toBe(originalRef);
      expect(at(root, '', 0)).toEqual(
        expect.objectContaining({id: '1', name: 'Alicia'}),
      );
    });

    test('root object changes when any modification occurs', () => {
      let root: Entry = {'': []};

      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });

      const rootRef = root;
      const listRef = entries(root, '');

      root = apply(root, {
        type: 'add',
        node: {row: {id: '2', name: 'Bob'}, relationships: {}},
      });

      // Root and list should have new references
      expect(root).not.toBe(rootRef);
      expect(root['']).not.toBe(listRef);
    });

    // Parent changes but untouched children keep their references:
    //
    //   P1 ─┬─ [C1, C2]    add C3     P1' ─┬─ [C1, C2, C3]
    //       │               ────►          │    ↑   ↑
    //       └─ ...                         └─   same refs
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
        applyChange(
          entry,
          change,
          schemaWithRelationship,
          '',
          formatWithRelationship,
          WITH_IDS,
          NO_MUTATE,
        );

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

    //   [A:rc=1]  ──add A──►  [A:rc=2]
    //       │                     │
    //       └─── different ref ───┘  (but same data)
    test('refCount increment creates new reference but preserves data', () => {
      let root: Entry = {'': []};

      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });

      const originalRef = at(root, '', 0);
      const originalRefCount = (originalRef as {[refCountSymbol]: number})[
        refCountSymbol
      ];

      // Add same row again (should increment refCount)
      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });

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

    // Sibling parents are independent. Changing P1's children doesn't touch P2:
    //
    //   [P1─[C1], P2─[C2]]    add C3 to P1    [P1'─[C1,C3], P2─[C2]]
    //              ↑            ────►                       ↑
    //              └────────────── same ref ────────────────┘
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
        applyChange(
          entry,
          change,
          schemaWithRelationship,
          '',
          formatWithRelationship,
          WITH_IDS,
          NO_MUTATE,
        );

      let root: Entry = {'': []};

      // Add two parents with children
      root = apply(root, {
        type: 'add',
        node: {
          row: {id: 'p1', name: 'Parent1'},
          relationships: {
            children: () => [
              {row: {id: 'c1', parentId: 'p1'}, relationships: {}},
            ],
          },
        },
      });

      root = apply(root, {
        type: 'add',
        node: {
          row: {id: 'p2', name: 'Parent2'},
          relationships: {
            children: () => [
              {row: {id: 'c2', parentId: 'p2'}, relationships: {}},
            ],
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

  // ═══════════════════════════════════════════════════════════════════════════
  // IN-PLACE MUTATION TESTS (mutate = true)
  // Verify that with mutate=true, containers are mutated in place but rows
  // remain immutable and views remain isolated.
  //
  // Key invariants:
  // 1. Multiple views never share object instances
  // 2. Row objects never mutate (always get new references when changed)
  // 3. Within a view: root and arrays ARE mutated in place
  // ═══════════════════════════════════════════════════════════════════════════
  describe('In-place mutation (mutate = true)', () => {
    const MUTATE = 1 as const;

    // Deep freeze helper to catch bugs where we mutate inputs
    function deepFreeze<T>(obj: T): T {
      Object.freeze(obj);
      for (const prop of Object.getOwnPropertyNames(obj)) {
        // oxlint-disable-next-line no-explicit-any
        const val = (obj as any)[prop];
        if (val && typeof val === 'object' && !Object.isFrozen(val)) {
          deepFreeze(val);
        }
      }

      return obj;
    }

    const simpleSchema = {
      tableName: 'item',
      columns: {id: {type: 'string'}, name: {type: 'string'}},
      primaryKey: ['id'],
      sort: [['id', 'asc']],
      system: 'client',
      relationships: {},
      isHidden: false,
      compareRows: makeComparator([['id', 'asc']]),
    } as const;

    const format: Format = {singular: false, relationships: {}};

    // Helper to reduce boilerplate in tests
    const apply = (root: Entry, change: ViewChange) =>
      applyChange(
        root,
        // we freeze here to catch potential bugs where applyChange mutates the
        // input change object (it shouldn't)
        deepFreeze(change),
        simpleSchema,
        '',
        format,
        WITH_IDS,
        MUTATE,
      );

    //   root/[A]  ──add B──►  root/[A, B]
    //     ↑   ↑                 ↑   ↑
    //     └───┴─ same refs ─────┴───┘
    test('root and array keep their reference when adding a new row', () => {
      let root: Entry = {'': []};
      const rootRef = root;
      const listRef = entries(root, '');

      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });

      const firstRowRef = at(root, '', 0);

      root = apply(root, {
        type: 'add',
        node: {row: {id: '2', name: 'Bob'}, relationships: {}},
      });

      // Root and list should keep same reference (mutated in place)
      expect(root).toBe(rootRef);
      expect(entries(root, '')).toBe(listRef);
      // First row should keep same reference (unchanged)
      expect(at(root, '', 0)).toBe(firstRowRef);
      expect(entries(root, '')).toHaveLength(2);
    });

    test('root and array keep their reference when removing a row', () => {
      let root: Entry = {'': []};
      const rootRef = root;
      const listRef = entries(root, '');

      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });
      root = apply(root, {
        type: 'add',
        node: {row: {id: '2', name: 'Bob'}, relationships: {}},
      });

      const firstRowRef = at(root, '', 0);

      root = apply(root, {
        type: 'remove',
        node: {row: {id: '2', name: 'Bob'}, relationships: {}},
      });

      // Root and list should keep same reference (mutated in place)
      expect(root).toBe(rootRef);
      expect(entries(root, '')).toBe(listRef);
      // First row should keep same reference (unchanged)
      expect(at(root, '', 0)).toBe(firstRowRef);
      expect(entries(root, '')).toHaveLength(1);
    });

    test('root, array and entries keep reference when editing a row', () => {
      let root: Entry = {'': []};
      const rootRef = root;
      const listRef = entries(root, '');

      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });
      root = apply(root, {
        type: 'add',
        node: {row: {id: '2', name: 'Bob'}, relationships: {}},
      });

      const firstRowRef = at(root, '', 0);
      const secondRowRef = at(root, '', 1);
      // Verify secondRowRef data before edit
      expect(secondRowRef).toEqual(
        expect.objectContaining({id: '2', name: 'Bob'}),
      );

      root = apply(root, {
        type: 'edit',
        oldNode: {row: {id: '2', name: 'Bob'}},
        node: {row: {id: '2', name: 'Bobby'}},
      });

      // Root and list should keep same reference (mutated in place)
      expect(root).toBe(rootRef);
      expect(entries(root, '')).toBe(listRef);
      // First row should keep same reference (unchanged)
      expect(at(root, '', 0)).toBe(firstRowRef);
      expect(at(root, '', 1)).toBe(secondRowRef);
    });

    //   [A]  ──edit A──►  [A']
    //    ↑                  │
    //  same ref       different ref
    //  (array)            (row)
    test('edited row keeps reference', () => {
      // TODO(arv): Mutate element if compare === 0
      let root: Entry = {'': []};
      const rootRef = root;
      const listRef = entries(root, '');

      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });

      const originalRef = at(root, '', 0);
      // Verify original data before edit
      expect(originalRef).toEqual(
        expect.objectContaining({id: '1', name: 'Alice'}),
      );

      root = apply(root, {
        type: 'edit',
        oldNode: {row: {id: '1', name: 'Alice'}},
        node: {row: {id: '1', name: 'Alicia'}},
      });

      // Root and list should keep same reference (mutated in place)
      expect(root).toBe(rootRef);
      expect(entries(root, '')).toBe(listRef);
      expect(at(root, '', 0)).toBe(originalRef);
      expect(at(root, '', 0)).toEqual(
        expect.objectContaining({id: '1', name: 'Alicia'}),
      );
    });

    test('root and list keep same reference on any mutation', () => {
      let root: Entry = {'': []};
      const rootRef = root;
      const listRef = entries(root, '');

      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });

      // Root and list should keep same reference (mutated in place)
      expect(root).toBe(rootRef);
      expect(root['']).toBe(listRef);

      root = apply(root, {
        type: 'add',
        node: {row: {id: '2', name: 'Bob'}, relationships: {}},
      });

      // Root and list should still keep same reference
      expect(root).toBe(rootRef);
      expect(root['']).toBe(listRef);
    });

    // Root, parent, and children arrays all mutate in place; unchanged child rows keep refs:
    //
    //   P1 ─┬─ [C1, C2]    add C3     P1 ─┬─ [C1, C2, C3]
    //    ↑  │                ────►      ↑  │   ↑   ↑
    //    │  └─ same refs ───────────────┘  └───┴─ same refs
    //    └────────────────────────────────────┘
    test('parent and child arrays mutated in place, child rows preserved', () => {
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
        applyChange(
          entry,
          // we freeze here to catch potential bugs where applyChange mutates the
          // input change object (it shouldn't)
          deepFreeze(change),
          schemaWithRelationship,
          '',
          formatWithRelationship,
          WITH_IDS,
          MUTATE,
        );

      let root: Entry = {'': []};
      const rootRef = root;
      const listRef = entries(root, '');

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
      const childrenArrayRef = entries(parent1, 'children');
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

      // Root and list should keep same reference (mutated in place)
      expect(root).toBe(rootRef);
      expect(entries(root, '')).toBe(listRef);
      // Parent should keep same reference (mutated in place)
      expect(at(root, '', 0)).toBe(parent1);
      // Children array should keep same reference (mutated in place)
      expect(entries(at(root, '', 0), 'children')).toBe(childrenArrayRef);
      // Existing children should keep their references
      expect(at(at(root, '', 0), 'children', 0)).toBe(child1Ref);
      expect(at(at(root, '', 0), 'children', 1)).toBe(child2Ref);
      // New child is added
      expect(entries(at(root, '', 0), 'children')).toHaveLength(3);
    });

    //   [A:rc=1]  ──add A──►  [A:rc=2]
    //       ↑                     │
    //   same array          same array ref
    test('refCount increment mutates entry', () => {
      let root: Entry = {'': []};
      const rootRef = root;
      const listRef = entries(root, '');

      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });

      const originalRef = at(root, '', 0);
      const originalRefCount = (originalRef as {[refCountSymbol]: number})[
        refCountSymbol
      ];
      expect(originalRefCount).toBe(1);

      // Add same row again (should increment refCount)
      root = apply(root, {
        type: 'add',
        node: {row: {id: '1', name: 'Alicia'}, relationships: {}},
      });

      const newRef = at(root, '', 0);

      // Root and list should keep same reference (mutated in place)
      expect(root).toBe(rootRef);
      expect(entries(root, '')).toBe(listRef);
      expect(newRef).toBe(originalRef);
      // RefCount should be incremented
      expect((newRef as {[refCountSymbol]: number})[refCountSymbol]).toBe(
        originalRefCount + 1,
      );
    });

    // Sibling parents are independent. Changing P1's children doesn't touch P2:
    //
    //   [P1─[C1], P2─[C2]]    add C3 to P1    [P1─[C1,C3], P2─[C2]]
    //    ↑   ↑      ↑          ────►           ↑   ↑        ↑
    //    └───┴──────┴───────── all same refs ─┴───┴────────┘
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
        applyChange(
          entry,
          // we freeze here to catch potential bugs where applyChange mutates the
          // input change object (it shouldn't)
          deepFreeze(change),
          schemaWithRelationship,
          '',
          formatWithRelationship,
          WITH_IDS,
          MUTATE,
        );

      let root: Entry = {'': []};
      const rootRef = root;
      const listRef = entries(root, '');

      // Add two parents with children
      root = apply(root, {
        type: 'add',
        node: {
          row: {id: 'p1', name: 'Parent1'},
          relationships: {
            children: () => [
              {row: {id: 'c1', parentId: 'p1'}, relationships: {}},
            ],
          },
        },
      });

      root = apply(root, {
        type: 'add',
        node: {
          row: {id: 'p2', name: 'Parent2'},
          relationships: {
            children: () => [
              {row: {id: 'c2', parentId: 'p2'}, relationships: {}},
            ],
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

      // Root and list should keep same reference (mutated in place)
      expect(root).toBe(rootRef);
      expect(entries(root, '')).toBe(listRef);
      // Both parents should keep same reference (mutated in place)
      expect(at(root, '', 0)).toBe(parent1Ref);
      expect(at(root, '', 1)).toBe(parent2Ref);
      // Parent2's children should keep same reference
      expect(at(root, '', 1)['children']).toBe(parent2ChildrenRef);
    });

    // ═════════════════════════════════════════════════════════════════════════
    // VIEW ISOLATION TESTS
    // Verify that different views never share object instances
    // ═════════════════════════════════════════════════════════════════════════
    test('different views never share row instances', () => {
      let view1: Entry = {'': []};
      let view2: Entry = {'': []};

      // Add same row to both views
      view1 = apply(view1, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });

      view2 = apply(view2, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });

      // Views should be different objects
      expect(view1).not.toBe(view2);
      expect(entries(view1, '')).not.toBe(entries(view2, ''));
      // Row instances should be different (no sharing between views)
      expect(at(view1, '', 0)).not.toBe(at(view2, '', 0));
    });

    test('editing a row in one view does not affect other views', () => {
      let view1: Entry = {'': []};
      let view2: Entry = {'': []};

      // Add same row to both views
      view1 = apply(view1, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });

      view2 = apply(view2, {
        type: 'add',
        node: {row: {id: '1', name: 'Alice'}, relationships: {}},
      });

      const view2RowRef = at(view2, '', 0);

      // Edit row in view1
      view1 = apply(view1, {
        type: 'edit',
        oldNode: {row: {id: '1', name: 'Alice'}},
        node: {row: {id: '1', name: 'Alicia'}},
      });

      // View1 should have updated data
      expect(at(view1, '', 0)).toEqual(
        expect.objectContaining({id: '1', name: 'Alicia'}),
      );
      // View2 should be completely unchanged (same reference, same data)
      expect(at(view2, '', 0)).toBe(view2RowRef);
      expect(at(view2, '', 0)).toEqual(
        expect.objectContaining({id: '1', name: 'Alice'}),
      );
    });
  });
});
