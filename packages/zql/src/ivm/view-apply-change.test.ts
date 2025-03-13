import {describe, expect, test} from 'vitest';
import {makeComparator} from './data.ts';
import type {SourceSchema} from './schema.ts';
import {applyChange, type ViewChange} from './view-apply-change.ts';
import type {Entry, Format} from './view.ts';

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
          athleteID: {type: 'string'},
          disciplineID: {type: 'string'},
        },
        primaryKey: ['eventID', 'athleteID', 'disciplineID'],
        sort: [
          ['eventID', 'asc'],
          ['athleteID', 'asc'],
          ['disciplineID', 'asc'],
        ],
        system: 'client',
        relationships: {
          athletes: {
            tableName: 'athlete',
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
          },
        },
        isHidden: true,
        compareRows: makeComparator([
          ['eventID', 'asc'],
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
      const refCountMap = new Map<Entry, number>();
      const parentEntry: Entry = {'': []};
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
                    athleteID: 'a1',
                    disciplineID: 'd1',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
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
                    athleteID: 'a1',
                    disciplineID: 'd2',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
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
          applyChange(
            parentEntry,
            change,
            schema,
            relationship,
            format,
            refCountMap,
          );
        }

        expect(parentEntry).toMatchInlineSnapshot(`
    {
      "": [
        {
          "athletes": [
            {
              "id": "a1",
              "name": "Mason Ho",
            },
          ],
          "id": "e1",
          "name": "Buffalo Big Board Classic",
        },
      ],
    }
  `);
        expect(refCountMap).toMatchInlineSnapshot(`
      Map {
        {
          "athletes": [
            {
              "id": "a1",
              "name": "Mason Ho",
            },
          ],
          "id": "e1",
          "name": "Buffalo Big Board Classic",
        } => 1,
        {
          "id": "a1",
          "name": "Mason Ho",
        } => 2,
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
                    athleteID: 'a1',
                    disciplineID: 'd1',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
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
          applyChange(
            parentEntry,
            change,
            schema,
            relationship,
            format,
            refCountMap,
          );
        }

        expect(parentEntry).toMatchInlineSnapshot(`
      {
        "": [
          {
            "athletes": [
              {
                "id": "a1",
                "name": "Mason Ho",
              },
            ],
            "id": "e1",
            "name": "Buffalo Big Board Classic",
          },
        ],
      }
    `);
        expect(refCountMap).toMatchInlineSnapshot(`
      Map {
        {
          "athletes": [
            {
              "id": "a1",
              "name": "Mason Ho",
            },
          ],
          "id": "e1",
          "name": "Buffalo Big Board Classic",
        } => 1,
        {
          "id": "a1",
          "name": "Mason Ho",
        } => 1,
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
                    athleteID: 'a1',
                    disciplineID: 'd2',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
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
          applyChange(
            parentEntry,
            change,
            schema,
            relationship,
            format,
            refCountMap,
          );
        }

        expect(parentEntry).toMatchInlineSnapshot(`
      {
        "": [
          {
            "athletes": [],
            "id": "e1",
            "name": "Buffalo Big Board Classic",
          },
        ],
      }
    `);
        expect(refCountMap).toMatchInlineSnapshot(`
          Map {
            {
              "athletes": [],
              "id": "e1",
              "name": "Buffalo Big Board Classic",
            } => 1,
          }
        `);
      }
    });

    test('singular: true', () => {
      // This should really be a WeakMap but for testing purposes we use a Map.
      const refCountMap = new Map<Entry, number>();
      const parentEntry: Entry = {'': []};
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
                    athleteID: 'a1',
                    disciplineID: 'd1',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
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
                    athleteID: 'a1',
                    disciplineID: 'd2',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
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
          applyChange(
            parentEntry,
            change,
            schema,
            relationship,
            format,
            refCountMap,
          );
        }

        expect(parentEntry).toMatchInlineSnapshot(`
          {
            "": [
              {
                "athletes": {
                  "id": "a1",
                  "name": "Mason Ho",
                },
                "id": "e1",
                "name": "Buffalo Big Board Classic",
              },
            ],
          }
        `);
        expect(refCountMap).toMatchInlineSnapshot(`
          Map {
            {
              "athletes": {
                "id": "a1",
                "name": "Mason Ho",
              },
              "id": "e1",
              "name": "Buffalo Big Board Classic",
            } => 1,
            {
              "id": "a1",
              "name": "Mason Ho",
            } => 2,
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
                    athleteID: 'a1',
                    disciplineID: 'd1',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
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
          applyChange(
            parentEntry,
            change,
            schema,
            relationship,
            format,
            refCountMap,
          );
        }

        expect(parentEntry).toMatchInlineSnapshot(`
          {
            "": [
              {
                "athletes": {
                  "id": "a1",
                  "name": "Mason Ho",
                },
                "id": "e1",
                "name": "Buffalo Big Board Classic",
              },
            ],
          }
        `);
        expect(refCountMap).toMatchInlineSnapshot(`
          Map {
            {
              "athletes": {
                "id": "a1",
                "name": "Mason Ho",
              },
              "id": "e1",
              "name": "Buffalo Big Board Classic",
            } => 1,
            {
              "id": "a1",
              "name": "Mason Ho",
            } => 1,
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
                    athleteID: 'a1',
                    disciplineID: 'd2',
                  },
                  relationships: {
                    athletes: () => [
                      {
                        row: {
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
          applyChange(
            parentEntry,
            change,
            schema,
            relationship,
            format,
            refCountMap,
          );
        }

        expect(parentEntry).toMatchInlineSnapshot(`
          {
            "": [
              {
                "athletes": undefined,
                "id": "e1",
                "name": "Buffalo Big Board Classic",
              },
            ],
          }
        `);
        expect(refCountMap).toMatchInlineSnapshot(`
          Map {
            {
              "athletes": undefined,
              "id": "e1",
              "name": "Buffalo Big Board Classic",
            } => 1,
          }
        `);
      }
    });
  });

  describe('Simple', () => {
    test('singular: false', () => {
      const refCountMap = new Map<Entry, number>();
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
      const root = {'': []};
      const format = {
        singular: false,
        relationships: {},
      };

      const apply = (change: ViewChange) =>
        applyChange(root, change, schema, '', format, refCountMap);

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
        },
        {
          "id": "2",
          "name": "Greg",
        },
      ],
    }
  `);
      expect(refCountMap).toMatchInlineSnapshot(`
    Map {
      {
        "id": "1",
        "name": "Aaron",
      } => 1,
      {
        "id": "2",
        "name": "Greg",
      } => 5,
    }
  `);

      for (let i = 0; i < 4; i++) {
        apply({
          type: 'remove',
          node: {
            row: {
              id: '2',
              name: 'N/A',
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
        },
        {
          "id": "2",
          "name": "Greg",
        },
      ],
    }
  `);
      expect(refCountMap).toMatchInlineSnapshot(`
    Map {
      {
        "id": "1",
        "name": "Aaron",
      } => 1,
      {
        "id": "2",
        "name": "Greg",
      } => 1,
    }
  `);

      apply({
        type: 'remove',
        node: {
          row: {
            id: '2',
            name: 'N/A',
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
        },
      ],
    }
  `);
      expect(refCountMap).toMatchInlineSnapshot(`
    Map {
      {
        "id": "1",
        "name": "Aaron",
      } => 1,
    }
  `);

      expect(() =>
        apply({
          type: 'remove',
          node: {
            row: {
              id: '2',
              name: 'N/A', // when removing the non primary keys are ignored
            },
            relationships: {},
          },
        }),
      ).toThrowError(new Error('node does not exist'));

      // Add id:1 again but with a different name
      apply({
        type: 'add',
        node: {
          row: {
            id: '1',
            name: 'Darick',
          },
          relationships: {},
        },
      });
      expect(root).toMatchInlineSnapshot(`
    {
      "": [
        {
          "id": "1",
          "name": "Darick",
        },
      ],
    }
  `);
      expect(refCountMap).toMatchInlineSnapshot(`
    Map {
      {
        "id": "1",
        "name": "Darick",
      } => 2,
    }
  `);
    });

    test('singular: true', () => {
      const refCountMap = new Map<Entry, number>();
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
      const root = {'': undefined};
      const format = {
        singular: true,
        relationships: {},
      };

      const apply = (change: ViewChange) =>
        applyChange(root, change, schema, '', format, refCountMap);

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
          },
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Aaron",
          } => 1,
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
      ).toThrowError(new Error('single output already exists'));

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
          },
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Greg",
          } => 2,
        }
      `);

      for (let i = 0; i < 3; i++) {
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
          "": {
            "id": "1",
            "name": "Greg",
          },
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Greg",
          } => 5,
        }
      `);

      for (let i = 0; i < 4; i++) {
        apply({
          type: 'remove',
          node: {
            row: {
              id: '1',
              name: 'N/A',
            },
            relationships: {},
          },
        });
      }
      expect(root).toMatchInlineSnapshot(`
        {
          "": {
            "id": "1",
            "name": "Greg",
          },
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Greg",
          } => 1,
        }
      `);

      apply({
        type: 'remove',
        node: {
          row: {
            id: '1',
            name: 'N/A',
          },
          relationships: {},
        },
      });
      expect(root).toMatchInlineSnapshot(`
        {
          "": undefined,
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`Map {}`);

      expect(() =>
        apply({
          type: 'remove',
          node: {
            row: {
              id: '1',
              name: 'N/A',
            },
            relationships: {},
          },
        }),
      ).toThrowError(new Error('node does not exist'));
    });

    test('edit, singular: false', () => {
      const refCountMap = new Map<Entry, number>();
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
      const root = {'': []};
      const format = {
        singular: false,
        relationships: {},
      };

      const apply = (change: ViewChange) =>
        applyChange(root, change, schema, '', format, refCountMap);

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
            },
          ],
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Aaron",
          } => 1,
        }
      `);

      apply({
        type: 'edit',
        oldNode: {
          row: {
            id: '1',
            name: 'N/A',
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
            },
          ],
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Greg",
          } => 1,
        }
      `);

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
            },
          ],
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Aaron",
          } => 3,
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
            },
          ],
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Greg",
          } => 3,
        }
      `);
    });

    test('edit primary key, singular: false', () => {
      const refCountMap = new Map<Entry, number>();
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
      const root = {'': []};
      const format = {
        singular: false,
        relationships: {},
      };

      const apply = (change: ViewChange) =>
        applyChange(root, change, schema, '', format, refCountMap);

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
            },
          ],
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Aaron",
          } => 1,
        }
      `);

      apply({
        type: 'edit',
        oldNode: {
          row: {
            id: '1',
            name: 'N/A',
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
            },
          ],
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "2",
            "name": "Greg",
          } => 1,
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
            },
            {
              "id": "2",
              "name": "Greg",
            },
          ],
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Aaron",
          } => 2,
          {
            "id": "2",
            "name": "Greg",
          } => 2,
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
            },
            {
              "id": "2",
              "name": "Greg",
            },
          ],
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Aaron",
          } => 1,
          {
            "id": "2",
            "name": "Greg",
          } => 3,
        }
      `);
    });

    test('edit, singular: true', () => {
      const refCountMap = new Map<Entry, number>();
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
      const root = {'': undefined};
      const format = {
        singular: true,
        relationships: {},
      };

      const apply = (change: ViewChange) =>
        applyChange(root, change, schema, '', format, refCountMap);

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
          },
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Aaron",
          } => 1,
        }
      `);

      apply({
        type: 'edit',
        oldNode: {
          row: {
            id: '1',
            name: 'N/A',
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
          },
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Greg",
          } => 1,
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
          },
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Greg",
          } => 2,
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
          },
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Aaron",
          } => 2,
        }
      `);
    });

    test('edit primary key, singular: true', () => {
      const refCountMap = new Map<Entry, number>();
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
      const root = {'': undefined};
      const format = {
        singular: true,
        relationships: {},
      };

      const apply = (change: ViewChange) =>
        applyChange(root, change, schema, '', format, refCountMap);

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
          },
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Aaron",
          } => 1,
        }
      `);

      apply({
        type: 'edit',
        oldNode: {
          row: {
            id: '1',
            name: 'N/A',
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
          },
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "2",
            "name": "Greg",
          } => 1,
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
          },
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "2",
            "name": "Greg",
          } => 2,
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
          },
        }
      `);
      expect(refCountMap).toMatchInlineSnapshot(`
        Map {
          {
            "id": "1",
            "name": "Aaron",
          } => 2,
        }
      `);
    });
  });
});
