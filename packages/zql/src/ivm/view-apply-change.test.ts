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
});
