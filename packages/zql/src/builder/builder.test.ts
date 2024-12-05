import {expect, test} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.js';
import {Catch} from '../ivm/catch.js';
import {MemoryStorage} from '../ivm/memory-storage.js';
import type {Source} from '../ivm/source.js';
import {createSource} from '../ivm/test/source-factory.js';
import {bindStaticParameters, buildPipeline} from './builder.js';

export function testSources() {
  const users = createSource(
    'table',
    {
      id: {type: 'number'},
      name: {type: 'string'},
      recruiterID: {type: 'number'},
    },
    ['id'],
  );
  users.push({type: 'add', row: {id: 1, name: 'aaron', recruiterID: null}});
  users.push({type: 'add', row: {id: 2, name: 'erik', recruiterID: 1}});
  users.push({type: 'add', row: {id: 3, name: 'greg', recruiterID: 1}});
  users.push({type: 'add', row: {id: 4, name: 'matt', recruiterID: 1}});
  users.push({type: 'add', row: {id: 5, name: 'cesar', recruiterID: 3}});
  users.push({type: 'add', row: {id: 6, name: 'darick', recruiterID: 3}});
  users.push({type: 'add', row: {id: 7, name: 'alex', recruiterID: 1}});

  const states = createSource('table', {code: {type: 'string'}}, ['code']);
  states.push({type: 'add', row: {code: 'CA'}});
  states.push({type: 'add', row: {code: 'HI'}});
  states.push({type: 'add', row: {code: 'AZ'}});
  states.push({type: 'add', row: {code: 'MD'}});
  states.push({type: 'add', row: {code: 'GA'}});

  const userStates = createSource(
    'table',
    {userID: {type: 'number'}, stateCode: {type: 'string'}},
    ['userID', 'stateCode'],
  );
  userStates.push({type: 'add', row: {userID: 1, stateCode: 'HI'}});
  userStates.push({type: 'add', row: {userID: 3, stateCode: 'AZ'}});
  userStates.push({type: 'add', row: {userID: 3, stateCode: 'CA'}});
  userStates.push({type: 'add', row: {userID: 4, stateCode: 'MD'}});
  userStates.push({type: 'add', row: {userID: 5, stateCode: 'AZ'}});
  userStates.push({type: 'add', row: {userID: 6, stateCode: 'CA'}});
  userStates.push({type: 'add', row: {userID: 7, stateCode: 'GA'}});

  const sources = {users, userStates, states};

  function getSource(name: string) {
    return (sources as Record<string, Source>)[name];
  }

  return {sources, getSource};
}

test('source-only', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [
          ['name', 'asc'],
          ['id', 'asc'],
        ],
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toEqual([
    {row: {id: 1, name: 'aaron', recruiterID: null}, relationships: {}},
    {row: {id: 7, name: 'alex', recruiterID: 1}, relationships: {}},
    {row: {id: 5, name: 'cesar', recruiterID: 3}, relationships: {}},
    {row: {id: 6, name: 'darick', recruiterID: 3}, relationships: {}},
    {row: {id: 2, name: 'erik', recruiterID: 1}, relationships: {}},
    {row: {id: 3, name: 'greg', recruiterID: 1}, relationships: {}},
    {row: {id: 4, name: 'matt', recruiterID: 1}, relationships: {}},
  ]);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam'}});
  expect(sink.pushes).toEqual([
    {
      type: 'add',
      node: {row: {id: 8, name: 'sam'}, relationships: {}},
    },
  ]);
});

test('filter', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'desc']],
        where: {
          type: 'simple',
          left: {
            type: 'column',
            name: 'name',
          },
          op: '>=',
          right: {
            type: 'literal',
            value: 'c',
          },
        },
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toEqual([
    {row: {id: 6, name: 'darick', recruiterID: 3}, relationships: {}},
    {row: {id: 5, name: 'cesar', recruiterID: 3}, relationships: {}},
    {row: {id: 4, name: 'matt', recruiterID: 1}, relationships: {}},
    {row: {id: 3, name: 'greg', recruiterID: 1}, relationships: {}},
    {row: {id: 2, name: 'erik', recruiterID: 1}, relationships: {}},
  ]);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam'}});
  sources.users.push({type: 'add', row: {id: 9, name: 'abby'}});
  sources.users.push({type: 'remove', row: {id: 8, name: 'sam'}});
  expect(sink.pushes).toEqual([
    {
      type: 'add',
      node: {row: {id: 8, name: 'sam'}, relationships: {}},
    },
    {
      type: 'remove',
      node: {row: {id: 8, name: 'sam'}, relationships: {}},
    },
  ]);
});

test('self-join', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        related: [
          {
            correlation: {parentField: ['recruiterID'], childField: ['id']},
            subquery: {
              table: 'users',
              alias: 'recruiter',
              orderBy: [['id', 'asc']],
            },
          },
        ],
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toEqual([
    {
      row: {id: 1, name: 'aaron', recruiterID: null},
      relationships: {
        recruiter: [],
      },
    },
    {
      row: {id: 2, name: 'erik', recruiterID: 1},
      relationships: {
        recruiter: [
          {row: {id: 1, name: 'aaron', recruiterID: null}, relationships: {}},
        ],
      },
    },
    {
      row: {id: 3, name: 'greg', recruiterID: 1},
      relationships: {
        recruiter: [
          {row: {id: 1, name: 'aaron', recruiterID: null}, relationships: {}},
        ],
      },
    },
    {
      row: {id: 4, name: 'matt', recruiterID: 1},
      relationships: {
        recruiter: [
          {row: {id: 1, name: 'aaron', recruiterID: null}, relationships: {}},
        ],
      },
    },
    {
      row: {id: 5, name: 'cesar', recruiterID: 3},
      relationships: {
        recruiter: [
          {row: {id: 3, name: 'greg', recruiterID: 1}, relationships: {}},
        ],
      },
    },
    {
      row: {id: 6, name: 'darick', recruiterID: 3},
      relationships: {
        recruiter: [
          {row: {id: 3, name: 'greg', recruiterID: 1}, relationships: {}},
        ],
      },
    },
    {
      row: {id: 7, name: 'alex', recruiterID: 1},
      relationships: {
        recruiter: [
          {row: {id: 1, name: 'aaron', recruiterID: null}, relationships: {}},
        ],
      },
    },
  ]);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam', recruiterID: 2}});
  sources.users.push({type: 'add', row: {id: 9, name: 'abby', recruiterID: 8}});
  sources.users.push({
    type: 'remove',
    row: {id: 8, name: 'sam', recruiterID: 2},
  });
  sources.users.push({type: 'add', row: {id: 8, name: 'sam', recruiterID: 3}});

  expect(sink.pushes).toEqual([
    {
      type: 'add',
      node: {
        row: {id: 8, name: 'sam', recruiterID: 2},
        relationships: {
          recruiter: [
            {row: {id: 2, name: 'erik', recruiterID: 1}, relationships: {}},
          ],
        },
      },
    },
    {
      type: 'add',
      node: {
        row: {id: 9, name: 'abby', recruiterID: 8},
        relationships: {
          recruiter: [
            {row: {id: 8, name: 'sam', recruiterID: 2}, relationships: {}},
          ],
        },
      },
    },
    {
      type: 'remove',
      node: {
        row: {id: 8, name: 'sam', recruiterID: 2},
        relationships: {
          recruiter: [
            {row: {id: 2, name: 'erik', recruiterID: 1}, relationships: {}},
          ],
        },
      },
    },
    {
      type: 'child',
      row: {id: 9, name: 'abby', recruiterID: 8},
      child: {
        relationshipName: 'recruiter',
        change: {
          type: 'remove',
          node: {row: {id: 8, name: 'sam', recruiterID: 2}, relationships: {}},
        },
      },
    },
    {
      type: 'add',
      node: {
        row: {id: 8, name: 'sam', recruiterID: 3},
        relationships: {
          recruiter: [
            {row: {id: 3, name: 'greg', recruiterID: 1}, relationships: {}},
          ],
        },
      },
    },
    {
      type: 'child',
      row: {id: 9, name: 'abby', recruiterID: 8},
      child: {
        relationshipName: 'recruiter',
        change: {
          type: 'add',
          node: {row: {id: 8, name: 'sam', recruiterID: 3}, relationships: {}},
        },
      },
    },
  ]);
});

test('self-join edit', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        related: [
          {
            correlation: {parentField: ['recruiterID'], childField: ['id']},
            subquery: {
              table: 'users',
              alias: 'recruiter',
              orderBy: [['id', 'asc']],
            },
          },
        ],
        limit: 3,
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {
          "recruiter": [],
        },
        "row": {
          "id": 1,
          "name": "aaron",
          "recruiterID": null,
        },
      },
      {
        "relationships": {
          "recruiter": [
            {
              "relationships": {},
              "row": {
                "id": 1,
                "name": "aaron",
                "recruiterID": null,
              },
            },
          ],
        },
        "row": {
          "id": 2,
          "name": "erik",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {
          "recruiter": [
            {
              "relationships": {},
              "row": {
                "id": 1,
                "name": "aaron",
                "recruiterID": null,
              },
            },
          ],
        },
        "row": {
          "id": 3,
          "name": "greg",
          "recruiterID": 1,
        },
      },
    ]
  `);

  // or was greg recruited by erik
  sources.users.push({
    type: 'edit',
    oldRow: {
      id: 3,
      name: 'greg',
      recruiterID: 1,
    },
    row: {
      id: 3,
      name: 'greg',
      recruiterID: 2,
    },
  });

  expect(sink.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {
            "recruiter": [
              {
                "relationships": {},
                "row": {
                  "id": 1,
                  "name": "aaron",
                  "recruiterID": null,
                },
              },
            ],
          },
          "row": {
            "id": 3,
            "name": "greg",
            "recruiterID": 1,
          },
        },
        "type": "remove",
      },
      {
        "node": {
          "relationships": {
            "recruiter": [
              {
                "relationships": {},
                "row": {
                  "id": 2,
                  "name": "erik",
                  "recruiterID": 1,
                },
              },
            ],
          },
          "row": {
            "id": 3,
            "name": "greg",
            "recruiterID": 2,
          },
        },
        "type": "add",
      },
    ]
  `);
});

test('multi-join', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        where: {
          type: 'simple',
          left: {
            type: 'column',
            name: 'id',
          },
          op: '<=',
          right: {
            type: 'literal',
            value: 3,
          },
        },
        related: [
          {
            correlation: {parentField: ['id'], childField: ['userID']},
            subquery: {
              table: 'userStates',
              alias: 'userStates',
              orderBy: [
                ['userID', 'asc'],
                ['stateCode', 'asc'],
              ],
              related: [
                {
                  correlation: {
                    parentField: ['stateCode'],
                    childField: ['code'],
                  },
                  subquery: {
                    table: 'states',
                    alias: 'states',
                    orderBy: [['code', 'asc']],
                  },
                },
              ],
            },
          },
        ],
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toEqual([
    {
      row: {id: 1, name: 'aaron', recruiterID: null},
      relationships: {
        userStates: [
          {
            row: {userID: 1, stateCode: 'HI'},
            relationships: {
              states: [{row: {code: 'HI'}, relationships: {}}],
            },
          },
        ],
      },
    },
    {
      row: {id: 2, name: 'erik', recruiterID: 1},
      relationships: {
        userStates: [],
      },
    },
    {
      row: {id: 3, name: 'greg', recruiterID: 1},
      relationships: {
        userStates: [
          {
            row: {userID: 3, stateCode: 'AZ'},
            relationships: {
              states: [{row: {code: 'AZ'}, relationships: {}}],
            },
          },
          {
            row: {userID: 3, stateCode: 'CA'},
            relationships: {
              states: [{row: {code: 'CA'}, relationships: {}}],
            },
          },
        ],
      },
    },
  ]);

  sources.userStates.push({type: 'add', row: {userID: 2, stateCode: 'HI'}});

  expect(sink.pushes).toEqual([
    {
      type: 'child',
      row: {id: 2, name: 'erik', recruiterID: 1},
      child: {
        relationshipName: 'userStates',
        change: {
          type: 'add',
          node: {
            row: {userID: 2, stateCode: 'HI'},
            relationships: {
              states: [{row: {code: 'HI'}, relationships: {}}],
            },
          },
        },
      },
    },
  ]);
});

test('join with limit', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        limit: 3,
        related: [
          {
            correlation: {parentField: ['id'], childField: ['userID']},
            subquery: {
              table: 'userStates',
              alias: 'userStates',
              orderBy: [
                ['userID', 'asc'],
                ['stateCode', 'asc'],
              ],
              limit: 1,
              related: [
                {
                  correlation: {
                    parentField: ['stateCode'],
                    childField: ['code'],
                  },
                  subquery: {
                    table: 'states',
                    alias: 'states',
                    orderBy: [['code', 'asc']],
                  },
                },
              ],
            },
          },
        ],
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toEqual([
    {
      row: {id: 1, name: 'aaron', recruiterID: null},
      relationships: {
        userStates: [
          {
            row: {userID: 1, stateCode: 'HI'},
            relationships: {
              states: [{row: {code: 'HI'}, relationships: {}}],
            },
          },
        ],
      },
    },
    {
      row: {id: 2, name: 'erik', recruiterID: 1},
      relationships: {
        userStates: [],
      },
    },
    {
      row: {id: 3, name: 'greg', recruiterID: 1},
      relationships: {
        userStates: [
          {
            row: {userID: 3, stateCode: 'AZ'},
            relationships: {
              states: [{row: {code: 'AZ'}, relationships: {}}],
            },
          },
        ],
      },
    },
  ]);

  sources.userStates.push({type: 'add', row: {userID: 2, stateCode: 'HI'}});

  expect(sink.pushes).toEqual([
    {
      type: 'child',
      row: {id: 2, name: 'erik', recruiterID: 1},
      child: {
        relationshipName: 'userStates',
        change: {
          type: 'add',
          node: {
            row: {userID: 2, stateCode: 'HI'},
            relationships: {
              states: [{row: {code: 'HI'}, relationships: {}}],
            },
          },
        },
      },
    },
  ]);
});

test('skip', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        start: {row: {id: 3}, exclusive: true},
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toEqual([
    {row: {id: 4, name: 'matt', recruiterID: 1}, relationships: {}},
    {row: {id: 5, name: 'cesar', recruiterID: 3}, relationships: {}},
    {row: {id: 6, name: 'darick', recruiterID: 3}, relationships: {}},
    {row: {id: 7, name: 'alex', recruiterID: 1}, relationships: {}},
  ]);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam'}});
  expect(sink.pushes).toEqual([
    {
      type: 'add',
      node: {row: {id: 8, name: 'sam'}, relationships: {}},
    },
  ]);
});

test('exists junction', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        limit: 2,
        where: {
          type: 'correlatedSubquery',
          related: {
            correlation: {parentField: ['id'], childField: ['userID']},
            subquery: {
              table: 'userStates',
              alias: 'zsubq_userStates',
              orderBy: [
                ['userID', 'asc'],
                ['stateCode', 'asc'],
              ],
              where: {
                type: 'correlatedSubquery',
                related: {
                  correlation: {
                    parentField: ['stateCode'],
                    childField: ['code'],
                  },
                  subquery: {
                    table: 'states',
                    alias: 'zsubq_states',
                    orderBy: [['code', 'asc']],
                  },
                },
                op: 'EXISTS',
              },
            },
          },
          op: 'EXISTS',
        },
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {
          "zsubq_userStates": [
            {
              "relationships": {
                "zsubq_states": [
                  {
                    "relationships": {},
                    "row": {
                      "code": "HI",
                    },
                  },
                ],
              },
              "row": {
                "stateCode": "HI",
                "userID": 1,
              },
            },
          ],
        },
        "row": {
          "id": 1,
          "name": "aaron",
          "recruiterID": null,
        },
      },
      {
        "relationships": {
          "zsubq_userStates": [
            {
              "relationships": {
                "zsubq_states": [
                  {
                    "relationships": {},
                    "row": {
                      "code": "AZ",
                    },
                  },
                ],
              },
              "row": {
                "stateCode": "AZ",
                "userID": 3,
              },
            },
            {
              "relationships": {
                "zsubq_states": [
                  {
                    "relationships": {},
                    "row": {
                      "code": "CA",
                    },
                  },
                ],
              },
              "row": {
                "stateCode": "CA",
                "userID": 3,
              },
            },
          ],
        },
        "row": {
          "id": 3,
          "name": "greg",
          "recruiterID": 1,
        },
      },
    ]
  `);

  // erik moves to hawaii
  sources.userStates.push({type: 'add', row: {userID: 2, stateCode: 'HI'}});

  expect(sink.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {
            "zsubq_userStates": [
              {
                "relationships": {
                  "zsubq_states": [
                    {
                      "relationships": {},
                      "row": {
                        "code": "AZ",
                      },
                    },
                  ],
                },
                "row": {
                  "stateCode": "AZ",
                  "userID": 3,
                },
              },
              {
                "relationships": {
                  "zsubq_states": [
                    {
                      "relationships": {},
                      "row": {
                        "code": "CA",
                      },
                    },
                  ],
                },
                "row": {
                  "stateCode": "CA",
                  "userID": 3,
                },
              },
            ],
          },
          "row": {
            "id": 3,
            "name": "greg",
            "recruiterID": 1,
          },
        },
        "type": "remove",
      },
      {
        "node": {
          "relationships": {
            "zsubq_userStates": [
              {
                "relationships": {
                  "zsubq_states": [
                    {
                      "relationships": {},
                      "row": {
                        "code": "HI",
                      },
                    },
                  ],
                },
                "row": {
                  "stateCode": "HI",
                  "userID": 2,
                },
              },
            ],
          },
          "row": {
            "id": 2,
            "name": "erik",
            "recruiterID": 1,
          },
        },
        "type": "add",
      },
    ]
  `);
});

test('exists junction with limit, remove row after limit, and last row', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        limit: 2,
        where: {
          type: 'correlatedSubquery',
          related: {
            correlation: {parentField: ['id'], childField: ['userID']},
            subquery: {
              table: 'userStates',
              alias: 'zsubq_userStates',
              orderBy: [
                ['userID', 'asc'],
                ['stateCode', 'asc'],
              ],
              where: {
                type: 'correlatedSubquery',
                related: {
                  correlation: {
                    parentField: ['stateCode'],
                    childField: ['code'],
                  },
                  subquery: {
                    table: 'states',
                    alias: 'zsubq_states',
                    orderBy: [['code', 'asc']],
                  },
                },
                op: 'EXISTS',
              },
            },
          },
          op: 'EXISTS',
        },
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {
          "zsubq_userStates": [
            {
              "relationships": {
                "zsubq_states": [
                  {
                    "relationships": {},
                    "row": {
                      "code": "HI",
                    },
                  },
                ],
              },
              "row": {
                "stateCode": "HI",
                "userID": 1,
              },
            },
          ],
        },
        "row": {
          "id": 1,
          "name": "aaron",
          "recruiterID": null,
        },
      },
      {
        "relationships": {
          "zsubq_userStates": [
            {
              "relationships": {
                "zsubq_states": [
                  {
                    "relationships": {},
                    "row": {
                      "code": "AZ",
                    },
                  },
                ],
              },
              "row": {
                "stateCode": "AZ",
                "userID": 3,
              },
            },
            {
              "relationships": {
                "zsubq_states": [
                  {
                    "relationships": {},
                    "row": {
                      "code": "CA",
                    },
                  },
                ],
              },
              "row": {
                "stateCode": "CA",
                "userID": 3,
              },
            },
          ],
        },
        "row": {
          "id": 3,
          "name": "greg",
          "recruiterID": 1,
        },
      },
    ]
  `);

  // row after limit
  sources.users.push({
    type: 'remove',
    row: {id: 4, name: 'matt', recruiterID: 1},
  });

  expect(sink.pushes).toMatchInlineSnapshot(`[]`);

  // last row, also after limit
  sources.users.push({
    type: 'remove',
    row: {id: 7, name: 'alex', recruiterID: 1},
  });

  expect(sink.pushes).toMatchInlineSnapshot(`[]`);
});

test('exists self join', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        where: {
          type: 'correlatedSubquery',
          related: {
            correlation: {parentField: ['recruiterID'], childField: ['id']},
            subquery: {
              table: 'users',
              alias: 'zsubq_recruiter',
              orderBy: [['id', 'asc']],
            },
          },
          op: 'EXISTS',
        },
        limit: 2,
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {
          "zsubq_recruiter": [
            {
              "relationships": {},
              "row": {
                "id": 1,
                "name": "aaron",
                "recruiterID": null,
              },
            },
          ],
        },
        "row": {
          "id": 2,
          "name": "erik",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {
          "zsubq_recruiter": [
            {
              "relationships": {},
              "row": {
                "id": 1,
                "name": "aaron",
                "recruiterID": null,
              },
            },
          ],
        },
        "row": {
          "id": 3,
          "name": "greg",
          "recruiterID": 1,
        },
      },
    ]
  `);

  // or was greg recruited by erik
  sources.users.push({
    type: 'edit',
    oldRow: {
      id: 3,
      name: 'greg',
      recruiterID: 1,
    },
    row: {
      id: 3,
      name: 'greg',
      recruiterID: 2,
    },
  });

  expect(sink.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {
            "zsubq_recruiter": [
              {
                "relationships": {},
                "row": {
                  "id": 1,
                  "name": "aaron",
                  "recruiterID": null,
                },
              },
            ],
          },
          "row": {
            "id": 3,
            "name": "greg",
            "recruiterID": 1,
          },
        },
        "type": "remove",
      },
      {
        "node": {
          "relationships": {
            "zsubq_recruiter": [
              {
                "relationships": {},
                "row": {
                  "id": 1,
                  "name": "aaron",
                  "recruiterID": null,
                },
              },
            ],
          },
          "row": {
            "id": 4,
            "name": "matt",
            "recruiterID": 1,
          },
        },
        "type": "add",
      },
      {
        "node": {
          "relationships": {
            "zsubq_recruiter": [
              {
                "relationships": {},
                "row": {
                  "id": 1,
                  "name": "aaron",
                  "recruiterID": null,
                },
              },
            ],
          },
          "row": {
            "id": 4,
            "name": "matt",
            "recruiterID": 1,
          },
        },
        "type": "remove",
      },
      {
        "node": {
          "relationships": {
            "zsubq_recruiter": [
              {
                "relationships": {},
                "row": {
                  "id": 2,
                  "name": "erik",
                  "recruiterID": 1,
                },
              },
            ],
          },
          "row": {
            "id": 3,
            "name": "greg",
            "recruiterID": 2,
          },
        },
        "type": "add",
      },
    ]
  `);
});

test('not exists self join', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        where: {
          type: 'correlatedSubquery',
          related: {
            correlation: {parentField: ['recruiterID'], childField: ['id']},
            subquery: {
              table: 'users',
              alias: 'zsubq_recruiter',
              orderBy: [['id', 'asc']],
            },
          },
          op: 'NOT EXISTS',
        },
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {
          "zsubq_recruiter": [],
        },
        "row": {
          "id": 1,
          "name": "aaron",
          "recruiterID": null,
        },
      },
    ]
  `);

  // aaron recruited himself
  sources.users.push({
    type: 'edit',
    oldRow: {
      id: 1,
      name: 'aaron',
      recruiterID: null,
    },
    row: {
      id: 1,
      name: 'aaron',
      recruiterID: 1,
    },
  });

  expect(sink.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {
            "zsubq_recruiter": [],
          },
          "row": {
            "id": 1,
            "name": "aaron",
            "recruiterID": null,
          },
        },
        "type": "remove",
      },
    ]
  `);
});

test('bind static parameters', () => {
  // Static params are replaced with their values

  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    where: {
      type: 'simple',
      left: {
        type: 'column',
        name: 'id',
      },
      op: '=',
      right: {type: 'static', anchor: 'authData', field: 'userID'},
    },
    related: [
      {
        correlation: {parentField: ['id'], childField: ['userID']},
        subquery: {
          table: 'userStates',
          alias: 'userStates',
          where: {
            type: 'simple',
            left: {
              type: 'column',
              name: 'stateCode',
            },
            op: '=',
            right: {
              type: 'static',
              anchor: 'preMutationRow',
              field: 'stateCode',
            },
          },
        },
      },
    ],
  };

  const newAst = bindStaticParameters(ast, {
    authData: {userID: 1},
    preMutationRow: {stateCode: 'HI'},
  });

  expect(newAst).toMatchInlineSnapshot(`
    {
      "orderBy": [
        [
          "id",
          "asc",
        ],
      ],
      "related": [
        {
          "correlation": {
            "childField": [
              "userID",
            ],
            "parentField": [
              "id",
            ],
          },
          "subquery": {
            "alias": "userStates",
            "related": undefined,
            "table": "userStates",
            "where": {
              "left": {
                "name": "stateCode",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "HI",
              },
              "type": "simple",
            },
          },
        },
      ],
      "table": "users",
      "where": {
        "left": {
          "name": "id",
          "type": "column",
        },
        "op": "=",
        "right": {
          "type": "literal",
          "value": 1,
        },
        "type": "simple",
      },
    }
  `);
});

test('empty or - nothing goes through', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        where: {
          type: 'or',
          conditions: [],
        },
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toEqual([]);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam'}});
  expect(sink.pushes).toEqual([]);
});

test('always false literal comparison - nothing goes through', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        where: {
          type: 'simple',
          left: {
            type: 'literal',
            value: true,
          },
          op: '=',
          right: {
            type: 'literal',
            value: false,
          },
        },
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toEqual([]);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam'}});
  expect(sink.pushes).toEqual([]);
});

test('always true literal comparison - everything goes through', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        where: {
          type: 'simple',
          left: {
            type: 'literal',
            value: true,
          },
          op: '=',
          right: {
            type: 'literal',
            value: true,
          },
        },
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.fetch()).toEqual([
    {
      relationships: {},
      row: {
        id: 1,
        name: 'aaron',
        recruiterID: null,
      },
    },
    {
      relationships: {},
      row: {
        id: 2,
        name: 'erik',
        recruiterID: 1,
      },
    },
    {
      relationships: {},
      row: {
        id: 3,
        name: 'greg',
        recruiterID: 1,
      },
    },
    {
      relationships: {},
      row: {
        id: 4,
        name: 'matt',
        recruiterID: 1,
      },
    },
    {
      relationships: {},
      row: {
        id: 5,
        name: 'cesar',
        recruiterID: 3,
      },
    },
    {
      relationships: {},
      row: {
        id: 6,
        name: 'darick',
        recruiterID: 3,
      },
    },
    {
      relationships: {},
      row: {
        id: 7,
        name: 'alex',
        recruiterID: 1,
      },
    },
  ]);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam'}});
  expect(sink.pushes).toEqual([
    {
      node: {
        relationships: {},
        row: {
          id: 8,
          name: 'sam',
        },
      },
      type: 'add',
    },
  ]);
});
