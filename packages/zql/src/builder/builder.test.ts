import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST, Disjunction} from '../../../zero-protocol/src/ast.ts';
import {Catch} from '../ivm/catch.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {
  bindStaticParameters,
  buildPipeline,
  groupSubqueryConditions,
} from './builder.ts';
import {TestBuilderDelegate} from './test-builder-delegate.ts';

const lc = createSilentLogContext();

export function testBuilderDelegate() {
  const users = createSource(
    lc,
    testLogConfig,
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

  const states = createSource(
    lc,
    testLogConfig,
    'table',
    {code: {type: 'string'}},
    ['code'],
  );
  states.push({type: 'add', row: {code: 'CA'}});
  states.push({type: 'add', row: {code: 'HI'}});
  states.push({type: 'add', row: {code: 'AZ'}});
  states.push({type: 'add', row: {code: 'MD'}});
  states.push({type: 'add', row: {code: 'GA'}});

  const userStates = createSource(
    lc,
    testLogConfig,
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

  return {sources, delegate: new TestBuilderDelegate(sources)};
}

test('source-only', () => {
  const {sources, delegate} = testBuilderDelegate();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [
          ['name', 'asc'],
          ['id', 'asc'],
        ],
      },
      delegate,
      'query-id',
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "id": 1,
          "name": "aaron",
          "recruiterID": null,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 7,
          "name": "alex",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 5,
          "name": "cesar",
          "recruiterID": 3,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 6,
          "name": "darick",
          "recruiterID": 3,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 2,
          "name": "erik",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 3,
          "name": "greg",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 4,
          "name": "matt",
          "recruiterID": 1,
        },
      },
    ]
  `);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam'}});
  expect(sink.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {},
          "row": {
            "id": 8,
            "name": "sam",
          },
        },
        "type": "add",
      },
    ]
  `);
});

test('filter', () => {
  const {sources, delegate} = testBuilderDelegate();
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
      delegate,
      'query-id',
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "id": 6,
          "name": "darick",
          "recruiterID": 3,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 5,
          "name": "cesar",
          "recruiterID": 3,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 4,
          "name": "matt",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 3,
          "name": "greg",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 2,
          "name": "erik",
          "recruiterID": 1,
        },
      },
    ]
  `);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam'}});
  sources.users.push({type: 'add', row: {id: 9, name: 'abby'}});
  sources.users.push({type: 'remove', row: {id: 8, name: 'sam'}});
  expect(sink.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {},
          "row": {
            "id": 8,
            "name": "sam",
          },
        },
        "type": "add",
      },
      {
        "node": {
          "relationships": {},
          "row": {
            "id": 8,
            "name": "sam",
          },
        },
        "type": "remove",
      },
    ]
  `);
});

test('self-join', () => {
  const {sources, delegate} = testBuilderDelegate();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        related: [
          {
            system: 'client',
            correlation: {parentField: ['recruiterID'], childField: ['id']},
            subquery: {
              table: 'users',
              alias: 'recruiter',
              orderBy: [['id', 'asc']],
            },
          },
        ],
      },
      delegate,
      'query-id',
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
          "id": 4,
          "name": "matt",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {
          "recruiter": [
            {
              "relationships": {},
              "row": {
                "id": 3,
                "name": "greg",
                "recruiterID": 1,
              },
            },
          ],
        },
        "row": {
          "id": 5,
          "name": "cesar",
          "recruiterID": 3,
        },
      },
      {
        "relationships": {
          "recruiter": [
            {
              "relationships": {},
              "row": {
                "id": 3,
                "name": "greg",
                "recruiterID": 1,
              },
            },
          ],
        },
        "row": {
          "id": 6,
          "name": "darick",
          "recruiterID": 3,
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
          "id": 7,
          "name": "alex",
          "recruiterID": 1,
        },
      },
    ]
  `);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam', recruiterID: 2}});
  sources.users.push({type: 'add', row: {id: 9, name: 'abby', recruiterID: 8}});
  sources.users.push({
    type: 'remove',
    row: {id: 8, name: 'sam', recruiterID: 2},
  });
  sources.users.push({type: 'add', row: {id: 8, name: 'sam', recruiterID: 3}});

  expect(sink.pushes).toMatchInlineSnapshot(`
    [
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
            "id": 8,
            "name": "sam",
            "recruiterID": 2,
          },
        },
        "type": "add",
      },
      {
        "node": {
          "relationships": {
            "recruiter": [
              {
                "relationships": {},
                "row": {
                  "id": 8,
                  "name": "sam",
                  "recruiterID": 2,
                },
              },
            ],
          },
          "row": {
            "id": 9,
            "name": "abby",
            "recruiterID": 8,
          },
        },
        "type": "add",
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
            "id": 8,
            "name": "sam",
            "recruiterID": 2,
          },
        },
        "type": "remove",
      },
      {
        "child": {
          "change": {
            "node": {
              "relationships": {},
              "row": {
                "id": 8,
                "name": "sam",
                "recruiterID": 2,
              },
            },
            "type": "remove",
          },
          "relationshipName": "recruiter",
        },
        "row": {
          "id": 9,
          "name": "abby",
          "recruiterID": 8,
        },
        "type": "child",
      },
      {
        "node": {
          "relationships": {
            "recruiter": [
              {
                "relationships": {},
                "row": {
                  "id": 3,
                  "name": "greg",
                  "recruiterID": 1,
                },
              },
            ],
          },
          "row": {
            "id": 8,
            "name": "sam",
            "recruiterID": 3,
          },
        },
        "type": "add",
      },
      {
        "child": {
          "change": {
            "node": {
              "relationships": {},
              "row": {
                "id": 8,
                "name": "sam",
                "recruiterID": 3,
              },
            },
            "type": "add",
          },
          "relationshipName": "recruiter",
        },
        "row": {
          "id": 9,
          "name": "abby",
          "recruiterID": 8,
        },
        "type": "child",
      },
    ]
  `);
});

test('self-join edit', () => {
  const {sources, delegate} = testBuilderDelegate();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        related: [
          {
            system: 'client',
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
      delegate,
      'query-id',
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
  const {sources, delegate} = testBuilderDelegate();
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
            system: 'client',
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
                  system: 'client',
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
      delegate,
      'query-id',
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {
          "userStates": [
            {
              "relationships": {
                "states": [
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
          "userStates": [],
        },
        "row": {
          "id": 2,
          "name": "erik",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {
          "userStates": [
            {
              "relationships": {
                "states": [
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
                "states": [
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

  sources.userStates.push({type: 'add', row: {userID: 2, stateCode: 'HI'}});

  expect(sink.pushes).toMatchInlineSnapshot(`
    [
      {
        "child": {
          "change": {
            "node": {
              "relationships": {
                "states": [
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
            "type": "add",
          },
          "relationshipName": "userStates",
        },
        "row": {
          "id": 2,
          "name": "erik",
          "recruiterID": 1,
        },
        "type": "child",
      },
    ]
  `);
});

test('join with limit', () => {
  const {sources, delegate} = testBuilderDelegate();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        limit: 3,
        related: [
          {
            system: 'client',
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
                  system: 'client',
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
      delegate,
      'query-id',
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {
          "userStates": [
            {
              "relationships": {
                "states": [
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
          "userStates": [],
        },
        "row": {
          "id": 2,
          "name": "erik",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {
          "userStates": [
            {
              "relationships": {
                "states": [
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

  sources.userStates.push({type: 'add', row: {userID: 2, stateCode: 'HI'}});

  expect(sink.pushes).toMatchInlineSnapshot(`
    [
      {
        "child": {
          "change": {
            "node": {
              "relationships": {
                "states": [
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
            "type": "add",
          },
          "relationshipName": "userStates",
        },
        "row": {
          "id": 2,
          "name": "erik",
          "recruiterID": 1,
        },
        "type": "child",
      },
    ]
  `);
});

test('skip', () => {
  const {sources, delegate} = testBuilderDelegate();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        start: {row: {id: 3}, exclusive: true},
      },
      delegate,
      'query-id',
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "id": 4,
          "name": "matt",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 5,
          "name": "cesar",
          "recruiterID": 3,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 6,
          "name": "darick",
          "recruiterID": 3,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 7,
          "name": "alex",
          "recruiterID": 1,
        },
      },
    ]
  `);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam'}});
  expect(sink.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {},
          "row": {
            "id": 8,
            "name": "sam",
          },
        },
        "type": "add",
      },
    ]
  `);
});

test('exists junction', () => {
  const {sources, delegate} = testBuilderDelegate();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        limit: 2,
        where: {
          type: 'correlatedSubquery',
          related: {
            system: 'client',
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
                  system: 'client',
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
      delegate,
      'query-id',
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

test('duplicative exists junction', () => {
  const {sources, delegate} = testBuilderDelegate();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        limit: 2,
        where: {
          type: 'and',
          conditions: [
            {
              type: 'correlatedSubquery',
              related: {
                system: 'client',
                correlation: {parentField: ['id'], childField: ['userID']},
                subquery: {
                  table: 'userStates',
                  alias: 'zsubq_userStates',
                  orderBy: [
                    ['userID', 'asc'],
                    ['stateCode', 'asc'],
                  ],
                },
              },
              op: 'EXISTS',
            },
            {
              type: 'correlatedSubquery',
              related: {
                system: 'client',
                correlation: {parentField: ['id'], childField: ['userID']},
                subquery: {
                  table: 'userStates',
                  alias: 'zsubq_userStates',
                  orderBy: [
                    ['userID', 'asc'],
                    ['stateCode', 'asc'],
                  ],
                },
              },
              op: 'EXISTS',
            },
          ],
        },
      },
      delegate,
      'query-id',
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {
          "zsubq_userStates_0": [
            {
              "relationships": {},
              "row": {
                "stateCode": "HI",
                "userID": 1,
              },
            },
          ],
          "zsubq_userStates_1": [
            {
              "relationships": {},
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
          "zsubq_userStates_0": [
            {
              "relationships": {},
              "row": {
                "stateCode": "AZ",
                "userID": 3,
              },
            },
            {
              "relationships": {},
              "row": {
                "stateCode": "CA",
                "userID": 3,
              },
            },
          ],
          "zsubq_userStates_1": [
            {
              "relationships": {},
              "row": {
                "stateCode": "AZ",
                "userID": 3,
              },
            },
            {
              "relationships": {},
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
            "zsubq_userStates_0": [
              {
                "relationships": {},
                "row": {
                  "stateCode": "AZ",
                  "userID": 3,
                },
              },
              {
                "relationships": {},
                "row": {
                  "stateCode": "CA",
                  "userID": 3,
                },
              },
            ],
            "zsubq_userStates_1": [
              {
                "relationships": {},
                "row": {
                  "stateCode": "AZ",
                  "userID": 3,
                },
              },
              {
                "relationships": {},
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
            "zsubq_userStates_0": [
              {
                "relationships": {},
                "row": {
                  "stateCode": "HI",
                  "userID": 2,
                },
              },
            ],
            "zsubq_userStates_1": [
              {
                "relationships": {},
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
  const {sources, delegate} = testBuilderDelegate();
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
      delegate,
      'query-id',
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
  const {sources, delegate} = testBuilderDelegate();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        where: {
          type: 'correlatedSubquery',
          related: {
            system: 'client',
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
      delegate,
      'query-id',
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
  const {sources, delegate} = testBuilderDelegate();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        where: {
          type: 'correlatedSubquery',
          related: {
            system: 'client',
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
      delegate,
      'query-id',
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
        system: 'client',
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
          "system": "client",
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
  const {sources, delegate} = testBuilderDelegate();
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
      delegate,
      'query-id',
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`[]`);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam'}});
  expect(sink.pushes).toMatchInlineSnapshot(`[]`);
});

test('empty and - everything goes through', () => {
  const {sources, delegate} = testBuilderDelegate();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        where: {
          type: 'and',
          conditions: [],
        },
      },
      delegate,
      'query-id',
    ),
  );

  expect(sink.fetch().length).toEqual(7);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam'}});
  expect(sink.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {},
          "row": {
            "id": 8,
            "name": "sam",
          },
        },
        "type": "add",
      },
    ]
  `);
});

test('always false literal comparison - nothing goes through', () => {
  const {sources, delegate} = testBuilderDelegate();
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
      delegate,
      'query-id',
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`[]`);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam'}});
  expect(sink.pushes).toMatchInlineSnapshot(`[]`);
});

test('always true literal comparison - everything goes through', () => {
  const {sources, delegate} = testBuilderDelegate();
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
      delegate,
      'query-id',
    ),
  );

  expect(sink.fetch()).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "id": 1,
          "name": "aaron",
          "recruiterID": null,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 2,
          "name": "erik",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 3,
          "name": "greg",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 4,
          "name": "matt",
          "recruiterID": 1,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 5,
          "name": "cesar",
          "recruiterID": 3,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 6,
          "name": "darick",
          "recruiterID": 3,
        },
      },
      {
        "relationships": {},
        "row": {
          "id": 7,
          "name": "alex",
          "recruiterID": 1,
        },
      },
    ]
  `);

  sources.users.push({type: 'add', row: {id: 8, name: 'sam'}});
  expect(sink.pushes).toMatchInlineSnapshot(`
    [
      {
        "node": {
          "relationships": {},
          "row": {
            "id": 8,
            "name": "sam",
          },
        },
        "type": "add",
      },
    ]
  `);
});

test('groupSubqueryConditions', () => {
  const empty: Disjunction = {
    type: 'or',
    conditions: [],
  };

  expect(groupSubqueryConditions(empty)).toEqual([[], []]);

  const oneSimple: Disjunction = {
    type: 'or',
    conditions: [
      {
        type: 'simple',
        left: {type: 'column', name: 'id'},
        op: '=',
        right: {type: 'literal', value: 1},
      },
    ],
  };

  expect(groupSubqueryConditions(oneSimple)).toEqual([
    [],
    [oneSimple.conditions[0]],
  ]);

  const oneSubquery: Disjunction = {
    type: 'or',
    conditions: [
      {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          system: 'client',
          correlation: {parentField: ['id'], childField: ['userID']},
          subquery: {
            table: 'userStates',
            alias: 'userStates',
            orderBy: [
              ['userID', 'asc'],
              ['stateCode', 'asc'],
            ],
          },
        },
      },
    ],
  };

  expect(groupSubqueryConditions(oneSubquery)).toEqual([
    [oneSubquery.conditions[0]],
    [],
  ]);

  const oneEach: Disjunction = {
    type: 'or',
    conditions: [oneSimple.conditions[0], oneSubquery.conditions[0]],
  };

  expect(groupSubqueryConditions(oneEach)).toEqual([
    [oneSubquery.conditions[0]],
    [oneSimple.conditions[0]],
  ]);

  const subqueryInAnd: Disjunction = {
    type: 'or',
    conditions: [
      {
        type: 'and',
        conditions: [oneSubquery.conditions[0]],
      },
      {
        type: 'and',
        conditions: [oneSimple.conditions[0]],
      },
    ],
  };

  expect(groupSubqueryConditions(subqueryInAnd)).toEqual([
    [subqueryInAnd.conditions[0]],
    [subqueryInAnd.conditions[1]],
  ]);
});
