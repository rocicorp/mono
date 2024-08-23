import {expect, test} from 'vitest';
import {buildPipeline} from './builder.js';
import {Catch} from '../ivm2/catch.js';
import {MemorySource} from '../ivm2/memory-source.js';
import {MemoryStorage} from '../ivm2/memory-storage.js';

function testSources() {
  const users = new MemorySource({id: 'number', name: 'string'}, ['id']);
  users.push({type: 'add', row: {id: 1, name: 'aaron', recruiterID: null}});
  users.push({type: 'add', row: {id: 2, name: 'erik', recruiterID: 1}});
  users.push({type: 'add', row: {id: 3, name: 'greg', recruiterID: 1}});
  users.push({type: 'add', row: {id: 4, name: 'matt', recruiterID: 1}});
  users.push({type: 'add', row: {id: 5, name: 'cesar', recruiterID: 3}});
  users.push({type: 'add', row: {id: 6, name: 'darick', recruiterID: 3}});
  users.push({type: 'add', row: {id: 7, name: 'alex', recruiterID: 1}});

  const states = new MemorySource({code: 'string'}, ['code']);
  states.push({type: 'add', row: {code: 'CA'}});
  states.push({type: 'add', row: {code: 'HI'}});
  states.push({type: 'add', row: {code: 'AZ'}});
  states.push({type: 'add', row: {code: 'MD'}});
  states.push({type: 'add', row: {code: 'GA'}});

  const userStates = new MemorySource({userID: 'number', stateCode: 'string'}, [
    'userID',
    'stateCode',
  ]);
  userStates.push({type: 'add', row: {userID: 1, stateCode: 'HI'}});
  userStates.push({type: 'add', row: {userID: 3, stateCode: 'AZ'}});
  userStates.push({type: 'add', row: {userID: 4, stateCode: 'MD'}});
  userStates.push({type: 'add', row: {userID: 5, stateCode: 'AZ'}});
  userStates.push({type: 'add', row: {userID: 6, stateCode: 'CA'}});
  userStates.push({type: 'add', row: {userID: 7, stateCode: 'GA'}});

  const sources = {users, userStates, states};

  function getSource(name: string) {
    return (sources as Record<string, MemorySource>)[name];
  }

  return {sources, getSource};
}

test('source-only', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['name', 'asc']],
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.hydrate()).toEqual([
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
          field: 'name',
          op: '>=',
          value: 'c',
        },
      },
      {
        getSource,
        createStorage: () => new MemoryStorage(),
      },
    ),
  );

  expect(sink.hydrate()).toEqual([
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
        subqueries: [
          {
            correlation: {
              parentField: 'recruiterID',
              op: '=',
              childField: 'id',
            },
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

  expect(sink.hydrate()).toEqual([
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

test('multi-join', () => {
  const {sources, getSource} = testSources();
  const sink = new Catch(
    buildPipeline(
      {
        table: 'users',
        orderBy: [['id', 'asc']],
        where: {
          type: 'simple',
          field: 'id',
          op: '<=',
          value: 3,
        },
        subqueries: [
          {
            correlation: {
              parentField: 'id',
              op: '=',
              childField: 'userID',
            },
            subquery: {
              table: 'userStates',
              alias: 'userStates',
              orderBy: [
                ['userID', 'asc'],
                ['stateCode', 'asc'],
              ],
              subqueries: [
                {
                  correlation: {
                    parentField: 'stateCode',
                    op: '=',
                    childField: 'code',
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

  expect(sink.hydrate()).toEqual([
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
