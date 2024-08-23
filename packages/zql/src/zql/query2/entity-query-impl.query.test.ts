import {expect, test} from 'vitest';
import {newEntityQuery} from './entity-query-impl.js';
import {testSources} from '../builder/builder.test.js';
import {MemoryStorage} from '../ivm2/memory-storage.js';
import {Catch} from '../ivm2/catch.js';

const userSchema = {
  table: 'users',
  fields: {
    id: {type: 'number'},
    name: {type: 'string'},
    recruiterID: {type: 'number'},
  },
  primaryKey: ['id'],
  relationships: {
    recruits: {
      source: 'id',
      dest: {
        field: 'recruiterID',
        schema: () => userSchema,
      },
    },
    recruiter: {
      source: 'recruiterID',
      dest: {
        field: 'id',
        schema: () => userSchema,
      },
    },
    state: {
      source: 'id',
      junction: {
        table: 'userStates',
        source: 'userID',
        dest: () => userStateSchema,
      },
      dest: {
        field: 'code',
        schema: () => stateSchema,
      },
    },
  },
} as const;

const stateSchema = {
  table: 'states',
  fields: {
    code: {type: 'string'},
  },
  primaryKey: ['code'],
} as const;

const userStateSchema = {
  table: 'userStates',
  fields: {
    userID: {type: 'number'},
    stateCode: {type: 'string'},
  },
  relationships: {
    users: {
      source: 'userID',
      dest: {
        field: 'id',
        schema: userSchema,
      },
    },
    states: {
      source: 'stateCode',
      dest: {
        field: 'code',
        schema: stateSchema,
      },
    },
  },
  primaryKey: ['userID', 'stateCode'],
} as const;

test('source-only', () => {
  const {sources, getSource} = testSources();
  const host = {getSource, createStorage: () => new MemoryStorage()};
  const sink = new Catch(
    newEntityQuery(host, userSchema).orderBy('name', 'asc').toPipeline(),
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

test('self-join', () => {
  const {getSource} = testSources();
  const host = {getSource, createStorage: () => new MemoryStorage()};
  const query = newEntityQuery(host, userSchema)
    .select('name')
    .related('recruits', q => q.select('name'))
    .where('name', '=', 'aaron');

  const sink = new Catch(query.toPipeline());
  expect(sink.fetch()).toEqual([
    {
      relationships: {
        recruits: [
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
              id: 7,
              name: 'alex',
              recruiterID: 1,
            },
          },
        ],
      },
      row: {
        id: 1,
        name: 'aaron',
        recruiterID: null,
      },
    },
  ]);
});

// More full featured tests to come after implementing `view` rather than testing against `toPipeline` and `catch`.
