import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import {Database} from '../../../../zqlite/src/db.js';
import {WriteAuthorizerImpl} from './write-authorizer.js';
import type {
  AuthorizationConfig,
  Rule,
} from '../../../../zero-schema/src/compiled-authorization.js';

const lc = createSilentLogContext();

const allowIfSubject = [
  'allow',
  {
    table: 'foo',
    where: {
      type: 'simple',
      field: 'id',
      op: '=',
      value: {anchor: 'authData', field: 'sub', type: 'static'},
    },
    orderBy: [['id', 'asc']],
  },
] satisfies Rule;

describe('can insert/update/delete/upsert', () => {
  let replica: Database;
  beforeEach(() => {
    replica = new Database(lc, ':memory:');
    replica.exec(/*sql*/ `CREATE TABLE foo (id TEXT PRIMARY KEY, a TEXT);
      INSERT INTO foo VALUES ('1', 'a');`);
  });

  test.each([
    {
      name: 'no policies',
      expected: true,
      authorization: undefined,
    },
    {
      name: 'empty (deny) row policy',
      expected: false,
      authorization: {
        foo: {
          row: {
            insert: [],
            update: [],
            delete: [],
          },
        },
      },
    },
    {
      name: 'empty (deny) cell policy',
      expected: false,
      authorization: {
        foo: {
          cell: {
            a: {
              insert: [],
              update: [],
              delete: [],
            },
          },
        },
      },
    },
    {
      name: 'row - allow if subject',
      expected: true,
      sub: '1',
      authorization: {
        foo: {
          row: {
            update: [allowIfSubject],
            delete: [allowIfSubject],
          },
        },
      },
    },
    {
      name: 'row - allow if subject denies when the subject is different',
      expected: false,
      sub: '2',
      authorization: {
        foo: {
          row: {
            insert: [],
            update: [allowIfSubject],
            delete: [allowIfSubject],
          },
        },
      },
    },
    {
      name: 'upsert uses insert policy if no row exists',
      id: '2',
      expected: false,
      actions: ['Upsert'],
      authorization: {
        foo: {
          row: {
            insert: [],
          },
        },
      },
    },
    {
      name: 'upsert uses update policy if a row exists to update',
      id: '1',
      expected: false,
      actions: ['Upsert'],
      authorization: {
        foo: {
          row: {
            update: [],
          },
        },
      },
    },
  ] satisfies {
    name: string;
    sub?: string | undefined;
    id?: string | undefined;
    actions?: ('Insert' | 'Update' | 'Delete' | 'Upsert')[] | undefined;
    expected: boolean;
    authorization: AuthorizationConfig | undefined;
  }[])('$name', ({authorization, sub, id, actions, expected}) => {
    const authorizer = new WriteAuthorizerImpl(
      lc,
      {},
      authorization,
      replica,
      'cg',
    );
    const jwtPayload = {
      sub: sub ?? '1',
    };
    for (const op of actions ??
      (['Insert', 'Update', 'Delete', 'Upsert'] as const)) {
      expect(
        authorizer[`can${op}`](jwtPayload, {
          tableName: 'foo',
          primaryKey: ['id'] as const,
          value: {id: id ?? 1, a: 'a'},
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
        } as any),
      ).toBe(expected);
    }
  });
});
