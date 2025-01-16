/**
 * Relationships as secondary.
 */

import {expectTypeOf, test} from 'vitest';
import {table, number, string} from './table-builder.js';
import {relationships} from './relationship-builder.js';
import type {Query} from '../../../zql/src/query/query.js';
import {createSchema} from './schema-builder.js';

const mockQuery = {
  select() {
    return this;
  },
  materialize() {
    return {
      get() {
        return this;
      },
    };
  },
  sub() {
    return this;
  },
  related() {
    return this;
  },
  where() {
    return this;
  },
  start() {
    return this;
  },
  one() {
    return this;
  },
  run() {
    return this;
  },
};

test('building a schema', () => {
  const user = table('user')
    .columns({
      id: string(),
      name: string(),
      recruiterId: number(),
    })
    .primaryKey('id');

  const issue = table('issue')
    .columns({
      id: string(),
      title: string(),
      ownerId: number(),
    })
    .primaryKey('id');

  const issueLabel = table('issueLabel')
    .columns({
      issueId: number(),
      labelId: number(),
    })
    .primaryKey('issueId', 'labelId');

  const label = table('label')
    .columns({
      id: number(),
      name: string(),
    })
    .primaryKey('id');

  const issueRelationships = relationships(issue, ({many, one}) => ({
    owner: one({
      sourceField: ['ownerId'],
      destField: ['id'],
      destSchema: user,
    }),
    labels: many(
      {
        sourceField: ['id'],
        destField: ['issueId'],
        destSchema: issueLabel,
      },
      {
        sourceField: ['labelId'],
        destField: ['id'],
        destSchema: label,
      },
    ),
  }));

  const userRelationships = relationships(user, ({one, many}) => ({
    recruiter: one({
      sourceField: ['id'],
      destField: ['recruiterId'],
      destSchema: user,
    }),
    ownedIssues: many({
      sourceField: ['id'],
      destField: ['ownerId'],
      destSchema: issue,
    }),
  }));

  const labelRelationships = relationships(label, ({many}) => ({
    issues: many(
      {
        sourceField: ['id'],
        destField: ['labelId'],
        destSchema: issueLabel,
      },
      {
        sourceField: ['issueId'],
        destField: ['id'],
        destSchema: issue,
      },
    ),
  }));

  const schema = createSchema(
    1,
    {user, issue, issueLabel, label},
    {
      userRelationships,
      issueRelationships,
      labelRelationships,
    },
  );

  const q = mockQuery as unknown as Query<typeof schema, 'user'>;
  const iq = mockQuery as unknown as Query<typeof schema, 'issue'>;
  const r = q
    .related('recruiter', q => q.related('recruiter', q => q.one()).one())
    .one()
    .run();
  expectTypeOf<typeof r>().toEqualTypeOf<
    | {
        readonly id: string;
        readonly name: string;
        readonly recruiterId: number;
        readonly recruiter:
          | {
              readonly id: string;
              readonly name: string;
              readonly recruiterId: number;
              readonly recruiter:
                | {
                    readonly id: string;
                    readonly name: string;
                    readonly recruiterId: number;
                  }
                | undefined;
            }
          | undefined;
      }
    | undefined
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  >({} as any);

  // recruiter is a singular relationship
  expectTypeOf(q.related('recruiter').run()).toEqualTypeOf<
    {
      readonly id: string;
      readonly name: string;
      readonly recruiterId: number;
      readonly recruiter:
        | {
            readonly id: string;
            readonly name: string;
            readonly recruiterId: number;
          }
        | undefined;
    }[]
  >();

  // recruiter is a singular relationship
  expectTypeOf(q.related('recruiter', q => q).run()).toEqualTypeOf<
    {
      readonly id: string;
      readonly name: string;
      readonly recruiterId: number;
      readonly recruiter:
        | {
            readonly id: string;
            readonly name: string;
            readonly recruiterId: number;
          }
        | undefined;
    }[]
  >();

  const id1 = iq
    .related('owner', q => q.related('ownedIssues', q => q.where('id', '1')))
    .run();
  expectTypeOf(id1).toEqualTypeOf<
    {
      readonly id: string;
      readonly title: string;
      readonly ownerId: number;
      readonly owner:
        | {
            readonly id: string;
            readonly name: string;
            readonly recruiterId: number;
            readonly ownedIssues: readonly {
              readonly id: string;
              readonly title: string;
              readonly ownerId: number;
            }[];
          }
        | undefined;
    }[]
  >({} as never);

  const id = iq.related('labels').run();
  expectTypeOf(id).toEqualTypeOf<
    {
      readonly id: string;
      readonly title: string;
      readonly ownerId: number;
      readonly labels: readonly {
        readonly id: number;
        readonly name: string;
      }[];
    }[]
  >();

  const lq = mockQuery as unknown as Query<typeof schema, 'label'>;
  const ld = lq.related('issues').run();
  expectTypeOf(ld).toEqualTypeOf<
    {
      readonly id: number;
      readonly name: string;
      readonly issues: readonly {
        readonly id: string;
        readonly title: string;
        readonly ownerId: number;
      }[];
    }[]
  >();
});
