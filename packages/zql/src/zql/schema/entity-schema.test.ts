import * as v from 'shared/src/valita.js';
import {expectTypeOf, test} from 'vitest';
import {InferType, table} from './entity-schema.js';

test('basic schema', () => {
  const userSchema = table({
    id: v.string(),
    name: v.string(),
    email: v.string(),
  });
  const issueSchema = table(
    {
      id: v.string(),
      title: v.string(),
      description: v.string().optional(),
      assignee: v.string(),
    },
    ['id'],
    {
      assignee: () => userSchema,
    },
  );

  type User = InferType<typeof userSchema>;
  expectTypeOf<User>().toMatchTypeOf<{
    id: string;
    name: string;
    email: string;
  }>();

  type Issue = InferType<typeof issueSchema>;
  expectTypeOf<Issue>().toMatchTypeOf<{
    id: string;
    title: string;
    description?: string | undefined;
    assignee: string;
  }>();
});
