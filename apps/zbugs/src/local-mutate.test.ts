import {test} from 'vitest';
import {zeroForTest} from '@rocicorp/zero/test-helpers';
import {builder, schema} from '../shared/schema.ts';
import {mutators} from '../shared/mutators.ts';
import type {AuthData} from '../shared/auth.ts';
import {defineMutator, defineMutators} from '@rocicorp/zero';
import {z} from 'zod/mini';

const userSchema = z.object({
  id: z.string(),
  login: z.string(),
  name: z.string(),
  role: z.union([z.literal('user'), z.literal('crew')]),
});

const mutatorsForTest = defineMutators({
  user: {
    create: defineMutator(userSchema, async ({tx, args}) => {
      await tx.mutate.user.insert({
        id: args.id,
        login: args.login,
        name: args.name,
        role: args.role,
        avatar: '',
      });
    }),
  },
});

test('local mutate', async () => {
  const zero = zeroForTest({
    cacheURL: null,
    kvStore: 'mem',
    schema,
    mutators: {
      ...mutators,
      ...mutatorsForTest,
    },
    userID: 'user-1',
    context: {sub: 'user-1', role: 'user'} as AuthData,
  });

  await zero.mutate(
    // @ts-ignore - wtf?
    mutatorsForTest.user.create({
      id: 'user-1',
      login: 'holden',
      name: 'James Holden',
      role: 'user',
    }),
  ).client;

  const result = await zero.mutate(
    // @ts-ignore - wtf?
    mutators.issue.create({
      id: 'issue-1',
      title: 'Test Issue',
      description: 'This is a test issue',
      projectID: 'project-1',
      created: Date.now(),
      modified: Date.now(),
    }),
  ).client;

  console.log('Result:', result);

  const issues = await zero.run(builder.issue);

  console.log('Issues:', issues);
});
