import {expect, test, vi, beforeEach, afterEach} from 'vitest';
import {zeroForTest} from '@rocicorp/zero/testing';
import {builder, schema} from '../shared/schema.ts';
import {mutators} from '../shared/mutators.ts';
import {defineMutator, defineMutators} from '@rocicorp/zero';
import {z} from 'zod/mini';

const userSchema = z.object({
  id: z.string(),
  login: z.string(),
  name: z.string(),
  role: z.union([z.literal('user'), z.literal('crew')]),
});

// Use the merge overload to inherit context type from the base mutators
const mutatorsForTest = defineMutators(mutators, {
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

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2025-01-15T12:00:00Z'));
});

afterEach(() => {
  vi.useRealTimers();
});

test('local mutate', async () => {
  const zero = zeroForTest({
    cacheURL: null,
    kvStore: 'mem',
    schema,
    mutators: mutatorsForTest,
    userID: 'user-1',
    // oxlint-disable-next-line no-explicit-any
    context: {sub: 'user-1', role: 'user'} as any,
  });

  await zero.mutate(
    mutatorsForTest.user.create({
      id: 'user-1',
      login: 'holden',
      name: 'James Holden',
      role: 'user',
    }),
  ).client;

  await zero.mutate(
    mutatorsForTest.issue.create({
      id: 'issue-1',
      title: 'Test Issue',
      description: 'This is a test issue',
      projectID: 'project-1',
      created: Date.now(),
      modified: Date.now(),
    }),
  ).client;

  const issues = await zero.run(builder.issue);

  expect(issues).toMatchInlineSnapshot(`
    [
      {
        "assigneeID": null,
        "created": 1736942400000,
        "creatorID": "user-1",
        "description": "This is a test issue",
        "id": "issue-1",
        "modified": 1736942400000,
        "open": true,
        "projectID": "project-1",
        "shortID": null,
        "title": "Test Issue",
        "visibility": "public",
        Symbol(rc): 1,
      },
    ]
  `);
});
