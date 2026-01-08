import {test} from 'vitest';
import {zeroForTest} from '@rocicorp/zero/test-helpers';
import {schema} from '../shared/schema.ts';
import {mutators} from '../shared/mutators.ts';

test('local mutate', async () => {
  const zero = zeroForTest({
    cacheURL: null,
    kvStore: 'mem',
    schema,
    mutators,
    userID: 'user-1',
    context: {sub: 'user-1', role: 'user', name: 'User One'} as const,
  });

  const result = zero.mutate(
    mutators.issue.create({
      id: 'issue-1',
      title: 'Test Issue',
      description: 'This is a test issue',
      projectID: 'project-1',
      created: Date.now(),
      modified: Date.now(),
    }),
  );
});
