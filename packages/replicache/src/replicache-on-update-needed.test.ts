/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {describe, expect, test, vi} from 'vitest';
import {
  initReplicacheTesting,
  replicacheForTesting,
  tickAFewTimes,
} from './test-util.ts';

initReplicacheTesting();

describe('onUpdateNeeded', () => {
  test('Called if there is a new client group in same idb', async () => {
    const rep1 = await replicacheForTesting(
      'called-when-new-branch',
      {
        mutators: {test1: () => undefined},
      },
      undefined,
      {useUniqueName: false},
    );
    let onUpdateNeededReason;
    rep1.onUpdateNeeded = reason => {
      onUpdateNeededReason = reason;
    };

    await replicacheForTesting(
      'called-when-new-branch',
      {
        mutators: {test2: () => undefined},
      },
      undefined,
      {useUniqueName: false},
    );

    await tickAFewTimes(vi);

    expect(onUpdateNeededReason).to.deep.equal({
      type: 'NewClientGroup',
    });
  });

  test('Called if there is a new client group in different idb', async () => {
    const rep1 = await replicacheForTesting(
      'called-when-new-branch-diff-idb',
      {
        mutators: {test1: () => undefined},
        schemaVersion: '1',
      },
      undefined,
      {useUniqueName: false},
    );
    let onUpdateNeededReason;
    rep1.onUpdateNeeded = reason => {
      onUpdateNeededReason = reason;
    };
    await replicacheForTesting(
      'called-when-new-branch-diff-idb',
      {
        mutators: {test1: () => undefined},
        schemaVersion: '2',
      },
      undefined,
      {useUniqueName: false},
    );
    expect(onUpdateNeededReason).to.deep.equal({
      type: 'NewClientGroup',
    });
  });

  test('Not called if new client is in same client group', async () => {
    const rep1 = await replicacheForTesting('not-called-same-client-group', {
      mutators: {test1: () => undefined},
    });
    let onUpdateNeededReason;
    rep1.onUpdateNeeded = reason => {
      onUpdateNeededReason = reason;
    };
    await replicacheForTesting('not-called-same-client-group', {
      mutators: {test1: () => undefined},
    });
    expect(onUpdateNeededReason).to.be.undefined;
  });

  test('Not called if new client has different name', async () => {
    const rep1 = await replicacheForTesting('not-called-diff-name-1', {
      mutators: {test1: () => undefined},
    });
    let onUpdateNeededReason;
    rep1.onUpdateNeeded = reason => {
      onUpdateNeededReason = reason;
    };
    await replicacheForTesting('not-called-diff-name-2', {
      mutators: {test1: () => undefined},
    });
    expect(onUpdateNeededReason).to.be.undefined;
  });
});
