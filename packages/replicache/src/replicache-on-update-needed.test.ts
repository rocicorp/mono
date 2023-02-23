import {expect} from '@esm-bundle/chai';
import {initReplicacheTesting, replicacheForTesting} from './test-util.js';

initReplicacheTesting();

suite('onUpdateNeeded', () => {
  test('Called if there is a new client group in same idb', async () => {
    const rep1 = await replicacheForTesting(
      'called-when-new-branch',
      {
        mutators: {test1: () => undefined},
      },
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
      {useUniqueName: false},
    );

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
