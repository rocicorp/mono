import {test, expect} from '@jest/globals';
import {ClientRecordMap, putClientRecord} from '../types/client-record.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {NullableVersion, putVersion} from '../types/version.js';
import {handlePull} from './pull.js';
import type {PullRequest, PullResponse} from '../protocol/pull.js';
import {clientRecord} from '../util/test-utils.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

test('pull', async () => {
  type Case = {
    name: string;
    clientRecords: ClientRecordMap;
    version: NullableVersion;
    pullRequest: PullRequest;
    expectedPullResponse: PullResponse;
  };

  const cases: Case[] = [
    {
      name: 'empty server state',
      clientRecords: new Map(),
      version: null,
      pullRequest: {
        profileID: 'p1',
        clientGroupID: 'cg1',
        cookie: 1,
        pullVersion: 1,
        schemaVersion: '',
      },
      expectedPullResponse: {
        cookie: 0,
        lastMutationIDChanges: {},
        patch: [],
      },
    },
    {
      name: 'pull returns mutation id changes for specified clientGroupID and no others',
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1, 1, 2)],
        ['c2', clientRecord('cg1', 1, 7, 2)],
        ['c4', clientRecord('cg2', 1, 7, 2)],
      ]),
      version: 3,
      pullRequest: {
        profileID: 'p1',
        clientGroupID: 'cg1',
        cookie: 1,
        pullVersion: 1,
        schemaVersion: '',
      },
      expectedPullResponse: {
        cookie: 3,
        lastMutationIDChanges: {c1: 1, c2: 7},
        patch: [],
      },
    },
    {
      name: 'pull only returns lastMutationID if it has changed since cookie, one change',
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1, 1, 2)],
        ['c2', clientRecord('cg1', 1, 7, 4)],
      ]),
      version: 5,
      pullRequest: {
        profileID: 'p1',
        clientGroupID: 'cg1',
        cookie: 3,
        pullVersion: 1,
        schemaVersion: '',
      },
      expectedPullResponse: {
        cookie: 5,
        lastMutationIDChanges: {c2: 7},
        patch: [],
      },
    },

    {
      name: 'pull only returns lastMutationID if it has changed since cookie, no changes',
      clientRecords: new Map([
        ['c1', clientRecord('cg1', 1, 1, 2)],
        ['c2', clientRecord('cg1', 1, 7, 4)],
      ]),
      version: 5,
      pullRequest: {
        profileID: 'p1',
        clientGroupID: 'cg1',
        cookie: 4,
        pullVersion: 1,
        schemaVersion: '',
      },
      expectedPullResponse: {
        cookie: 5,
        lastMutationIDChanges: {},
        patch: [],
      },
    },
  ];

  const durable = await getMiniflareDurableObjectStorage(id);

  for (const c of cases) {
    await durable.deleteAll();
    const storage = new DurableStorage(durable);
    for (const [clientID, clientRecord] of c.clientRecords) {
      await putClientRecord(clientID, clientRecord, storage);
    }
    if (c.version !== null) {
      await putVersion(c.version, storage);
    }

    const pullResponse = await handlePull(storage, c.pullRequest);

    expect(pullResponse).toEqual(c.expectedPullResponse);
  }
});
