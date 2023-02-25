import {test, expect} from '@jest/globals';
import {ClientRecordMap, putClientRecord} from '../types/client-record.js';
import {DurableStorage} from '../storage/durable-storage.js';
import type {NullableVersion} from 'reflect-protocol';
import {handlePull} from './pull.js';
import {clientRecord, Mocket} from '../util/test-utils.js';
import type {PullRequestBody, PullResponseBody} from 'reflect-protocol';
import {putVersion} from '../types/version.js';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

test('pull', async () => {
  type Case = {
    name: string;
    clientRecords: ClientRecordMap;
    version: NullableVersion;
    pullRequest: PullRequestBody;
    expectedPullResponse: PullResponseBody;
  };

  const cases: Case[] = [
    {
      name: 'empty server state',
      clientRecords: new Map(),
      version: null,
      pullRequest: {
        clientGroupID: 'cg1',
        cookie: 1,
        requestID: 'r1',
      },
      expectedPullResponse: {
        cookie: 0,
        lastMutationIDChanges: {},
        requestID: 'r1',
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
        clientGroupID: 'cg1',
        cookie: 1,
        requestID: 'r1',
      },
      expectedPullResponse: {
        cookie: 3,
        lastMutationIDChanges: {c1: 1, c2: 7},
        requestID: 'r1',
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
        clientGroupID: 'cg1',
        cookie: 3,
        requestID: 'r1',
      },
      expectedPullResponse: {
        cookie: 5,
        lastMutationIDChanges: {c2: 7},
        requestID: 'r1',
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
        clientGroupID: 'cg1',
        cookie: 4,
        requestID: 'r1',
      },
      expectedPullResponse: {
        cookie: 5,
        lastMutationIDChanges: {},
        requestID: 'r1',
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

    const mocket = new Mocket();
    await handlePull(storage, c.pullRequest, mocket);
    expect(mocket.log).toEqual([
      ['send', JSON.stringify(['pull', c.expectedPullResponse])],
    ]);
  }
});
