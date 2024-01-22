import {
  afterEach,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import type {DecodedIdToken} from 'firebase-admin/auth';
import {FieldValue, Timestamp} from 'firebase-admin/firestore';
import {https} from 'firebase-functions/v2';
import {
  apiKeyDataConverter,
  apiKeyPath,
  type Permissions,
} from 'mirror-schema/src/api-key.js';
import {fakeFirestore} from 'mirror-schema/src/test-helpers.js';
import {Queue} from 'shared/src/queue.js';
import type {UpdateKeyRequest, UpdateKeyResponse} from '../../keys/updates.js';
import {getMockReq, mockFunctionParamsAndSecrets} from '../../test-helpers.js';
import {
  INTERNAL_FUNCTION_HEADER,
  INTERNAL_FUNCTION_SECRET_NAME,
} from '../internal/auth.js';
import {
  FLUSH_UPDATES_TIMEOUT,
  UpdateBuffer,
  UpdateCoordinator,
  update,
} from './update.function.js';

test('UpdateBuffer', () => {
  const buf = new UpdateBuffer();
  expect(buf.timestamps).toEqual({});
  expect(buf.coalesced).toBe(0);

  buf.add('/foo/bar/baz', 123);
  expect(buf.timestamps).toEqual({
    '/foo/bar/baz': 123,
  });
  expect(buf.coalesced).toBe(0);

  buf.add('/foo/bar/baz', 120);
  expect(buf.timestamps).toEqual({
    '/foo/bar/baz': 123,
  });
  expect(buf.coalesced).toBe(1);

  buf.add('/foo/bar/bonk', 128);
  buf.add('/foo/bar/baz', 130);
  expect(buf.timestamps).toEqual({
    '/foo/bar/baz': 130,
    '/foo/bar/bonk': 128,
  });
  expect(buf.coalesced).toBe(2);
});

beforeEach(() => {
  jest.useFakeTimers();
});

afterEach(() => {
  jest.resetAllMocks();
});

test('UpdateCoordinator', async () => {
  const coordinator = new UpdateCoordinator();

  const p1 = coordinator.add('/foo/bar', 1234);

  const p2 = coordinator.add('/foo/bar', 2345);
  expect(await p1).toBe(null);

  const p3 = coordinator.add('/foo/boo', 1000);
  expect(await p2).toBe(null);

  await jest.advanceTimersByTimeAsync(FLUSH_UPDATES_TIMEOUT);
  const result = await p3;

  expect(result).toBeInstanceOf(UpdateBuffer);
  expect((result as UpdateBuffer).timestamps).toEqual({
    '/foo/bar': 2345,
    '/foo/boo': 1000,
  });
});

describe('keys-update', () => {
  // Note: The Firestore emulator does not work with jest.useFakeTimers().
  // Luckily, the Firestore logic in this function is pretty simple, so
  // the fakeFirestore suffices.
  const firestore = fakeFirestore();
  const updateFunction = https.onCall(update(firestore));

  const TEAM_ID_1 = 'keys-update-test-team-id-1';
  const TEAM_ID_2 = 'keys-update-test-team-id-2';
  const API_KEY_NAME_1 = 'api-key-1';
  const API_KEY_NAME_2 = 'api-key-2';

  const apiKeyPaths: [string, number | null][] = [
    [apiKeyPath(TEAM_ID_1, API_KEY_NAME_1), null],
    [apiKeyPath(TEAM_ID_1, API_KEY_NAME_2), null],
    [apiKeyPath(TEAM_ID_2, API_KEY_NAME_1), 99999],
    [apiKeyPath(TEAM_ID_2, API_KEY_NAME_2), null],
  ];

  beforeEach(async () => {
    await Promise.all(
      apiKeyPaths.map(([path, lastUsed]) =>
        firestore
          .doc(path)
          .withConverter(apiKeyDataConverter)
          .set({
            value: 'ignored',
            permissions: {'app:publish': true} as Permissions,
            created: FieldValue.serverTimestamp(),
            lastUsed: lastUsed ? Timestamp.fromMillis(lastUsed) : null,
            appIDs: ['ignore'],
          }),
      ),
    );
    mockFunctionParamsAndSecrets();
  });

  afterEach(async () => {
    // Clean up global emulator data.
    const batch = firestore.batch();
    apiKeyPaths.forEach(([path]) => batch.delete(firestore.doc(path)));
    await batch.commit();
  });

  function callUpdate(data: UpdateKeyRequest) {
    return updateFunction.run({
      data,
      auth: {
        uid: 'ignored',
        token: {email: 'foo@bar.com'} as DecodedIdToken,
      },
      rawRequest: getMockReq({
        headers: {
          [INTERNAL_FUNCTION_HEADER]: `default-${INTERNAL_FUNCTION_SECRET_NAME}`,
        },
      }),
    });
  }

  test('update buffering and flushing', async () => {
    const responses = new Queue<UpdateKeyResponse | Error>();

    const requests: UpdateKeyRequest[] = [
      {teamID: TEAM_ID_1, keyName: API_KEY_NAME_1, lastUsed: 1234},
      {teamID: TEAM_ID_1, keyName: API_KEY_NAME_2, lastUsed: 2345},
      {teamID: TEAM_ID_2, keyName: API_KEY_NAME_1, lastUsed: 12340},
      {teamID: TEAM_ID_2, keyName: API_KEY_NAME_2, lastUsed: 23450},
      {teamID: TEAM_ID_1, keyName: API_KEY_NAME_1, lastUsed: 1245},
      {teamID: TEAM_ID_1, keyName: API_KEY_NAME_2, lastUsed: 2300},
      {teamID: TEAM_ID_2, keyName: API_KEY_NAME_1, lastUsed: 12356},
      {teamID: TEAM_ID_2, keyName: API_KEY_NAME_2, lastUsed: 23498},
      {teamID: TEAM_ID_1, keyName: API_KEY_NAME_1, lastUsed: 1200},
      {teamID: TEAM_ID_1, keyName: API_KEY_NAME_2, lastUsed: 2387},
      {teamID: TEAM_ID_2, keyName: API_KEY_NAME_1, lastUsed: 12323},
      {teamID: TEAM_ID_2, keyName: API_KEY_NAME_2, lastUsed: 23499},
    ];

    for (const req of requests) {
      void callUpdate(req)
        .then(resp => responses.enqueue(resp))
        .catch(e => responses.enqueue(e as Error));
    }

    for (let i = 0; i < requests.length - 1; i++) {
      expect(await responses.dequeue()).toEqual({});
    }

    await jest.advanceTimersByTimeAsync(FLUSH_UPDATES_TIMEOUT);

    expect(await responses.dequeue()).toEqual({
      flushed: {
        updates: {
          [apiKeyPath(TEAM_ID_1, API_KEY_NAME_1)]: 1245,
          [apiKeyPath(TEAM_ID_1, API_KEY_NAME_2)]: 2387,
          [apiKeyPath(TEAM_ID_2, API_KEY_NAME_2)]: 23499,
        },
        coalesced: 8,
      },
    });
    const expectedTimestamps = [1245, 2387, 99999, 23499];
    for (let i = 0; i < expectedTimestamps.length; i++) {
      const lastUsed = expectedTimestamps[i];
      const keyDoc = await firestore.doc(apiKeyPaths[i][0]).get();
      expect(keyDoc.data()?.lastUsed.toMillis()).toBe(lastUsed);
    }
  });
});
