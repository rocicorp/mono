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
  appKeyDataConverter,
  appKeyPath,
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

  const APP_ID_1 = 'keys-update-test-app-id-1';
  const APP_ID_2 = 'keys-update-test-app-id-2';
  const APP_KEY_NAME_1 = 'app-key-1';
  const APP_KEY_NAME_2 = 'app-key-2';

  const appKeyPaths: [string, number | null][] = [
    [appKeyPath(APP_ID_1, APP_KEY_NAME_1), null],
    [appKeyPath(APP_ID_1, APP_KEY_NAME_2), null],
    [appKeyPath(APP_ID_2, APP_KEY_NAME_1), 99999],
    [appKeyPath(APP_ID_2, APP_KEY_NAME_2), null],
  ];

  beforeEach(async () => {
    await Promise.all(
      appKeyPaths.map(([path, lastUsed]) =>
        firestore
          .doc(path)
          .withConverter(appKeyDataConverter)
          .set({
            value: 'ignored',
            permissions: {'app:publish': true} as Permissions,
            created: FieldValue.serverTimestamp(),
            lastUsed: lastUsed ? Timestamp.fromMillis(lastUsed) : null,
          }),
      ),
    );
    mockFunctionParamsAndSecrets();
  });

  afterEach(async () => {
    // Clean up global emulator data.
    const batch = firestore.batch();
    appKeyPaths.forEach(([path]) => batch.delete(firestore.doc(path)));
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
      {appID: APP_ID_1, keyName: APP_KEY_NAME_1, lastUsed: 1234},
      {appID: APP_ID_1, keyName: APP_KEY_NAME_2, lastUsed: 2345},
      {appID: APP_ID_2, keyName: APP_KEY_NAME_1, lastUsed: 12340},
      {appID: APP_ID_2, keyName: APP_KEY_NAME_2, lastUsed: 23450},
      {appID: APP_ID_1, keyName: APP_KEY_NAME_1, lastUsed: 1245},
      {appID: APP_ID_1, keyName: APP_KEY_NAME_2, lastUsed: 2300},
      {appID: APP_ID_2, keyName: APP_KEY_NAME_1, lastUsed: 12356},
      {appID: APP_ID_2, keyName: APP_KEY_NAME_2, lastUsed: 23498},
      {appID: APP_ID_1, keyName: APP_KEY_NAME_1, lastUsed: 1200},
      {appID: APP_ID_1, keyName: APP_KEY_NAME_2, lastUsed: 2387},
      {appID: APP_ID_2, keyName: APP_KEY_NAME_1, lastUsed: 12323},
      {appID: APP_ID_2, keyName: APP_KEY_NAME_2, lastUsed: 23499},
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
          [appKeyPath(APP_ID_1, APP_KEY_NAME_1)]: 1245,
          [appKeyPath(APP_ID_1, APP_KEY_NAME_2)]: 2387,
          [appKeyPath(APP_ID_2, APP_KEY_NAME_2)]: 23499,
        },
        coalesced: 8,
      },
    });
    const expectedTimestamps = [1245, 2387, 99999, 23499];
    for (let i = 0; i < expectedTimestamps.length; i++) {
      const lastUsed = expectedTimestamps[i];
      const keyDoc = await firestore.doc(appKeyPaths[i][0]).get();
      expect(keyDoc.data()?.lastUsed.toMillis()).toBe(lastUsed);
    }
  });
});
