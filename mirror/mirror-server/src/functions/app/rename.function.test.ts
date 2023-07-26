import {expect, test} from '@jest/globals';
import {
  fakeFirestore,
  setUser,
  setApp,
  setAppName,
  getApp,
  getAppName,
} from 'mirror-schema/src/test-helpers.js';
import {https} from 'firebase-functions/v2';
import {
  FunctionsErrorCode,
  HttpsError,
  Request,
} from 'firebase-functions/v2/https';
import type {AuthData} from 'firebase-functions/v2/tasks';
import {rename} from './rename.function.js';
import type {RenameAppRequest} from 'mirror-protocol/src/app.js';
import {appNameIndexPath} from 'mirror-schema/src/app.js';

type Case = {
  name: string;
  newName: string;
  extraDocs?: {[path: string]: object};
  error?: FunctionsErrorCode;
};
const cases: Case[] = [
  {
    name: 'alphanumeric',
    newName: 'valid0name0',
  },
  {
    name: 'alphanumeric with hyphens',
    newName: 'this-is-1-valid-name0',
  },
  {
    name: 'same as old name',
    newName: 'old-name',
  },
  {
    name: 'cannot be uppercase',
    newName: 'NotAValidName',
    error: 'invalid-argument',
  },
  {
    name: 'cannot start with digit',
    newName: '0is-not-allowed',
    error: 'invalid-argument',
  },
  {
    name: 'cannot end with hyphen',
    newName: 'cannot-end-with-hyphen-',
    error: 'invalid-argument',
  },
  {
    name: 'new name in use',
    newName: 'taken-name',
    extraDocs: {[appNameIndexPath('taken-name')]: {appID: 'otherApp'}},
    error: 'already-exists',
  },
  {
    name: 'corrupt name index doc',
    newName: 'valid-name',
    extraDocs: {[appNameIndexPath('old-name')]: {appID: 'otherApp'}},
    error: 'internal',
  },
];

for (const c of cases) {
  test(c.name, async () => {
    const firestore = fakeFirestore();
    await setUser(firestore, 'fooUser', 'f@b.com', '', {barTeam: 'admin'});
    await setApp(firestore, 'barApp', {teamID: 'barTeam', name: 'old-name'});
    await setAppName(firestore, 'barApp', 'old-name');
    for (const [path, data] of Object.entries(c.extraDocs ?? {})) {
      await firestore.doc(path).set(data);
    }

    const renameFunction = https.onCall(rename(firestore));
    const request: RenameAppRequest = {
      requester: {
        userAgent: {
          type: 'reflect-cli',
          version: '0.0.1',
        },
        userID: 'fooUser',
      },
      appID: 'barApp',
      name: c.newName,
    };

    let error: HttpsError | undefined = undefined;
    try {
      await renameFunction.run({
        auth: {uid: 'fooUser'} as AuthData,
        data: request,
        rawRequest: null as unknown as Request,
      });
    } catch (e) {
      expect(e).toBeInstanceOf(HttpsError);
      error = e as HttpsError;
    }

    expect(error?.code).toBe(c.error);

    if (!c.error) {
      const app = await getApp(firestore, 'barApp');
      expect(app.name).toBe(c.newName);
      const appName = await getAppName(firestore, c.newName);
      expect(appName.appID).toBe('barApp');

      const oldAppName = await firestore
        .doc(appNameIndexPath('old-name'))
        .get();
      expect(oldAppName.data()).toBeUndefined;
    }
  });
}
