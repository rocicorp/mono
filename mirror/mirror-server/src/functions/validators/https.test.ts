import {test, jest, expect} from '@jest/globals';
import {https} from 'firebase-functions/v2';
import type {Request} from 'firebase-functions/v2/https';
import {baseAppRequestFields} from 'mirror-protocol/src/app.js';
import type {Auth} from 'firebase-admin/auth';
import {
  appAuthorization,
  tokenAuthentication,
  userAuthorization,
} from './auth.js';
import {validateRequest} from './schema.js';
import * as v from 'shared/src/valita.js';
import {fakeFirestore} from 'mirror-schema/src/test-helpers.js';
import {getMockReq, getMockRes} from '@jest-mock/express';
import {setUser, setApp} from 'mirror-schema/src/test-helpers.js';

const testRequestSchema = v.object({
  ...baseAppRequestFields,
  foo: v.string(),
});

test('onRequestBuilder', async () => {
  const auth = {
    verifyIdToken: jest
      .fn()
      .mockImplementation(() => Promise.resolve({uid: 'foo'})),
  };
  const firestore = fakeFirestore();
  const handler = validateRequest(testRequestSchema)
    .validate(tokenAuthentication(auth as unknown as Auth))
    .validate(userAuthorization())
    .validate(appAuthorization(firestore))
    .handle((req, ctx) => {
      const {response} = ctx;
      response.json({userID: req.requester.userID, appName: ctx.app.name});
    });
  const authenticatedFunction = https.onRequest(handler);

  await setUser(firestore, 'foo', 'foo@bar.com', 'bob', {fooTeam: 'admin'});
  await setApp(firestore, 'myApp', {teamID: 'fooTeam', name: 'MyAppName'});

  const req = getMockReq({
    body: {
      requester: {
        userID: 'foo',
        userAgent: {type: 'reflect-cli', version: '0.0.1'},
      },
      foo: 'boo',
      appID: 'myApp',
    },
    headers: {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      Authorization: 'Bearer this-is-the-encoded-token',
    },
  }) as unknown as Request;
  const {res} = getMockRes();

  await authenticatedFunction(req, res);
  expect(auth.verifyIdToken).toBeCalledWith('this-is-the-encoded-token');
  expect(res.json).toBeCalledWith({userID: 'foo', appName: 'MyAppName'});
});
