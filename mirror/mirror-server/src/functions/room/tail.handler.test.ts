import type {Firestore} from '@google-cloud/firestore';
import {getMockReq, getMockRes} from '@jest-mock/express';
import {beforeEach, describe, expect, jest, test} from '@jest/globals';
import type {Auth} from 'firebase-admin/auth';
import type {https} from 'firebase-functions/v2';
import type {TailMessage} from 'mirror-protocol/src/tail-message.js';
import {
  fakeFirestore,
  setApp,
  setProvider,
  setUser,
} from 'mirror-schema/src/test-helpers.js';
import {sleep} from 'shared/src/sleep.js';
import type WebSocket from 'ws';
import {mockFunctionParamsAndSecrets} from '../../test-helpers.js';
import {tail} from './tail.handler.js';

export class MockSocket {
  readonly url: string | URL;
  protocol: string;
  messages: string[] = [];
  closed = false;
  onUpstream?: (message: string) => void;
  onclose?: (event: WebSocket.CloseEvent) => void;
  onerror?: (event: WebSocket.ErrorEvent) => void;
  onmessage?: (event: WebSocket.MessageEvent) => void;
  constructor(url: string | URL, protocol = '') {
    this.url = url;
    this.protocol = protocol;
  }
  message(message: string) {
    this.onmessage?.({
      data: Buffer.from(message, 'utf8'),
      type: 'message',
      target: this as unknown as WebSocket,
    });
  }
  send(message: string) {
    this.messages.push(message);
    this.onUpstream?.(message);
  }

  close() {
    this.closed = true;
    const closeEvent = {
      code: 1000,
      reason: 'mock close',
      wasClean: true,
      target: this as unknown as WebSocket,
      type: 'close',
    };
    this.onclose?.(closeEvent);
  }
}

mockFunctionParamsAndSecrets();

describe('test tail', () => {
  let firestore: Firestore;
  let auth: Auth;
  let wsMock: MockSocket;
  let createTailFunction: (
    req: https.Request,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    res: any,
  ) => void | Promise<void>;
  let createTailMockPromise: Promise<void>;
  let createTailResolver: () => void;

  beforeEach(async () => {
    firestore = fakeFirestore();
    wsMock = new MockSocket('wss://example.com');

    auth = {
      verifyIdToken: jest
        .fn()
        .mockImplementation(() => Promise.resolve({uid: 'foo'})),
    } as unknown as Auth;

    createTailMockPromise = new Promise<void>(resolve => {
      createTailResolver = resolve;
    });

    const createTailMock = () => {
      setTimeout(createTailResolver, 0);
      return wsMock as unknown as WebSocket;
    };

    createTailFunction = tail(firestore, auth, createTailMock);
    await setUser(firestore, 'foo', 'foo@bar.com', 'bob', {fooTeam: 'admin'});
    await setApp(firestore, 'myApp', {
      teamID: 'fooTeam',
      name: 'MyAppName',
      provider: 'tail-test-provider',
    });
    await setProvider(firestore, 'tail-test-provider', {});
  });

  const getRequestWithHeaders = (): https.Request =>
    getMockReq({
      body: {
        requester: {
          userID: 'foo',
          userAgent: {type: 'reflect-cli', version: '0.0.1'},
        },
        appID: 'myApp',
        roomID: 'myRoom',
      },
      headers: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        Authorization: 'Bearer this-is-the-encoded-token',
      },
    }) as unknown as https.Request;

  test('valid auth in header', async () => {
    const req = getRequestWithHeaders();

    const {res} = getMockRes();
    req.res = res;
    const createTailPromise = createTailFunction(req, res);
    await createTailMockPromise;
    wsMock.close();
    await createTailPromise;
    expect(auth.verifyIdToken).toBeCalledWith('this-is-the-encoded-token');
  });

  test('handle message', async () => {
    const req = getRequestWithHeaders();

    const {res} = getMockRes();
    req.res = res;
    const createTailPromise = createTailFunction(req, res);
    await createTailMockPromise;
    wsMock.message(JSON.stringify({type: 'connected'}));
    wsMock.message(
      JSON.stringify({
        type: 'log',
        level: 'info',
        message: ['info message', 'one'],
      }),
    );
    wsMock.message(
      JSON.stringify({
        type: 'log',
        level: 'debug',
        message: ['debug message', 123, true],
      }),
    );
    await sleep(1);
    expect(res.write).toBeCalledTimes(3);
    expect(req.res.write).toHaveBeenNthCalledWith(
      1,
      'data: {"type":"connected"}\n\n',
    );
    expect(req.res.write).toHaveBeenNthCalledWith(
      2,
      'data: {"type":"log","level":"info","message":["info message","one"]}\n\n',
    );
    expect(req.res.write).toHaveBeenNthCalledWith(
      3,
      'data: {"type":"log","level":"debug","message":["debug message",123,true]}\n\n',
    );

    wsMock.close();
    await createTailPromise;
    expect(auth.verifyIdToken).toBeCalledWith('this-is-the-encoded-token');
  });

  async function testForwardMessage(
    m: TailMessage,
    expected: string,
    expectsError?: boolean,
  ) {
    const req = getRequestWithHeaders();

    const {res} = getMockRes();
    req.res = res;
    const createTailPromise = createTailFunction(req, res);
    await createTailMockPromise;
    wsMock.message(JSON.stringify(m));
    await sleep(1);
    if (expectsError) {
      expect(res.write).toBeCalledTimes(2);
      expect(req.res.write).toHaveBeenNthCalledWith(1, 'event: error\n');
      expect(req.res.write).toHaveBeenNthCalledWith(2, expected);
    } else {
      expect(res.write).toBeCalledTimes(1);
      expect(req.res.write).toHaveBeenNthCalledWith(1, expected);
    }

    wsMock.close();
    await createTailPromise;
    expect(auth.verifyIdToken).toBeCalledWith('this-is-the-encoded-token');
  }

  test('Invalid auth in header', async () => {
    const m: TailMessage = {
      type: 'error',
      kind: 'Unauthorized',
      message: 'missing x',
    };
    await testForwardMessage(
      m,
      'data: {"type":"error","kind":"Unauthorized","message":"missing x"}\n\n',
    );
  });

  test('No such room', async () => {
    await testForwardMessage(
      {type: 'error', kind: 'RoomNotFound', message: 'no such room'},
      'data: {"type":"error","kind":"RoomNotFound","message":"no such room"}\n\n',
    );
  });

  test('InvalidConnectionRequest', async () => {
    await testForwardMessage(
      {
        type: 'error',
        kind: 'InvalidConnectionRequest',
        message: 'missing roomID',
      },
      'data: {"type":"error","kind":"InvalidConnectionRequest","message":"missing roomID"}\n\n',
    );
  });

  test('Invalid type', async () => {
    await testForwardMessage(
      42 as unknown as TailMessage,
      'data: Expected object. Got 42\n\n',
      true,
    );
  });
});
