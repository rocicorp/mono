import {
  test,
  describe,
  expect,
  beforeEach,
  jest,
  afterEach,
} from '@jest/globals';
import {
  ClientRecord,
  clientRecordKey,
  clientRecordSchema,
} from '../types/client-record.js';
import {getEntry, putEntry} from '../db/data.js';
import type {
  ClientGroupID,
  ClientID,
  ClientMap,
  Socket,
} from '../types/client-state.js';
import {
  client,
  clientRecord,
  createSilentLogContext,
  Mocket,
} from '../util/test-utils.js';
import {getConnectRequest, handleConnection} from '../server/connect.js';
import {USER_DATA_HEADER_NAME} from './auth.js';
import {encodeHeaderValue} from '../util/headers.js';
import {DurableStorage} from '../storage/durable-storage.js';
import {putVersion} from '../types/version.js';
import type {NullableVersion} from 'reflect-protocol';
import type {ErrorKind} from 'reflect-protocol';

const {roomDO} = getMiniflareBindings();
const id = roomDO.newUniqueId();

const START_TIME = 1683000000000;
beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(START_TIME);
});

afterEach(() => {
  jest.restoreAllMocks();
});

function freshClient(
  id: ClientID,
  userID: string,
  clientGroupID: ClientGroupID,
  socket: Socket,
  debugPerf = false,
  lastActivityTimestamp = START_TIME,
) {
  const [clientID, c] = client(
    id,
    userID,
    clientGroupID,
    socket,
    undefined,
    debugPerf,
    lastActivityTimestamp,
  );
  c.clockOffsetMs = undefined;
  return [clientID, c] as const;
}

function createHeadersWithValidUserData(userID: string) {
  const headers = new Headers();
  headers.set(
    USER_DATA_HEADER_NAME,
    encodeHeaderValue(JSON.stringify({userID})),
  );
  return headers;
}

function createHeadersWithInvalidUserData() {
  const headers = new Headers();
  headers.set(USER_DATA_HEADER_NAME, 'invalid');
  return headers;
}

function createHeadersWithUserDataMissingUserID() {
  const headers = new Headers();
  headers.set(USER_DATA_HEADER_NAME, encodeHeaderValue(JSON.stringify({})));
  return headers;
}

function createHeadersWithUserDataWithEmptyUserID() {
  const headers = new Headers();
  headers.set(
    USER_DATA_HEADER_NAME,
    encodeHeaderValue(JSON.stringify({userID: ''})),
  );
  return headers;
}

describe('handleConnection', () => {
  type Case = {
    name: string;
    url: string;
    headers: Headers;
    expectErrorKind?: ErrorKind;
    expectErrorMessage?: string;
    existingRecord?: ClientRecord;
    expectedRecord?: ClientRecord;
    existingClients: ClientMap;
    expectedClients: (socket: Socket) => ClientMap;
    socket?: Socket;
    version: NullableVersion;
    wsid?: string | undefined;
  };
  const c2 = client('c2', 'u2', 'cg1');
  const cases: Case[] = [
    {
      name: 'invalid clientid',
      url: 'http://google.com/?clientGroupID=cg1&baseCookie=1&timestamp=t1&lmid=0&wsid=wsidx',
      headers: createHeadersWithValidUserData('u1'),
      expectErrorKind: 'InvalidConnectionRequest',
      expectErrorMessage: 'invalid querystring - missing clientID',
      existingClients: new Map(),
      expectedClients: () => new Map(),
      version: 1,
    },
    {
      name: 'invalid clientGroupID',
      url: 'http://google.com/?clientID=c1&baseCookie=1&timestamp=t1&lmid=0',
      headers: createHeadersWithValidUserData('u1'),
      expectErrorKind: 'InvalidConnectionRequest',
      expectErrorMessage: 'invalid querystring - missing clientGroupID',
      existingClients: new Map(),
      expectedClients: () => new Map(),
      version: 1,
    },
    {
      name: 'invalid timestamp',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=1&lmid=0&wsid=wsidx',
      headers: createHeadersWithValidUserData('u1'),
      expectErrorKind: 'InvalidConnectionRequest',
      expectErrorMessage: 'invalid querystring - missing ts',
      existingClients: new Map(),
      expectedClients: () => new Map(),
      version: 1,
    },
    {
      name: 'invalid (non-numeric) timestamp',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=1&ts=xx&lmid=0&wsid=wsidx',
      headers: createHeadersWithValidUserData('u1'),
      expectErrorKind: 'InvalidConnectionRequest',
      expectErrorMessage:
        'invalid querystring parameter ts, got: xx, url: http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=1&ts=xx&lmid=0&wsid=wsidx',
      existingClients: new Map(),
      expectedClients: () => new Map(),
      version: 1,
    },
    {
      name: 'missing lmid',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=1&ts=123',
      headers: createHeadersWithValidUserData('u1'),
      expectErrorKind: 'InvalidConnectionRequest',
      expectErrorMessage: 'invalid querystring - missing lmid',
      existingClients: new Map(),
      expectedClients: () => new Map(),
      version: 1,
    },
    {
      name: 'invalid (non-numeric) lmid',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=1&ts=123&lmid=xx',
      headers: createHeadersWithValidUserData('u1'),
      expectErrorKind: 'InvalidConnectionRequest',
      expectErrorMessage:
        'invalid querystring parameter lmid, got: xx, url: http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=1&ts=123&lmid=xx',
      existingClients: new Map(),
      expectedClients: () => new Map(),
      version: 1,
    },
    {
      name: 'baseCookie: null and version: null',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=&ts=42&lmid=0&wsid=wsidx',
      headers: createHeadersWithValidUserData('u1'),
      existingClients: new Map(),
      expectedClients: socket =>
        new Map([freshClient('c1', 'u1', 'cg1', socket)]),
      existingRecord: clientRecord('cg1', null, 0),
      expectedRecord: clientRecord('cg1', null, 0),
      version: null,
    },
    {
      name: 'baseCookie: 1 and version null',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=1&ts=42&lmid=0&wsid=wsidx',
      headers: createHeadersWithValidUserData('u1'),
      expectErrorKind: 'InvalidConnectionRequestBaseCookie',
      expectErrorMessage: `Unexpected baseCookie.`,
      existingClients: new Map(),
      expectedClients: () => new Map(),
      version: null,
    },
    {
      name: 'baseCookie: 2 and version: 1',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=2&ts=42&lmid=0&wsid=wsidx',
      headers: createHeadersWithValidUserData('u1'),
      expectErrorKind: 'InvalidConnectionRequestBaseCookie',
      expectErrorMessage: `Unexpected baseCookie.`,
      existingClients: new Map(),
      expectedClients: () => new Map(),
      version: null,
    },
    {
      name: 'baseCookie: 1 and version: 2',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=1&ts=42&lmid=0&wsid=wsidx',
      headers: createHeadersWithValidUserData('u1'),
      existingClients: new Map(),
      expectedClients: socket =>
        new Map([freshClient('c1', 'u1', 'cg1', socket, false)]),
      existingRecord: clientRecord('cg1', 2, 0),
      expectedRecord: clientRecord('cg1', 1, 0),
      version: 2,
    },
    {
      name: 'baseCookie: 1 and version: 2, debugPerf',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=1&ts=42&lmid=0&wsid=wsidx&debugPerf=true',
      headers: createHeadersWithValidUserData('u1'),
      existingClients: new Map(),
      expectedClients: socket =>
        new Map([freshClient('c1', 'u1', 'cg1', socket, true)]),
      existingRecord: clientRecord('cg1', 2, 0),
      expectedRecord: clientRecord('cg1', 1, 0),
      version: 2,
    },
    {
      name: 'baseCookie: null w/existing clients',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=&ts=42&lmid=0&wsid=wsidx',
      headers: createHeadersWithValidUserData('u1'),
      existingClients: new Map([c2]),
      expectedClients: socket =>
        new Map([freshClient('c1', 'u1', 'cg1', socket), c2]),
      expectedRecord: clientRecord('cg1', null, 0, null),
      version: 1,
    },
    {
      name: 'existing record',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=7&ts=42&lmid=0&wsid=wsidx',
      headers: createHeadersWithValidUserData('u1'),
      existingClients: new Map(),
      expectedClients: socket =>
        new Map([freshClient('c1', 'u1', 'cg1', socket)]),
      existingRecord: clientRecord('cg1', 4, 0),
      expectedRecord: clientRecord('cg1', 7, 0),
      version: 7,
    },
    {
      name: 'missing wsid',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=7&ts=123&lmid=0',
      headers: createHeadersWithValidUserData('u1'),
      existingClients: new Map(),
      expectedClients: socket =>
        new Map([freshClient('c1', 'u1', 'cg1', socket)]),
      existingRecord: clientRecord('cg1', 4, 0),
      expectedRecord: clientRecord('cg1', 7, 0),
      version: 7,
      wsid: '',
    },
    {
      name: 'missing user data',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=7&ts=42&lmid=0&wsid=wsidx',
      headers: new Headers(),
      expectErrorKind: 'InvalidConnectionRequest',
      expectErrorMessage: 'missing user-data',
      existingClients: new Map(),
      expectedClients: () => new Map(),
      version: 10,
    },
    {
      name: 'invalid user data',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=7&ts=42&lmid=0&wsid=wsidx',
      headers: createHeadersWithInvalidUserData(),
      expectErrorKind: 'InvalidConnectionRequest',
      expectErrorMessage: 'invalid user-data - failed to decode/parse',
      existingClients: new Map(),
      expectedClients: () => new Map(),
      version: 7,
    },
    {
      name: 'user data missing userID',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=7&ts=42&lmid=0&wsid=wsidx',
      headers: createHeadersWithUserDataMissingUserID(),
      expectErrorKind: 'InvalidConnectionRequest',
      expectErrorMessage: 'invalid user-data - missing userID',
      existingClients: new Map(),
      expectedClients: () => new Map(),
      version: 7,
    },
    {
      name: 'user data with empty userID',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=7&ts=42&lmid=0&wsid=wsidx',
      headers: createHeadersWithUserDataWithEmptyUserID(),
      expectErrorKind: 'InvalidConnectionRequest',
      expectErrorMessage: 'invalid user-data - missing userID',
      existingClients: new Map(),
      expectedClients: () => new Map(),
      version: 7,
    },
    {
      name: 'Invalid lastMutationID',
      url: 'http://google.com/?clientID=c1&clientGroupID=cg1&baseCookie=7&ts=42&lmid=100&wsid=wsidx',
      existingClients: new Map(),
      expectedClients: socket =>
        new Map([freshClient('c1', 'u1', 'cg1', socket)]),
      headers: createHeadersWithValidUserData('u1'),
      existingRecord: clientRecord('cg1', 7, 0),
      expectErrorKind: 'InvalidConnectionRequestLastMutationID',
      expectErrorMessage: `Unexpected lmid.`,
      version: 7,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const durable = await getMiniflareDurableObjectStorage(id);
      const storage = new DurableStorage(durable);
      const {wsid = 'wsidx'} = c;

      await durable.deleteAll();
      if (c.existingRecord) {
        await putEntry(durable, clientRecordKey('c1'), c.existingRecord, {});
      }

      if (c.version !== null) {
        await putVersion(c.version, storage);
      }

      const onMessage = () => undefined;
      const onClose = () => undefined;
      const mocket = new Mocket();
      const clients = c.existingClients;

      await handleConnection(
        createSilentLogContext(),
        mocket,
        storage,
        new URL(c.url),
        c.headers,
        clients,
        onMessage,
        onClose,
      );

      if (c.expectErrorMessage) {
        expect(mocket.log).toEqual([
          [
            'send',
            JSON.stringify(['error', c.expectErrorKind, c.expectErrorMessage]),
          ],
          ['close'],
        ]);
        return;
      }

      try {
        expect(mocket.log).toEqual([
          ['send', JSON.stringify(['connected', {wsid}])],
        ]);
        const expectedClients = c.expectedClients(mocket);
        expect(clients).toEqual(expectedClients);

        const actualRecord = await getEntry(
          durable,
          clientRecordKey('c1'),
          clientRecordSchema,
          {},
        );
        expect(actualRecord).toEqual(c.expectedRecord);
      } catch (e) {
        console.log('c.name failed:', c.name);
        throw e;
      }
    });
  }
});

test('getConnectRequest', () => {
  const testError = (
    url: string,
    expectedError: string,
    headers: Headers = new Headers(),
  ) => {
    expect(getConnectRequest(new URL(url), headers)).toEqual({
      error: expectedError,
      result: null,
    });
  };

  const testResult = (
    url: string,
    headers: Headers,
    expectedResult: unknown,
  ) => {
    expect(getConnectRequest(new URL(url), headers)).toEqual({
      error: null,
      result: expectedResult,
    });
  };

  testError(
    'https://www.example.com',
    'invalid querystring - missing clientID',
  );
  testError(
    'https://www.example.com/?clientID=123',
    'invalid querystring - missing clientGroupID',
  );
  testError(
    'https://www.example.com/?clientID=123&clientGroupID=cg1',
    'invalid querystring - missing ts',
  );

  let url = 'https://www.example.com/?clientID=123&clientGroupID=cg1&ts=abc';
  testError(url, `invalid querystring parameter ts, got: abc, url: ${url}`);
  testError(
    'https://www.example.com/?clientID=123&clientGroupID=cg1&ts=123',
    'invalid querystring - missing lmid',
  );
  testError(
    'https://www.example.com/?clientID=123&clientGroupID=cg1&ts=123&lmid=456&wsid=wsidx',
    'missing user-data',
  );
  url =
    'https://www.example.com/?clientID=123&clientGroupID=cg1&ts=123&lmid=456&baseCookie=abc';
  testError(
    url,
    `invalid querystring parameter baseCookie, got: abc, url: ${url}`,
  );
  testError(
    'https://www.example.com/?clientID=123&clientGroupID=cg1&ts=123&lmid=456&wsid=wsidx',
    'invalid user-data - failed to decode/parse',
    new Headers([[USER_DATA_HEADER_NAME, 'abc']]),
  );
  testError(
    'https://www.example.com/?clientID=123&clientGroupID=cg1&ts=123&lmid=456&wsid=wsidx',
    'invalid user-data - missing userID',
    new Headers([[USER_DATA_HEADER_NAME, '42']]),
  );
  testError(
    'https://www.example.com/?clientID=123&clientGroupID=cg1&ts=123&lmid=456&wsid=wsidx',
    'invalid user-data - missing userID',
    new Headers([[USER_DATA_HEADER_NAME, '{"userID":null}']]),
  );
  testError(
    'https://www.example.com/?clientID=123&clientGroupID=cg1&ts=123&lmid=456&wsid=wsidx',
    'invalid user-data - failed to decode/parse',
    new Headers([[USER_DATA_HEADER_NAME, '{"userID":"u1}']]),
  );

  testResult(
    'https://www.example.com/?clientID=cid1&clientGroupID=cg1&ts=123&lmid=456&wsid=wsidx1',
    new Headers([[USER_DATA_HEADER_NAME, '{"userID":"u1","more":"data"}']]),
    {
      clientID: 'cid1',
      clientGroupID: 'cg1',
      userData: {userID: 'u1', more: 'data'},
      timestamp: 123,
      lmid: 456,
      baseCookie: null,
      wsid: 'wsidx1',
      debugPerf: false,
    },
  );
  testResult(
    'https://www.example.com/?clientID=cid1&clientGroupID=cg1&ts=123&lmid=456&baseCookie=789&wsid=wsidx2',
    new Headers([[USER_DATA_HEADER_NAME, '{"userID":"u1","more":"data"}']]),
    {
      clientID: 'cid1',
      clientGroupID: 'cg1',
      userData: {userID: 'u1', more: 'data'},
      timestamp: 123,
      lmid: 456,
      baseCookie: 789,
      wsid: 'wsidx2',
      debugPerf: false,
    },
  );
  testResult(
    'https://www.example.com/?clientID=cid1&clientGroupID=cg1&ts=123&lmid=456&baseCookie=789&wsid=wsidx2&debugPerf=true',
    new Headers([[USER_DATA_HEADER_NAME, '{"userID":"u1","more":"data"}']]),
    {
      clientID: 'cid1',
      clientGroupID: 'cg1',
      userData: {userID: 'u1', more: 'data'},
      timestamp: 123,
      lmid: 456,
      baseCookie: 789,
      wsid: 'wsidx2',
      debugPerf: true,
    },
  );
});
