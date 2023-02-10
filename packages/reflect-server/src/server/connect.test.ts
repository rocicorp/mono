import { test, expect } from "@jest/globals";
import {
  ClientRecord,
  clientRecordKey,
  clientRecordSchema,
} from "../../src/types/client-record.js";
import { getEntry, putEntry } from "../../src/db/data.js";
import type { ClientMap, Socket } from "../../src/types/client-state.js";
import {
  client,
  clientRecord,
  createSilentLogContext,
  Mocket,
} from "../util/test-utils.js";
import { handleConnection } from "../../src/server/connect.js";
import { USER_DATA_HEADER_NAME } from "./auth.js";
import { encodeHeaderValue } from "../util/headers.js";
import { DurableStorage } from "../storage/durable-storage.js";

const { roomDO } = getMiniflareBindings();
const id = roomDO.newUniqueId();

function freshClient(
  id: string,
  userID: string,
  socket: Socket = new Mocket()
) {
  const [clientID, c] = client(id, userID, socket);
  c.clockBehindByMs = undefined;
  return [clientID, c] as const;
}

function createHeadersWithValidUserData(userID: string) {
  const headers = new Headers();
  headers.set(
    USER_DATA_HEADER_NAME,
    encodeHeaderValue(JSON.stringify({ userID }))
  );
  return headers;
}

function createHeadersWithInvalidUserData() {
  const headers = new Headers();
  headers.set(USER_DATA_HEADER_NAME, "invalid");
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
    encodeHeaderValue(JSON.stringify({ userID: "" }))
  );
  return headers;
}

test("handleConnection", async () => {
  type Case = {
    name: string;
    url: string;
    headers: Headers;
    expectErrorResponse?: string;
    existingRecord?: ClientRecord;
    expectedRecord?: ClientRecord;
    existingClients: ClientMap;
    expectedClients: (socket: Socket) => ClientMap;
    socket?: Socket;
  };
  const c2 = client("c2", "u2");
  const cases: Case[] = [
    {
      name: "invalid clientid",
      url: "http://google.com/?baseCookie=1&timestamp=t1&lmid=0",
      headers: createHeadersWithValidUserData("u1"),
      expectErrorResponse: "Error: invalid querystring - missing clientID",
      existingClients: new Map(),
      expectedClients: () => new Map(),
    },
    {
      name: "invalid timestamp",
      url: "http://google.com/?clientID=c1&baseCookie=1&lmid=0",
      headers: createHeadersWithValidUserData("u1"),
      expectErrorResponse: "Error: invalid querystring - missing ts",
      existingClients: new Map(),
      expectedClients: () => new Map(),
    },
    {
      name: "invalid (non-numeric) timestamp",
      url: "http://google.com/?clientID=c1&baseCookie=1&ts=xx&lmid=0",
      headers: createHeadersWithValidUserData("u1"),
      expectErrorResponse:
        "Error: invalid querystring parameter ts, url: http://google.com/?clientID=c1&baseCookie=1&ts=xx&lmid=0, got: xx",
      existingClients: new Map(),
      expectedClients: () => new Map(),
    },
    {
      name: "missing lmid",
      url: "http://google.com/?clientID=c1&baseCookie=1&ts=123",
      headers: createHeadersWithValidUserData("u1"),
      expectErrorResponse: "Error: invalid querystring - missing lmid",
      existingClients: new Map(),
      expectedClients: () => new Map(),
    },
    {
      name: "inmvalid (non-numeric) lmid",
      url: "http://google.com/?clientID=c1&baseCookie=1&ts=123&lmid=xx",
      headers: createHeadersWithValidUserData("u1"),
      expectErrorResponse:
        "Error: invalid querystring parameter lmid, url: http://google.com/?clientID=c1&baseCookie=1&ts=123&lmid=xx, got: xx",
      existingClients: new Map(),
      expectedClients: () => new Map(),
    },
    {
      name: "no existing clients",
      url: "http://google.com/?clientID=c1&baseCookie=1&ts=42&lmid=0",
      headers: createHeadersWithValidUserData("u1"),
      existingClients: new Map(),
      expectedClients: (socket) => new Map([freshClient("c1", "u1", socket)]),
      expectedRecord: clientRecord(1, 0),
    },
    {
      name: "baseCookie: null",
      url: "http://google.com/?clientID=c1&baseCookie=&ts=42&lmid=0",
      headers: createHeadersWithValidUserData("u1"),
      existingClients: new Map(),
      expectedClients: (socket) => new Map([freshClient("c1", "u1", socket)]),
      expectedRecord: clientRecord(null, 0),
    },
    {
      name: "existing clients",
      url: "http://google.com/?clientID=c1&baseCookie=1&ts=42&lmid=0",
      headers: createHeadersWithValidUserData("u1"),
      existingClients: new Map([c2]),
      expectedClients: (socket) =>
        new Map([freshClient("c1", "u1", socket), c2]),
      expectedRecord: clientRecord(1, 0),
    },
    {
      name: "existing record",
      url: "http://google.com/?clientID=c1&baseCookie=7&ts=42&lmid=0",
      headers: createHeadersWithValidUserData("u1"),
      existingClients: new Map(),
      expectedClients: (socket) => new Map([freshClient("c1", "u1", socket)]),
      existingRecord: clientRecord(1, 42),
      expectedRecord: clientRecord(7, 42),
    },
    {
      name: "missing user data",
      url: "http://google.com/?clientID=c1&baseCookie=7&ts=42&lmid=0",
      headers: new Headers(),
      expectErrorResponse: "Error: missing user-data",
      existingClients: new Map(),
      expectedClients: () => new Map(),
    },
    {
      name: "invalid user data",
      url: "http://google.com/?clientID=c1&baseCookie=7&ts=42&lmid=0",
      headers: createHeadersWithInvalidUserData(),
      expectErrorResponse: "Error: invalid user-data - failed to decode/parse",
      existingClients: new Map(),
      expectedClients: () => new Map(),
    },
    {
      name: "user data missing userID",
      url: "http://google.com/?clientID=c1&baseCookie=7&ts=42&lmid=0",
      headers: createHeadersWithUserDataMissingUserID(),
      expectErrorResponse: "Error: invalid user-data - missing userID",
      existingClients: new Map(),
      expectedClients: () => new Map(),
    },
    {
      name: "user data with empty userID",
      url: "http://google.com/?clientID=c1&baseCookie=7&ts=42&lmid=0",
      headers: createHeadersWithUserDataWithEmptyUserID(),
      expectErrorResponse: "Error: invalid user-data - missing userID",
      existingClients: new Map(),
      expectedClients: () => new Map(),
    },
    {
      name: "Invalid lastMutationID",
      url: "http://google.com/?clientID=c1&baseCookie=7&ts=42&lmid=100",
      existingClients: new Map(),
      expectedClients: (socket) => new Map([freshClient("c1", "u1", socket)]),
      headers: createHeadersWithValidUserData("u1"),
      expectErrorResponse: "Unexpected lmid",
    },
  ];

  const durable = await getMiniflareDurableObjectStorage(id);
  const storage = new DurableStorage(durable);

  for (const c of cases) {
    if (c.existingRecord) {
      await putEntry(durable, clientRecordKey("c1"), c.existingRecord, {});
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
      onClose
    );

    if (c.expectErrorResponse) {
      expect(mocket.log).toEqual([
        ["send", JSON.stringify(["error", c.expectErrorResponse])],
        ["close"],
      ]);
      continue;
    }
    expect(mocket.log).toEqual([["send", JSON.stringify(["connected", {}])]]);

    const expectedClients = c.expectedClients(mocket);
    expect(clients).toEqual(expectedClients);

    const actualRecord = await getEntry(
      durable,
      clientRecordKey("c1"),
      clientRecordSchema,
      {}
    );
    expect(actualRecord).toEqual(c.expectedRecord);
  }
});
