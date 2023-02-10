import {
  ClientRecord,
  clientRecordKey,
  clientRecordSchema,
} from "../../src/types/client-record";
import { getEntry, putEntry } from "../../src/db/data";
import { ClientMap, Socket } from "../../src/types/client-state";
import { client, clientRecord, Mocket } from "../util/test-utils";
import { handleConnection } from "../../src/server/connect";
import { LogContext } from "../../src/util/logger";

const { server } = getMiniflareBindings();
const id = server.newUniqueId();

function freshClient(id: string, socket: Socket = new Mocket()) {
  const [clientID, c] = client(id, socket);
  c.clockBehindByMs = undefined;
  return [clientID, c] as const;
}

test("handleConnection", async () => {
  type Case = {
    name: string;
    url: string;
    expectErrorResponse?: string;
    existingRecord?: ClientRecord;
    expectedRecord?: ClientRecord;
    existingClients: ClientMap;
    expectedClients: (socket: Socket) => ClientMap;
    socket?: Socket;
  };
  const c2 = client("c2");
  const cases: Case[] = [
    {
      name: "invalid clientid",
      url: "http://google.com/?baseCookie=1&timestamp=t1",
      expectErrorResponse: "Error: invalid querystring - missing clientID",
      existingClients: new Map(),
      expectedClients: (_) => new Map(),
    },
    {
      name: "invalid timestamp",
      url: "http://google.com/?clientID=c1&baseCookie=1",
      expectErrorResponse: "Error: invalid querystring - missing ts",
      existingClients: new Map(),
      expectedClients: (_) => new Map(),
    },
    {
      name: "invalid (non-numeric) timestamp",
      url: "http://google.com/?clientID=c1&baseCookie=1&ts=xx",
      expectErrorResponse:
        "Error: invalid querystring parameter ts, url: http://google.com/?clientID=c1&baseCookie=1&ts=xx, got: xx",
      existingClients: new Map(),
      expectedClients: (_) => new Map(),
    },
    {
      name: "no existing clients",
      url: "http://google.com/?clientID=c1&baseCookie=1&ts=42",
      existingClients: new Map(),
      expectedClients: (socket) => new Map([freshClient("c1", socket)]),
      expectedRecord: clientRecord(1, 0),
    },
    {
      name: "baseCookie: null",
      url: "http://google.com/?clientID=c1&baseCookie=&ts=42",
      existingClients: new Map(),
      expectedClients: (socket) => new Map([freshClient("c1", socket)]),
      expectedRecord: clientRecord(null, 0),
    },
    {
      name: "existing clients",
      url: "http://google.com/?clientID=c1&baseCookie=1&ts=42",
      existingClients: new Map([c2]),
      expectedClients: (socket) => new Map([freshClient("c1", socket), c2]),
      expectedRecord: clientRecord(1, 0),
    },
    {
      name: "existing record",
      url: "http://google.com/?clientID=c1&baseCookie=7&ts=42",
      existingClients: new Map(),
      expectedClients: (socket) => new Map([freshClient("c1", socket)]),
      existingRecord: clientRecord(1, 88),
      expectedRecord: clientRecord(7, 88),
    },
  ];

  const durable = await getMiniflareDurableObjectStorage(id);

  for (const c of cases) {
    if (c.existingRecord) {
      await putEntry(durable, clientRecordKey("c1"), c.existingRecord);
    }

    const onMessage = () => undefined;
    const onClose = () => undefined;
    const mocket = new Mocket();

    await handleConnection(
      new LogContext("info"),
      mocket,
      durable,
      new URL(c.url),
      c.existingClients,
      onMessage,
      onClose
    );

    if (c.expectErrorResponse) {
      expect(mocket.log).toEqual([["send", c.expectErrorResponse], ["close"]]);
      continue;
    }
    expect(mocket.log).toEqual([["send", JSON.stringify(["connected", {}])]]);

    const expectedClients = c.expectedClients(mocket);
    expect(c.existingClients).toEqual(expectedClients);

    const actualRecord = await getEntry(
      durable,
      clientRecordKey("c1"),
      clientRecordSchema
    );
    expect(actualRecord).toEqual(c.expectedRecord);
  }
});
