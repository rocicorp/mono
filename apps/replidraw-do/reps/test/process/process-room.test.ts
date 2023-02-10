import { WriteTransaction } from "replicache";
import { DurableStorage } from "../../src/storage/durable-storage";
import { ClientPokeBody } from "../../src/types/client-poke-body";
import {
  ClientRecord,
  getClientRecord,
  putClientRecord,
} from "../../src/types/client-record";
import { ClientMap } from "../../src/types/client-state";
import { getUserValue, UserValue } from "../../src/types/user-value";
import { getVersion, Version, versionKey } from "../../src/types/version";
import { client, clientRecord, mutation } from "../util/test-utils";
import { FRAME_LENGTH_MS, processRoom } from "../../src/process/process-room";
import { LogContext } from "../../src/util/logger";

const { server } = getMiniflareBindings();
const id = server.newUniqueId();

test("processRoom", async () => {
  type Case = {
    name: string;
    clientRecords: Map<string, ClientRecord>;
    headVersion: Version;
    clients: ClientMap;
    expectedError?: string;
    expectedPokes?: ClientPokeBody[];
    expectedUserValues?: Map<string, UserValue>;
    expectedClientRecords?: Map<string, ClientRecord>;
    expectedVersion: Version;
  };

  const startTime = 100;
  const endTime = 200;

  const cases: Case[] = [
    {
      name: "no client record",
      clientRecords: new Map(),
      headVersion: 42,
      clients: new Map([client("c1")]),
      expectedUserValues: new Map(),
      expectedError: "Error: Client record not found: c1",
      expectedVersion: 42,
    },
    {
      name: "no mutations, clients out of date",
      clientRecords: new Map([
        ["c1", clientRecord()],
        ["c2", clientRecord()],
      ]),
      headVersion: 1,
      clients: new Map([client("c1"), client("c2")]),
      expectedPokes: [
        {
          clientID: "c1",
          poke: {
            baseCookie: null,
            cookie: 1,
            lastMutationID: 1,
            patch: [],
            timestamp: 100,
          },
        },
        {
          clientID: "c2",
          poke: {
            baseCookie: null,
            cookie: 1,
            lastMutationID: 1,
            patch: [],
            timestamp: 100,
          },
        },
      ],
      expectedClientRecords: new Map([
        ["c1", clientRecord(1)],
        ["c2", clientRecord(1)],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 1,
    },
    {
      name: "no mutations, one client out of date",
      clientRecords: new Map([
        ["c1", clientRecord(1)],
        ["c2", clientRecord()],
      ]),
      headVersion: 1,
      clients: new Map([client("c1"), client("c2")]),
      expectedPokes: [
        {
          clientID: "c2",
          poke: {
            baseCookie: null,
            cookie: 1,
            lastMutationID: 1,
            patch: [],
            timestamp: 100,
          },
        },
      ],
      expectedClientRecords: new Map([
        ["c1", clientRecord(1)],
        ["c2", clientRecord(1)],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 1,
    },
    {
      name: "mutations after range",
      clientRecords: new Map([["c1", clientRecord(1)]]),
      headVersion: 1,
      // mutation is at time=300, but we only play from 100-200
      clients: new Map([
        client("c1", undefined, 0, mutation(1, "inc", null, 300)),
      ]),
      expectedPokes: [],
      expectedClientRecords: new Map([["c1", clientRecord(1)]]),
      expectedUserValues: new Map(),
      expectedVersion: 1,
    },
    {
      name: "mutations in range",
      clientRecords: new Map([
        ["c1", clientRecord(1)],
        ["c2", clientRecord(1)],
      ]),
      headVersion: 1,
      clients: new Map([
        client(
          "c1",
          undefined,
          0,
          mutation(2, "inc", null, 105),
          mutation(3, "inc", null, 125)
        ),
        client("c2", undefined, 0, mutation(2, "inc", null, 100)),
      ]),
      expectedPokes: [
        {
          clientID: "c1",
          poke: {
            baseCookie: 1,
            // even though two mutations play we only bump version at most once per frame
            cookie: 2,
            // c1 played one mutation this frame
            lastMutationID: 2,
            patch: [
              // two count mutations played, leaving value at 2
              {
                op: "put",
                key: "count",
                value: 2,
              },
            ],
            timestamp: 100,
          },
        },
        // same thing for second client
        {
          clientID: "c2",
          poke: {
            baseCookie: 1,
            cookie: 2,
            lastMutationID: 2,
            patch: [
              {
                op: "put",
                key: "count",
                value: 2,
              },
            ],
            timestamp: 100,
          },
        },
        {
          clientID: "c1",
          poke: {
            baseCookie: 2,
            cookie: 3,
            lastMutationID: 3,
            patch: [
              {
                op: "put",
                key: "count",
                value: 3,
              },
            ],
            timestamp: 100 + FRAME_LENGTH_MS,
          },
        },
        {
          clientID: "c2",
          poke: {
            baseCookie: 2,
            cookie: 3,
            // only c1 played a mutation this time
            lastMutationID: 2,
            patch: [
              {
                op: "put",
                key: "count",
                value: 3,
              },
            ],
            timestamp: 100 + FRAME_LENGTH_MS,
          },
        },
      ],
      expectedClientRecords: new Map([
        ["c1", clientRecord(3, 3)],
        ["c2", clientRecord(3, 2)],
      ]),
      expectedUserValues: new Map(),
      expectedVersion: 3,
    },
    {
      name: "mutations before range are included",
      clientRecords: new Map([["c1", clientRecord(1)]]),
      headVersion: 1,
      clients: new Map([
        client(
          "c1",
          undefined,
          0,
          mutation(2, "inc", null, 50),
          mutation(3, "inc", null, 100)
        ),
      ]),
      expectedPokes: [
        {
          clientID: "c1",
          poke: {
            baseCookie: 1,
            // even though two mutations play we only bump version at most once per frame
            cookie: 2,
            // two mutations played
            lastMutationID: 3,
            patch: [
              // two count mutations played, leaving value at 2
              {
                op: "put",
                key: "count",
                value: 2,
              },
            ],
            timestamp: 100,
          },
        },
      ],
      expectedClientRecords: new Map([["c1", clientRecord(2, 3)]]),
      expectedUserValues: new Map(),
      expectedVersion: 2,
    },
    {
      name: "mutations late in range",
      clientRecords: new Map([["c1", clientRecord(1)]]),
      headVersion: 1,
      clients: new Map([
        client("c1", undefined, 0, mutation(2, "inc", null, 150)),
      ]),
      expectedPokes: [
        {
          clientID: "c1",
          poke: {
            baseCookie: 1,
            cookie: 2,
            lastMutationID: 2,
            patch: [
              {
                op: "put",
                key: "count",
                value: 1,
              },
            ],
            // We don't get a poke until several frames go by
            timestamp:
              100 + FRAME_LENGTH_MS + FRAME_LENGTH_MS + FRAME_LENGTH_MS,
          },
        },
      ],
      expectedClientRecords: new Map([["c1", clientRecord(2, 2)]]),
      expectedUserValues: new Map(),
      expectedVersion: 2,
    },
  ];

  const durable = await getMiniflareDurableObjectStorage(id);

  const mutators = new Map(
    Object.entries({
      inc: async (tx: WriteTransaction) => {
        let count = ((await tx.get("count")) as number) ?? 0;
        count++;
        await tx.put("count", count);
      },
    })
  );

  for (const c of cases) {
    await durable.deleteAll();
    const storage = new DurableStorage(durable);
    await storage.put(versionKey, c.headVersion);
    for (const [clientID, record] of c.clientRecords) {
      await putClientRecord(clientID, record, storage);
    }

    const p = processRoom(
      new LogContext("info"),
      c.clients,
      mutators,
      startTime,
      endTime,
      durable
    );
    if (c.expectedError) {
      try {
        await p;
        fail("Expected error");
      } catch (e) {
        expect(String(e)).toEqual(c.expectedError);
      }
    } else {
      const pokes = await p;
      expect(pokes).toEqual(c.expectedPokes);
    }

    for (const [clientID, record] of c.expectedClientRecords ?? new Map()) {
      expect(await getClientRecord(clientID, storage)).toEqual(record);
    }

    for (const [key, value] of c.expectedUserValues ?? new Map()) {
      expect(await getUserValue(key, storage)).toEqual(value);
    }

    expect(await getVersion(storage)).toEqual(c.expectedVersion);
  }
});
