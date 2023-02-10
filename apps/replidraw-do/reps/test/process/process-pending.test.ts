import { WriteTransaction } from "replicache";
import { PokeBody } from "../../src/protocol/poke";
import { DurableStorage } from "../../src/storage/durable-storage";
import {
  ClientRecord,
  getClientRecord,
  putClientRecord,
} from "../../src/types/client-record";
import { ClientID, ClientMap } from "../../src/types/client-state";
import { getUserValue, UserValue } from "../../src/types/user-value";
import { getVersion, putVersion, Version } from "../../src/types/version";
import { client, clientRecord, Mocket, mutation } from "../util/test-utils";
import { processPending } from "../../src/process/process-pending";
import { LogContext } from "../../src/util/logger";

const { server } = getMiniflareBindings();
const id = server.newUniqueId();

test("processPending", async () => {
  type Case = {
    name: string;
    version: Version;
    clientRecords: Map<ClientID, ClientRecord>;
    clients: ClientMap;
    expectedError?: string;
    expectedClients: ClientMap;
    expectedVersion: Version;
    expectedPokes?: Map<Mocket, PokeBody[]>;
    expectedUserValues?: Map<string, UserValue>;
    expectedClientRecords?: Map<ClientID, ClientRecord>;
  };

  const s1 = new Mocket();
  const s2 = new Mocket();

  const cases: Case[] = [
    {
      name: "none pending",
      version: 1,
      clientRecords: new Map([["c1", clientRecord(1)]]),
      clients: new Map(),
      expectedClients: new Map(),
      expectedVersion: 1,
      expectedPokes: new Map(),
      expectedUserValues: new Map(),
      expectedClientRecords: new Map([["c1", clientRecord(1)]]),
    },
    {
      name: "one client, one mutation",
      version: 1,
      clientRecords: new Map([["c1", clientRecord(1)]]),
      clients: new Map([client("c1", s1, 0, mutation(2, "inc", null, 100))]),
      expectedClients: new Map([client("c1", s1, 0)]),
      expectedVersion: 2,
      expectedPokes: new Map([
        [
          s1,
          [
            {
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
              timestamp: 100,
            },
          ],
        ],
      ]),
      expectedUserValues: new Map([
        ["count", { value: 1, version: 2, deleted: false }],
      ]),
      expectedClientRecords: new Map([["c1", clientRecord(2, 2)]]),
    },
    {
      name: "two clients, two mutations",
      version: 1,
      clientRecords: new Map([
        ["c1", clientRecord(1)],
        ["c2", clientRecord(1)],
      ]),
      clients: new Map([
        client("c1", s1, 0, mutation(2, "inc", null, 100)),
        client("c2", s2, 0, mutation(2, "inc", null, 120)),
      ]),
      expectedClients: new Map([client("c1", s1, 0), client("c2", s2, 0)]),
      expectedVersion: 3,
      expectedPokes: new Map([
        [
          s1,
          [
            {
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
              timestamp: 100,
            },
            {
              baseCookie: 2,
              cookie: 3,
              lastMutationID: 2,
              patch: [
                {
                  op: "put",
                  key: "count",
                  value: 2,
                },
              ],
              timestamp: 116 + 2 / 3,
            },
          ],
        ],
        [
          s2,
          [
            {
              baseCookie: 1,
              cookie: 2,
              lastMutationID: 1,
              patch: [
                {
                  op: "put",
                  key: "count",
                  value: 1,
                },
              ],
              timestamp: 100,
            },
            {
              baseCookie: 2,
              cookie: 3,
              lastMutationID: 2,
              patch: [
                {
                  op: "put",
                  key: "count",
                  value: 2,
                },
              ],
              timestamp: 116 + 2 / 3,
            },
          ],
        ],
      ]),
      expectedUserValues: new Map([
        ["count", { value: 2, version: 3, deleted: false }],
      ]),
      expectedClientRecords: new Map([
        ["c1", clientRecord(3, 2)],
        ["c2", clientRecord(3, 2)],
      ]),
    },
    {
      name: "two clients, two mutations, one not ready",
      version: 1,
      clientRecords: new Map([
        ["c1", clientRecord(1)],
        ["c2", clientRecord(1)],
      ]),
      clients: new Map([
        client("c1", s1, 0, mutation(2, "inc", null, 100)),
        client("c2", s2, 0, mutation(2, "inc", null, 300)),
      ]),
      expectedClients: new Map([
        client("c1", s1, 0),
        client("c2", s2, 0, mutation(2, "inc", null, 300)),
      ]),
      expectedVersion: 2,
      expectedPokes: new Map([
        [
          s1,
          [
            {
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
              timestamp: 100,
            },
          ],
        ],
        [
          s2,
          [
            {
              baseCookie: 1,
              cookie: 2,
              lastMutationID: 1,
              patch: [
                {
                  op: "put",
                  key: "count",
                  value: 1,
                },
              ],
              timestamp: 100,
            },
          ],
        ],
      ]),
      expectedUserValues: new Map([
        ["count", { value: 1, version: 2, deleted: false }],
      ]),
      expectedClientRecords: new Map([
        ["c1", clientRecord(2, 2)],
        ["c2", clientRecord(2, 1)],
      ]),
    },
  ];

  const mutators = new Map(
    Object.entries({
      inc: async (tx: WriteTransaction) => {
        let count = ((await tx.get("count")) as number) ?? 0;
        count++;
        await tx.put("count", count);
      },
    })
  );

  const startTime = 100;
  const endTime = 200;

  const durable = await getMiniflareDurableObjectStorage(id);

  for (const c of cases) {
    console.log(c.name);
    await durable.deleteAll();
    const storage = new DurableStorage(durable);
    await putVersion(c.version, storage);
    for (const [clientID, record] of c.clientRecords) {
      await putClientRecord(clientID, record, storage);
    }
    for (const [, clientState] of c.clients) {
      (clientState.socket as Mocket).log.length = 0;
    }
    const p = processPending(
      new LogContext("info"),
      durable,
      c.clients,
      mutators,
      startTime,
      endTime
    );
    if (c.expectedError) {
      try {
        await p;
        fail("should have thrown");
      } catch (e) {
        expect(String(e)).toEqual(c.expectedError);
      }
      continue;
    }

    await p;
    expect(c.clients).toEqual(c.expectedClients);

    expect(c.expectedError).toBeUndefined;
    expect(await getVersion(storage)).toEqual(c.expectedVersion);
    for (const [mocket, clientPokes] of c.expectedPokes ?? []) {
      console.log(mocket.log);
      expect(mocket.log).toEqual(
        clientPokes.map((poke) => ["send", JSON.stringify(["poke", poke])])
      );
    }
    for (const [expKey, expValue] of c.expectedUserValues ?? new Map()) {
      expect(await getUserValue(expKey, storage)).toEqual(expValue);
    }
    for (const [expClientID, expRecord] of c.expectedClientRecords ??
      new Map()) {
      expect(await getClientRecord(expClientID, storage)).toEqual(expRecord);
    }
  }
});
