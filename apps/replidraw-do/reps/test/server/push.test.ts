import { Mutation } from "../../src/protocol/push";
import { client, Mocket, mutation } from "../util/test-utils";
import { handlePush } from "../../src/server/push";
import { LogContext } from "../../src/util/logger";
import { ClientMap } from "@/types/client-state";

test("handlePush", async () => {
  const s1 = new Mocket();
  const s2 = new Mocket();
  const s3 = new Mocket();

  type Case = {
    name: string;
    existingClients: ClientMap;
    mutations: Mutation[];
    expectedError: string;
    expectedClients: ClientMap;
  };

  const cases: Case[] = [
    {
      name: "no clients",
      existingClients: new Map(),
      mutations: [],
      expectedError: "no such client: c1",
      expectedClients: new Map(),
    },
    {
      name: "wrong client",
      existingClients: new Map([client("c2", s2)]),
      mutations: [],
      expectedError: "no such client: c1",
      expectedClients: new Map([client("c2", s2)]),
    },
    {
      name: "no mutations",
      existingClients: new Map([
        client("c1", s1, 1, mutation(1, "foo", {}, 1)),
      ]),
      mutations: [],
      expectedError: "",
      expectedClients: new Map([
        client("c1", s1, 1, mutation(1, "foo", {}, 1)),
      ]),
    },
    {
      name: "empty pending, single mutation",
      existingClients: new Map([client("c1", s1, 0)]),
      mutations: [mutation(1)],
      expectedError: "",
      expectedClients: new Map([client("c1", s1, 0, mutation(1))]),
    },
    {
      name: "empty pending, multiple mutations",
      existingClients: new Map([client("c1", s1, 0)]),
      mutations: [mutation(1), mutation(2)],
      expectedError: "",
      expectedClients: new Map([client("c1", s1, 0, mutation(1), mutation(2))]),
    },
    {
      name: "empty pending, multiple mutations ooo",
      existingClients: new Map([client("c1", s1, 0)]),
      mutations: [mutation(2), mutation(1)],
      expectedError: "",
      expectedClients: new Map([client("c1", s1, 0, mutation(1), mutation(2))]),
    },
    {
      name: "single pending, single mutation end",
      existingClients: new Map([client("c1", s1, 0, mutation(1))]),
      mutations: [mutation(2)],
      expectedError: "",
      expectedClients: new Map([client("c1", s1, 0, mutation(1), mutation(2))]),
    },
    {
      name: "single pending, single mutation start",
      existingClients: new Map([client("c1", s1, 0, mutation(2))]),
      mutations: [mutation(1)],
      expectedError: "",
      expectedClients: new Map([client("c1", s1, 0, mutation(1), mutation(2))]),
    },
    {
      name: "multi pending, single mutation middle",
      existingClients: new Map([client("c1", s1, 0, mutation(1), mutation(3))]),
      mutations: [mutation(2)],
      expectedError: "",
      expectedClients: new Map([
        client("c1", s1, 0, mutation(1), mutation(2), mutation(3)),
      ]),
    },
    {
      name: "single pending, gap after",
      existingClients: new Map([client("c1", s1, 0, mutation(1))]),
      mutations: [mutation(3)],
      expectedError: "",
      expectedClients: new Map([client("c1", s1, 0, mutation(1), mutation(3))]),
    },
    {
      name: "single pending, gap before",
      existingClients: new Map([client("c1", s1, 0, mutation(3))]),
      mutations: [mutation(1)],
      expectedError: "",
      expectedClients: new Map([client("c1", s1, 0, mutation(1), mutation(3))]),
    },
    {
      name: "single pending, duplicate",
      existingClients: new Map([client("c1", s1, 0, mutation(1))]),
      mutations: [mutation(1)],
      expectedError: "",
      expectedClients: new Map([client("c1", s1, 0, mutation(1))]),
    },
    {
      name: "multi pending, duplicate",
      existingClients: new Map([client("c1", s1, 0, mutation(1), mutation(2))]),
      mutations: [mutation(1)],
      expectedError: "",
      expectedClients: new Map([client("c1", s1, 0, mutation(1), mutation(2))]),
    },
    {
      name: "timestamp adjustment",
      existingClients: new Map([client("c1", s1, 7)]),
      mutations: [mutation(1, "foo", {}, 3)],
      expectedError: "",
      expectedClients: new Map([
        client("c1", s1, 7, mutation(1, "foo", {}, 10)),
      ]),
    },
    {
      name: "negative timestamp adjustment",
      existingClients: new Map([client("c1", s1, -7)]),
      mutations: [mutation(1, "foo", {}, 3)],
      expectedError: "",
      expectedClients: new Map([
        client("c1", s1, -7, mutation(1, "foo", {}, -4)),
      ]),
    },
  ];

  for (const c of cases) {
    s1.log.length = 0;
    s2.log.length = 0;
    s3.log.length = 0;

    const push = {
      mutations: c.mutations,
      pushVersion: 0,
      schemaVersion: "",
      timestamp: 42,
    };
    const clients = c.existingClients;
    handlePush(
      new LogContext("info"),
      clients,
      "c1",
      push,
      s1,
      () => 42,
      () => undefined
    );
    if (c.expectedError) {
      expect(s1.log).toEqual([
        ["send", JSON.stringify(["error", c.expectedError])],
      ]);
    } else {
      expect(s1.log).toEqual([]);
    }
    /*
    console.log(
      JSON.stringify(server.rooms.get("r1")?.clients.get("c1")?.pending)
    );
    console.log(
      JSON.stringify(c.expectedClients.get("r1")?.clients.get("c1")?.pending)
    );
    */
    expect(clients).toEqual(c.expectedClients);
  }
});
