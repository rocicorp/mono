import { test, expect, beforeEach } from "@jest/globals";
import type { Mutation } from "../protocol/push.js";
import {
  client as clientUtil,
  createSilentLogContext,
  Mocket,
  mutation,
} from "../util/test-utils.js";
import { handlePush } from "../server/push.js";
import type { ClientState } from "../types/client-state.js";

let s1: Mocket;
beforeEach(() => {
  s1 = new Mocket();
});

function client(...mutations: Mutation[]): ClientState {
  return clientWTimestampAdjust(0, ...mutations);
}

function clientWTimestampAdjust(
  clockBehindByMs: number,
  ...mutations: Mutation[]
): ClientState {
  return clientUtil("c1", "u1", s1, clockBehindByMs, ...mutations)[1];
}

test("handlePush", async () => {
  type Case = {
    name: string;
    client: ClientState;
    mutations: Mutation[];
    expectedClient: ClientState;
  };

  const cases: Case[] = [
    {
      name: "no mutations",
      client: client(mutation(1, "foo", {}, 1)),
      mutations: [],
      expectedClient: client(mutation(1, "foo", {}, 1)),
    },
    {
      name: "empty pending, single mutation",
      client: client(),
      mutations: [mutation(1)],
      expectedClient: client(mutation(1)),
    },
    {
      name: "empty pending, multiple mutations",
      client: client(),
      mutations: [mutation(1), mutation(2)],
      expectedClient: client(mutation(1), mutation(2)),
    },
    {
      name: "empty pending, multiple mutations ooo",
      client: client(),
      mutations: [mutation(2), mutation(1)],
      expectedClient: client(mutation(1), mutation(2)),
    },
    {
      name: "single pending, single mutation end",
      client: client(mutation(1)),
      mutations: [mutation(2)],
      expectedClient: client(mutation(1), mutation(2)),
    },
    {
      name: "single pending, single mutation start",
      client: client(mutation(2)),
      mutations: [mutation(1)],
      expectedClient: client(mutation(1), mutation(2)),
    },
    {
      name: "multi pending, single mutation middle",
      client: client(mutation(1), mutation(3)),
      mutations: [mutation(2)],
      expectedClient: client(mutation(1), mutation(2), mutation(3)),
    },
    {
      name: "single pending, gap after",
      client: client(mutation(1)),
      mutations: [mutation(3)],
      expectedClient: client(mutation(1), mutation(3)),
    },
    {
      name: "single pending, gap before",
      client: client(mutation(3)),
      mutations: [mutation(1)],
      expectedClient: client(mutation(1), mutation(3)),
    },
    {
      name: "single pending, duplicate",
      client: client(mutation(1)),
      mutations: [mutation(1)],
      expectedClient: client(mutation(1)),
    },
    {
      name: "multi pending, duplicate",
      client: client(mutation(1), mutation(2)),
      mutations: [mutation(1)],
      expectedClient: client(mutation(1), mutation(2)),
    },
    {
      name: "timestamp adjustment",
      client: clientWTimestampAdjust(7),
      mutations: [mutation(1, "foo", {}, 3)],
      expectedClient: clientWTimestampAdjust(7, mutation(1, "foo", {}, 10)),
    },
    {
      name: "negative timestamp adjustment",
      client: clientWTimestampAdjust(-7),
      mutations: [mutation(1, "foo", {}, 3)],
      expectedClient: clientWTimestampAdjust(-7, mutation(1, "foo", {}, -4)),
    },
  ];

  for (const c of cases) {
    s1.log.length = 0;

    const push = {
      clientID: "c1",
      mutations: c.mutations,
      pushVersion: 0,
      schemaVersion: "",
      timestamp: 42,
    };
    handlePush(
      createSilentLogContext(),
      c.client,
      push,
      () => 42,
      () => undefined
    );
    expect(s1.log).toEqual([]);
    expect(c.client).toEqual(c.expectedClient);
  }
});
