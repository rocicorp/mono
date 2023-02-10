import { test, expect } from "@jest/globals";
import type { PushBody } from "../../src/protocol/push.js";
import type { ClientID, ClientMap } from "../../src/types/client-state.js";
import {
  client,
  createSilentLogContext,
  Mocket,
  mutation,
} from "../util/test-utils.js";
import { handleMessage } from "../../src/server/message.js";

test("handleMessage", async () => {
  type Case = {
    name: string;
    data: string;
    clients?: ClientMap;
    clientID?: ClientID;
    expectedError?: string;
    expectedPush?: PushBody;
    expectSocketClosed?: boolean;
  };

  const cases: Case[] = [
    {
      name: "empty",
      data: "",
      expectedError: "SyntaxError: Unexpected end of JSON input",
    },
    // {
    //   name: "invalid push",
    //   data: "[]",
    //   expectedError: "Should have at least 2 items",
    // },
    {
      name: "valid push",
      data: JSON.stringify([
        "push",
        {
          clientID: "c1",
          mutations: [mutation(1), mutation(2)],
          pushVersion: 1,
          schemaVersion: "",
          timestamp: 42,
        },
      ]),
      expectedPush: {
        clientID: "c1",
        mutations: [mutation(1), mutation(2)],
        pushVersion: 1,
        schemaVersion: "",
        timestamp: 42,
      },
    },
    {
      name: "missing client push",
      data: JSON.stringify([
        "push",
        {
          clientID: "c1",
          mutations: [mutation(1), mutation(2)],
          pushVersion: 1,
          schemaVersion: "",
          timestamp: 42,
        },
      ]),
      clients: new Map(),
      clientID: "c1",
      expectedError: "no such client: c1",
      expectSocketClosed: true,
    },
    {
      name: "missing client ping",
      data: JSON.stringify(["ping", {}]),
      clients: new Map(),
      clientID: "c1",
      expectedError: "no such client: c1",
      expectSocketClosed: true,
    },
  ];

  for (const c of cases) {
    const s1 = new Mocket();
    const clientID = c.clientID !== undefined ? c.clientID : "c1";
    const clients: ClientMap =
      c.clients || new Map([client(clientID, "u1", s1)]);
    // let called = false;

    // const handlePush = (
    //   // pClients: ClientMap,
    //   pClientID: ClientID,
    //   pBody: PushBody,
    //   pWS: Socket
    // ) => {
    //   expect(pClientID).toEqual(clientID);
    //   expect(pBody).toEqual(c.expectedPush);
    //   expect(pWS).toEqual(s1);
    //   called = true;
    // };
    handleMessage(
      createSilentLogContext(),
      clients,
      clientID,
      c.data,
      s1,
      () => undefined
    );
    if (c.expectedError) {
      expect(s1.log.length).toEqual(c.expectSocketClosed ? 2 : 1);
      const [type, message] = s1.log[0];
      expect(type).toEqual("send");
      expect(message).toContain(c.expectedError);
    } else {
      // expect(called);
    }
    if (c.expectSocketClosed) {
      expect(s1.log.length).toBeGreaterThan(0);
      expect(s1.log[s1.log.length - 1][0]).toEqual("close");
      0;
    }
  }
});
