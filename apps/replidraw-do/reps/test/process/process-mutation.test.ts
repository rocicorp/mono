import { WriteTransaction } from "replicache";
import { DurableStorage } from "../../src/storage/durable-storage";
import { ClientMutation } from "../../src/types/client-mutation";
import {
  ClientRecord,
  getClientRecord,
  putClientRecord,
} from "../../src/types/client-record";
import { getUserValue } from "../../src/types/user-value";
import { getVersion } from "../../src/types/version";
import { clientMutation, clientRecord } from "../util/test-utils";
import {
  MutatorMap,
  processMutation,
} from "../../src/process/process-mutation";
import { LogContext } from "../../src/util/logger";

const { server } = getMiniflareBindings();
const id = server.newUniqueId();

test("processMutation", async () => {
  type Case = {
    name: string;
    existingRecord?: ClientRecord;
    mutation: ClientMutation;
    expectedError?: string;
    expectedRecord?: ClientRecord;
    expectAppWrite: boolean;
    expectVersionWrite: boolean;
  };

  const cases: Case[] = [
    {
      name: "clientID not found",
      mutation: clientMutation("c1", 1),
      expectedError: "Error: Client c1 not found",
      expectAppWrite: false,
      expectVersionWrite: false,
    },
    {
      name: "duplicate mutation",
      existingRecord: clientRecord(null, 1),
      mutation: clientMutation("c1", 1),
      expectedRecord: clientRecord(null, 1),
      expectAppWrite: false,
      expectVersionWrite: false,
    },
    {
      name: "ooo mutation",
      existingRecord: clientRecord(null, 1),
      mutation: clientMutation("c1", 3),
      expectedRecord: clientRecord(null, 1),
      expectAppWrite: false,
      expectVersionWrite: false,
    },
    {
      name: "unknown mutator",
      existingRecord: clientRecord(null, 1),
      mutation: clientMutation("c1", 2, "unknown"),
      expectedRecord: clientRecord(null, 2),
      expectAppWrite: false,
      expectVersionWrite: true,
    },
    {
      name: "mutator throws",
      existingRecord: clientRecord(null, 1),
      mutation: clientMutation("c1", 2, "throws"),
      expectedRecord: clientRecord(null, 2),
      expectAppWrite: false,
      expectVersionWrite: true,
    },
    {
      name: "success",
      existingRecord: clientRecord(null, 1),
      mutation: clientMutation("c1", 2, "foo"),
      expectedRecord: clientRecord(null, 2),
      expectAppWrite: true,
      expectVersionWrite: true,
    },
  ];

  const mutators: MutatorMap = new Map([
    [
      "foo",
      async (tx: WriteTransaction) => {
        await tx.put("foo", "bar");
      },
    ],
    [
      "throws",
      async () => {
        throw new Error("bonk");
      },
    ],
  ]);

  const durable = await getMiniflareDurableObjectStorage(id);

  for (const c of cases) {
    const storage = new DurableStorage(durable);
    const version = 2;
    const { clientID } = c.mutation;

    if (c.existingRecord) {
      await putClientRecord(clientID, c.existingRecord, storage);
    }

    let err: string | undefined;
    try {
      await processMutation(
        new LogContext("info"),
        c.mutation,
        mutators,
        storage,
        version
      );
    } catch (e) {
      err = String(e);
    }

    expect(err).toEqual(c.expectedError);
    expect(await getClientRecord(clientID, storage)).toEqual(c.expectedRecord);
    expect(await getUserValue("foo", storage)).toEqual(
      c.expectAppWrite ? { version, deleted: false, value: "bar" } : undefined
    );

    const expectedVersion = c.expectVersionWrite ? version : undefined;
    expect(await getVersion(storage)).toEqual(expectedVersion);
  }
});
