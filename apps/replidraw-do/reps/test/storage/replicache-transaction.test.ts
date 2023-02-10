import { ReplicacheTransaction } from "../../src/storage/replicache-transaction";
import { EntryCache } from "../../src/storage/entry-cache";
import {
  UserValue,
  userValueKey,
  userValueSchema,
} from "../../src/types/user-value";
import { DurableStorage } from "@/storage/durable-storage";

const { server } = getMiniflareBindings();
const id = server.newUniqueId();

test("ReplicacheTransaction", async () => {
  const storage = new DurableStorage(
    await getMiniflareDurableObjectStorage(id)
  );

  const entryCache = new EntryCache(storage);
  const writeTx = new ReplicacheTransaction(entryCache, "c1", 1);

  expect(!(await writeTx.has("foo")));
  expect(await writeTx.get("foo")).toBeUndefined;

  await writeTx.put("foo", "bar");
  expect(await writeTx.has("foo"));
  expect(await writeTx.get("foo")).toEqual("bar");

  // They don't overlap until one flushes and the other is reloaded.
  const writeTx2 = new ReplicacheTransaction(new EntryCache(storage), "c1", 2);
  expect(!(await writeTx2.has("foo")));
  expect(await writeTx2.get("foo")).toBeUndefined;

  // TODO: scan

  // Go ahead and flush one
  await entryCache.flush();
  const writeTx3 = new ReplicacheTransaction(entryCache, "c1", 3);
  expect(await writeTx3.has("foo"));
  expect(await writeTx3.get("foo")).toEqual("bar");

  // Check the underlying storage gets written in the way we expect.
  const expected: UserValue = {
    deleted: false,
    value: "bar",
    version: 1,
  };
  expect(await storage.get(userValueKey("foo"), userValueSchema)).toEqual(
    expected
  );

  // delete has special return value
  expect(await writeTx3.del("foo"));
  expect(!(await writeTx3.del("bar")));
});
