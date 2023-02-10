import { JSONValue } from "replicache";
import { ZodSchema } from "zod";

// DurableObjects has a lot of clever optimisations we can take advantage of,
// but they require some thought as to whether they fit with what we are doing.
// These settings make DO behave more like a basic kv store and thus work
// better with our existing code.
// TODO: Evaluate these options and perhaps simplify our code by taking advantage.
const options = {
  // We already the currency at a higher level in the game loop.
  allowConcurrency: true,
  // Only for writes: We need to carefully consider transactionality and when we
  // return responses.
  allowUnconfirmed: true,
};

export async function getEntry<T extends JSONValue>(
  durable: DurableObjectStorage,
  key: string,
  schema: ZodSchema<T>
): Promise<T | undefined> {
  const value = await durable.get(key, options);
  if (value === undefined) {
    return undefined;
  }
  return schema.parse(value);
}

export async function putEntry<T extends JSONValue>(
  durable: DurableObjectStorage,
  key: string,
  value: T
): Promise<void> {
  await durable.put(key, value, options);
}

export async function delEntry(
  durable: DurableObjectStorage,
  key: string
): Promise<void> {
  await durable.delete(key, options);
}
