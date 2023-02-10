import { Patch } from "../protocol/poke";
import { userValuePrefix, userValueSchema } from "../types/user-value";
import { Version } from "../types/version";

export async function getPatch(
  durable: DurableObjectStorage,
  fromCookie: Version
): Promise<Patch> {
  const result = await durable.list({
    prefix: userValuePrefix,
    allowConcurrency: true,
  });

  const patch: Patch = [];
  for (const [key, value] of result) {
    const validValue = userValueSchema.parse(value);

    // TODO: More efficient way of finding changed values.
    if (validValue.version <= fromCookie) {
      continue;
    }

    const unwrappedKey = key.substring(userValuePrefix.length);
    const unwrappedValue = validValue.value;
    if (validValue.deleted) {
      patch.push({
        op: "del",
        key: unwrappedKey,
      });
    } else {
      patch.push({
        op: "put",
        key: unwrappedKey,
        value: unwrappedValue,
      });
    }
  }
  return patch;
}
