import type {ReadonlyJSONValue} from 'shared/src/json.js';
import type * as valita from 'shared/src/valita.js';
import type {ListOptions, Storage} from './storage.js';

// The default safe batch size for scans is based on the CF limit for multi-keyed get(),
// which is a semi-arbitrary heuristic/value for avoiding loading too much data at once.
const defaultSafeBatchSize = 128;

// Subset of the Storage interface used by the `scan` implementations.
type Lister = Pick<Storage, 'list'>;

export async function* scan<T extends ReadonlyJSONValue>(
  storage: Lister,
  options: ListOptions,
  schema: valita.Type<T>,
): AsyncIterable<[key: string, value: T]> {
  for await (const batch of batchScan(
    storage,
    options,
    schema,
    defaultSafeBatchSize,
  )) {
    for (const entry of batch) {
      yield entry;
    }
  }
}

export async function* batchScan<T extends ReadonlyJSONValue>(
  storage: Lister,
  options: ListOptions,
  schema: valita.Type<T>,
  batchSize: number,
): AsyncIterable<Map<string, T>> {
  batchSize = batchSize ?? defaultSafeBatchSize;
  let remainingLimit = options.limit;
  const batchOptions = {
    ...options,
    limit: Math.min(batchSize, remainingLimit ?? batchSize),
  };

  while (batchOptions.limit > 0) {
    const batch = await storage.list(batchOptions, schema);
    if (batch.size === 0) {
      break;
    }

    // Guaranteed to be non-empty.
    yield batch;

    let lastKey = '';
    for (const key of batch.keys()) {
      lastKey = key;
    }
    batchOptions.start = {
      key: lastKey,
      exclusive: true,
    };
    if (remainingLimit) {
      remainingLimit -= batch.size;
      batchOptions.limit = Math.min(batchSize, remainingLimit);
    }
  }
}
