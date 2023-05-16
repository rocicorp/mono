import type {ReadonlyJSONValue} from 'shared/json.js';
import type * as valita from 'shared/valita.js';
import type {ListOptions} from './storage.js';

// The default safe batch size for scans is based on the CF limit for multi-keyed get().
const defaultSafeBatchSize = 128;

/**
 * Contains implementations of Storage methods based purely on
 * other storage methods.
 */
export abstract class AbstractStorage {
  abstract list<T extends ReadonlyJSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
  ): Promise<Map<string, T>>;

  async scan<T extends ReadonlyJSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
    processBatch: (batch: Map<string, T>) => Promise<void>,
    safeBatchSize?: number,
  ): Promise<void> {
    safeBatchSize = safeBatchSize || defaultSafeBatchSize;
    let remainingLimit = options.limit;
    const batchOptions = {
      ...options,
      limit: Math.min(safeBatchSize, remainingLimit ?? safeBatchSize),
    };

    while (batchOptions.limit > 0) {
      const batch = await this.list(batchOptions, schema);
      if (batch.size == 0) {
        break;
      }

      // Guaranteed to be non-empty.
      await processBatch(batch);

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
        batchOptions.limit = Math.min(safeBatchSize, remainingLimit);
      }
    }

    // Final empty batch signals that the results have been fully scanned.
    await processBatch(new Map());
  }
}
