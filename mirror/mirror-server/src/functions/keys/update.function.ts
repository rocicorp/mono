import {Lock} from '@rocicorp/lock';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {Timestamp, type Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {apiKeyDataConverter, apiKeyPath} from 'mirror-schema/src/api-key.js';
import {assert} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import {
  UpdateBatch,
  updateKeyRequestSchema,
  updateKeyResponseSchema,
} from '../../keys/updates.js';
import {internalFunctionHeader} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';

export const FLUSH_UPDATES_TIMEOUT = 5_000;

/** Coordinates the flushing of buffered updates. */
export class UpdateCoordinator {
  readonly #lock: Lock = new Lock();
  #buffer: UpdateBuffer = new UpdateBuffer();
  #timerID: ReturnType<typeof setTimeout> | null = null;
  #baton: Resolver<UpdateBuffer | null> = resolver();

  /**
   * Adds an update to be flushed.
   *
   * The returned Promise will resolve to the `UpdateBuffer` if the flush
   * timeout fires, in which case the caller is responsible for flushing
   * the buffer.
   *
   * If another caller adds another update before the flush timeout fires,
   * the first Promise will resolve to `null`, relinquishing the previous caller
   * of the responsibility of flushing.
   *
   * It follows that only a single caller (the last one) is responsible for
   * flushing the buffer.
   */
  async add(keyPath: string, timestamp: number): Promise<UpdateBuffer | null> {
    const baton = await this.#lock.withLock(() => {
      this.#buffer.add(keyPath, timestamp);
      if (!this.#timerID) {
        this.#timerID = setTimeout(
          () => void this.#flushBuffer(),
          FLUSH_UPDATES_TIMEOUT,
        );
      }
      this.#baton.resolve(null); // Release the current baton holder.
      this.#baton = resolver(); // Take the baton.
      return this.#baton;
    });
    return baton.promise;
  }

  #flushBuffer() {
    return this.#lock.withLock(() => {
      this.#timerID = null;
      const buffer = this.#buffer;
      this.#buffer = new UpdateBuffer();
      this.#baton.resolve(buffer); // Signal the current baton holder to flush.
      this.#baton = resolver(); // Unheld baton === empty buffer === no timer
    });
  }
}

export class UpdateBuffer {
  readonly timestamps: Record<string, number> = {};
  coalesced: number = 0;

  add(keyPath: string, timestamp: number) {
    assert(timestamp > 0);
    const existing = this.timestamps[keyPath] ?? 0;
    if (existing > 0) {
      this.coalesced++;
    }
    if (timestamp > existing) {
      this.timestamps[keyPath] = timestamp;
    }
  }
}

/**
 * Global buffer of updates that are shared across invocations of a function
 * instance. The `appKeys-update` functions is configured with a high
 * "concurrency" value so that a single instance can service many invocations.
 */
const globalUpdateCoordinator = new UpdateCoordinator();

/**
 * The function implementation is such that the last function to add an update
 * to the UpdateCoordinator is the one responsible for waiting for the flush
 * timeout to fire. All previous function invocations return early when a new
 * function is invoked and "takes the baton" for eventually flushing the buffer.
 */
export const update = (firestore: Firestore) =>
  validateSchema(updateKeyRequestSchema, updateKeyResponseSchema)
    .validate(internalFunctionHeader())
    .handle(async req => {
      const {appID, keyName, lastUsed} = req;

      // Add the update to the global buffer, and wait for either the buffering
      // timeout to fire, or for the baton to be passed to another updater.
      const buffer = await globalUpdateCoordinator.add(
        apiKeyPath(appID, keyName),
        lastUsed,
      );

      // Baton was passed to another request before the buffering timeout fires.
      if (buffer === null) {
        return {};
      }

      // This request is the baton holder when the buffering timer fired. Flush!
      return firestore.runTransaction(async tx => {
        const keyDocs = await tx.getAll(
          ...Object.keys(buffer.timestamps).map(path =>
            firestore.doc(path).withConverter(apiKeyDataConverter),
          ),
        );
        const updateBatch: UpdateBatch = {
          updates: {},
          coalesced: buffer.coalesced,
        };
        for (const doc of keyDocs) {
          const keyPath = doc.ref.path;
          if (!doc.exists) {
            logger.warn(`Key ${keyPath} was deleted. Skipping lastUsed update`);
            continue;
          }
          const key = must(doc.data());
          const lastUsed = key.lastUsed?.toMillis() ?? 0;
          const newLastUsed = buffer.timestamps[keyPath] ?? 0;
          if (newLastUsed <= lastUsed) {
            logger.warn(
              `Last used time for ${keyPath} (${lastUsed}) is later that ${newLastUsed}`,
            );
            continue;
          }
          updateBatch.updates[keyPath] = newLastUsed;
          tx.update(doc.ref, {lastUsed: Timestamp.fromMillis(newLastUsed)});
        }
        logger.info(
          `Flushing update batch for ${keyDocs.length} key(s)`,
          updateBatch,
        );
        return {flushed: updateBatch};
      });
    });
