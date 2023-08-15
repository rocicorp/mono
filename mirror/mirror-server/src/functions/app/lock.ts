import {Timestamp, type DocumentReference} from '@google-cloud/firestore';
import type {LockDoc} from 'mirror-schema/src/lock.js';
import {toMillis} from 'mirror-schema/src/timestamp.js';
import {watch} from 'mirror-schema/src/watch.js';
import {must} from 'shared/src/must.js';
import {logger} from 'firebase-functions';
import {Lock as InMemoryLock} from '@rocicorp/lock';
import {assert} from 'shared/src/asserts.js';

/**
 * The interval at which the lock is renewed if the lock holding logic has not
 * completed. The actual lease duration includes an additional buffer to allow
 * time for the renewal to take place.
 *
 * The interval is chosen to be a balance between (1) something long enough such that
 * lease renewal is not common, as lock renewals require Firestore I/O and
 * introduce the possibility of transient errors, and (2) short enough such that
 * recovery from an aborted lock-holding process is reasonable.
 *
 * If necessary, this interval can be made configurable, but for simplicity it
 * is currently a constant.
 */
export const LEASE_INTERVAL_MS = 20_000;
export const LEASE_BUFFER_MS = 2_000;

/**
 * Lock is used for Firestore-mediated serialization of access to external systems.
 * An example use case is ensuring that only one Cloudflare `publish` is running for
 * a given app at any given time (e.g. even if a user-requested publish and
 * server-update are concurrently requested).
 *
 * Internally, Lock works by lock holders creating a LockDoc at an agreed upon path,
 * running their logic, and then deleting the doc. The existence of the LockDoc indicates
 * to other processes that the lock is held; those processes then wait for the current
 * lock holder to "release" the lock by deleting the document.
 *
 * Because it is possible for the lock holder to be aborted without proper cleanup
 * (e.g. process killed), it is important to consider the possibility of a LockDoc being
 * "orphaned" and never deleted by the lock holder. To handle this scenario, a LockDoc
 * is actually a timed "lease" of the lock, with the expiration time stored in the
 * document itself. While the lock holder process is running, the Lock class periodically
 * extends the lease by moving the expiration forward in time. If the process gets killed,
 * then, the lease eventually expires, signalling to other processes that the document
 * can be deleted and created anew.
 */
export class Lock {
  readonly #doc: DocumentReference<LockDoc>;

  /**
   *
   * @param doc The document used to coordinate the lock.
   */
  constructor(doc: DocumentReference<LockDoc>) {
    this.#doc = doc;
  }

  /**
   * Runs logic while exclusively holding the Firestore-mediated Lock.
   *
   * @param name The name of what lock is being acquired for, for debugging.
   * @param fn The function to run while holding the lock.
   * @param timeoutMs The timeout after which lock acquisition should be aborted with
   *    a `resource-exhausted` error. Defaults to 10 minutes.
   * @returns The value returned by `fn`.
   */
  async withLock<T>(
    name: string,
    fn: () => MaybePromise<T>,
    timeoutMs: number = 1000 * 60 * 10,
  ): Promise<T> {
    const lockHolder = await this.#acquireLock(name, timeoutMs);
    try {
      return await fn();
    } finally {
      await lockHolder.release();
    }
  }

  async #acquireLock(name: string, timeoutMs: number): Promise<LockHolder> {
    const acquireStart = Timestamp.now();
    let expirationTimer: NodeJS.Timer | undefined;

    for await (const snapshot of watch(this.#doc, timeoutMs)) {
      clearTimeout(expirationTimer);

      if (!snapshot.exists) {
        // Common case: LockDoc does not exist. Try to create it.
        try {
          const lockHolder = new LockHolder(this.#doc, name);
          return await lockHolder.acquire(acquireStart);
        } catch (e) {
          // Could be an error, or could be contention (a different Locker won).
          // Let the loop run on the next snapshot of the LockDoc.
          logger.warn(`Error acquiring ${this.#doc.path} lock for ${name}`, e);
        }
      } else {
        // LockDoc exists.
        const {holder, expiration} = must(snapshot.data());
        const expirationMs = toMillis(expiration);
        logger.info(
          `Existing ${
            this.#doc.path
          } lock held by ${holder} set to expire at ${new Date(
            expirationMs,
          ).toISOString()}`,
        );
        // Wait for the current lock holder to release (i.e. delete) the Lock,
        // or for the timer to delete it at expiration. Note that because the delete
        // is preconditioned on the lock's updateTime, it will not delete the lock if
        // the lock was concurrently updated (i.e. its lease extended).
        const lockTimes = {
          createTime: must(snapshot.createTime),
          updateTime: must(snapshot.updateTime),
        };
        expirationTimer = setTimeout(
          () => this.#expireLock(holder, lockTimes),
          expirationMs - Date.now(),
        );
      }
    }
    // The watch loop only exits with a `return` or a TimeoutError ('resource-exhausted').
    throw new Error('impossible');
  }

  async #expireLock(holder: string, lock: LockTimes): Promise<void> {
    try {
      const deleteResult = await this.#doc.delete({
        lastUpdateTime: lock.updateTime,
      });
      const elapsed =
        deleteResult.writeTime.toMillis() - lock.createTime.toMillis();
      logger.info(
        `Expired ${
          this.#doc.path
        } lock held by ${holder} after for ${elapsed} milliseconds`,
      );
    } catch (e) {
      logger.error(
        `Error expiring ${this.#doc.path} lock held by ${holder}`,
        e,
      );
    }
  }
}

class LockHolder {
  readonly #doc: DocumentReference<LockDoc>;
  readonly #name: string;

  // An InMemory lock is used to make updates to the Firestore doc and the
  // #lock metadata atomic.
  readonly #updateLock = new InMemoryLock();
  readonly #lock: LockTimes = {
    createTime: Timestamp.fromMillis(0),
    updateTime: Timestamp.fromMillis(0),
  };
  #extensionTimer: NodeJS.Timer | undefined;

  constructor(doc: DocumentReference<LockDoc>, name: string) {
    this.#doc = doc;
    this.#name = name;
  }

  acquire(start: Timestamp): Promise<LockHolder> {
    assert(this.#lock.updateTime.toMillis() === 0);

    return this.#updateLock.withLock(async () => {
      const writeResult = await this.#doc.create(this.#newLease());
      this.#lock.createTime = writeResult.writeTime;
      this.#lock.updateTime = writeResult.writeTime;
      this.#extensionTimer = setInterval(
        () => this.#extend(),
        LEASE_INTERVAL_MS,
      );

      logger.info(
        `Acquired ${this.#doc.path} lock for ${this.#name} in ${
          writeResult.writeTime.toMillis() - start.toMillis()
        } milliseconds`,
      );
      return this;
    });
  }

  #extend(): Promise<void> {
    assert(this.#lock.updateTime.toMillis() > 0);

    return this.#updateLock.withLock(async () => {
      try {
        const writeResult = await this.#doc.update(this.#newLease(), {
          lastUpdateTime: this.#lock.updateTime,
        });
        this.#lock.updateTime = writeResult.writeTime;
      } catch (e) {
        logger.error(
          `Error extending ${this.#doc.path} lock for ${this.#name}`,
          e,
        );
      }
    });
  }

  /**
   * Creates a LockDoc with a lease expiration `LEASE_INTERVAL_MS`, with a small
   * buffer for the lease extension operation.
   */
  #newLease(): LockDoc {
    return {
      holder: this.#name,
      expiration: Timestamp.fromMillis(
        Date.now() + LEASE_INTERVAL_MS + LEASE_BUFFER_MS,
      ),
    };
  }

  release(): Promise<void> {
    assert(this.#lock.updateTime.toMillis() > 0);
    clearTimeout(this.#extensionTimer);

    return this.#updateLock.withLock(async () => {
      try {
        const deleted = await this.#doc.delete({
          lastUpdateTime: this.#lock.updateTime,
        });
        const elapsed =
          deleted.writeTime.toMillis() - this.#lock.createTime.toMillis();
        logger.info(
          `${this.#doc.path} lock held by ${
            this.#name
          } for ${elapsed} milliseconds`,
        );
      } catch (e) {
        // A failure to release the Lock need not result in an error in the calling code.
        // Log the exception and let the expiration logic (of the next lock acquirer)
        // clean up the lock.
        logger.error(
          `Error releasing ${this.#doc.path} lock held by ${this.#name}`,
          e,
        );
      }
    });
  }
}

type MaybePromise<T> = T | Promise<T>;

type LockTimes = {
  createTime: Timestamp;
  updateTime: Timestamp;
};
