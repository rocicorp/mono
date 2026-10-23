import {Subscription} from '../../types/subscription.ts';
import type {ReplicatorMode} from '../replicator/replicator.ts';
import type {
  BackedUpWatermark,
  BackupMonitor,
  BackupSource,
} from './backup-monitor.ts';
import type {PreSerializedBatch} from './broadcast.ts';
import {PROTOCOL_VERSION, type Downstream} from './change-streamer.ts';
import {Subscriber, type SubscriberOptions} from './subscriber.ts';

let nextID = 1;

export function createSubscriber(
  watermark = '00',
  caughtUp = false,
  options: SubscriberOptions = {},
  mode: ReplicatorMode = 'serving',
): [Subscriber, Downstream[], Subscription<string | PreSerializedBatch>] {
  const id = '' + nextID++;
  const received: Downstream[] = [];
  const sub = Subscription.create<string | PreSerializedBatch>({
    cleanup: unconsumed => {
      for (const m of unconsumed) {
        if (typeof m === 'string') {
          received.push(JSON.parse(m));
        } else {
          for (const c of m.changes) {
            received.push(JSON.parse(c[2]));
          }
        }
      }
    },
  });
  const subscriber = new Subscriber(
    PROTOCOL_VERSION,
    id,
    mode,
    watermark,
    sub,
    {wsBatched: true, ...options},
  );
  if (caughtUp) {
    void subscriber.setCaughtUp();
  }

  return [subscriber, received, sub];
}

/**
 * Consumes the monitor's backups as the change-streamer does, passing each
 * watermark to `track`, and returns the monitor's run().
 */
export function consume(
  monitor: BackupMonitor,
  track: (watermark: string) => void,
) {
  void (async () => {
    for await (const {watermark} of monitor.backups()) {
      track(watermark);
    }
  })();
  return monitor.run();
}

/**
 * A {@link BackupSource} through which a test reports backed-up watermarks to
 * the change-streamer constructed with it (as its `backupMonitor`).
 */
export class FakeBackups implements BackupSource {
  readonly #backups = Subscription.create<BackedUpWatermark>({
    coalesce: curr => curr,
  });
  readonly syncRequests: string[] = [];

  backups() {
    return this.#backups;
  }

  requestSync(watermark: string) {
    this.syncRequests.push(watermark);
  }

  /**
   * Reports that the replica has been backed up through `watermark`, and
   * resolves once the change-streamer has tracked it, i.e. requested the next
   * one after processing it.
   */
  async track(watermark: string) {
    const result = await this.#backups.push({
      watermark,
      backupTimeMs: Date.now(),
    }).result;
    if (result !== 'consumed') {
      throw new Error(`backup watermark ${watermark} was ${result}`);
    }
  }
}
