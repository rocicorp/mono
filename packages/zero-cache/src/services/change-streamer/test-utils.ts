import {Subscription} from '../../types/subscription.ts';
import type {ReplicatorMode} from '../replicator/replicator.ts';
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
    () => ({tag: 'status'}),
    {wsBatched: true, ...options},
  );
  if (caughtUp) {
    void subscriber.setCaughtUp();
  }

  return [subscriber, received, sub];
}
