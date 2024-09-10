import {toLexiVersion} from 'zero-cache/src/types/lsn.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {Downstream} from './change-streamer.js';
import {Subscriber} from './subscriber.js';

let nextID = 1;

export function createSubscriber(
  watermark = '0/0',
  caughtUp = false,
): [Subscriber, Downstream[], Subscription<Downstream>] {
  // Sanity check the watermark.
  toLexiVersion(watermark);

  const id = '' + nextID++;
  const received: Downstream[] = [];
  const sub = Subscription.create<Downstream>({
    cleanup: unconsumed => received.push(...unconsumed),
  });
  const subscriber = new Subscriber(id, watermark, sub);
  if (caughtUp) {
    subscriber.setCaughtUp();
  }

  return [subscriber, received, sub];
}
