import {Subscription} from '../../types/subscription.ts';
import type {Downstream} from './change-streamer.ts';
import {Subscriber} from './subscriber.ts';

let nextID = 1;

export function createSubscriber(
  watermark = '00',
  caughtUp = false,
): [Subscriber, Downstream[], Subscription<Downstream>] {
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
