import {Subscription} from '../../types/subscription.ts';
import {PROTOCOL_VERSION, type Downstream} from './change-streamer.ts';
import {Subscriber} from './subscriber.ts';

let nextID = 1;

export function createSubscriber(
  watermark = '00',
  caughtUp = false,
): [Subscriber, Downstream[], Subscription<string>] {
  const id = '' + nextID++;
  const received: Downstream[] = [];
  const sub = Subscription.create<string>({
    cleanup: unconsumed => received.push(...unconsumed.map(m => JSON.parse(m))),
  });
  const subscriber = new Subscriber(
    PROTOCOL_VERSION,
    id,
    watermark,
    sub,
    () => ({tag: 'status'}),
  );
  if (caughtUp) {
    void subscriber.setCaughtUp();
  }

  return [subscriber, received, sub];
}
