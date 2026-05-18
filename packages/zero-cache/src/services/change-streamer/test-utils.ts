import type {StringifiedStreamPayload} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {CHANGE_STREAMER_V6_PROTOCOL_VERSION} from './change-streamer-protocol.ts';
import {type Downstream} from './change-streamer.ts';
import {Subscriber} from './subscriber.ts';

let nextID = 1;

export function createSubscriber(
  watermark = '00',
  caughtUp = false,
  protocolVersion = CHANGE_STREAMER_V6_PROTOCOL_VERSION,
): [Subscriber, Downstream[], Subscription<StringifiedStreamPayload>] {
  const id = '' + nextID++;
  const received: Downstream[] = [];
  const sub = Subscription.create<StringifiedStreamPayload>({
    cleanup: unconsumed => received.push(...unconsumed.flatMap(parsePayload)),
  });
  const subscriber = new Subscriber(
    protocolVersion,
    id,
    watermark,
    sub,
    () => ({tag: 'status'}),
  );
  if (caughtUp) {
    subscriber.setCaughtUp();
  }

  return [subscriber, received, sub];
}

function parsePayload(payload: StringifiedStreamPayload): Downstream[] {
  return (typeof payload === 'string' ? [payload] : payload).flatMap(m => {
    const parsed = JSON.parse(m);
    return parsed[0] === 'change-batch' ? parsed[1].changes : [parsed];
  });
}
