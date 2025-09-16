/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {Subscription} from '../../types/subscription.ts';
import {PROTOCOL_VERSION, type Downstream} from './change-streamer.ts';
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
  const subscriber = new Subscriber(PROTOCOL_VERSION, id, watermark, sub);
  if (caughtUp) {
    subscriber.setCaughtUp();
  }

  return [subscriber, received, sub];
}
