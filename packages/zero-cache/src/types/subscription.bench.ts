import {assert} from '../../../shared/src/asserts.ts';
import {bench, describe} from '../../../shared/src/bench.ts';
import {Subscription} from './subscription.ts';

// These are local queue measurements. The preloaded cases deliberately bypass
// producer backpressure; the bounded cases keep the same subscription alive.
describe('Subscription', () => {
  for (const size of [1_000, 10_000, 20_000, 100_000]) {
    bench(`preload and drain ${size} messages`, () => run(size, size), {
      min_samples: 10,
    });
  }

  for (const window of [1, 64, 1_024]) {
    bench(
      `100000 messages in windows of ${window}`,
      () => run(100_000, window),
      {
        min_samples: 10,
      },
    );
  }

  bench('100000 messages to an awaiting consumer', async () => {
    const subscription = Subscription.create<number>();
    const iterator = subscription[Symbol.asyncIterator]();
    for (let i = 0; i < 100_000; i++) {
      const pending = iterator.next();
      subscription.push(i);
      const entry = await pending;
      assert(!entry.done && entry.value === i, 'expected FIFO delivery');
    }
    const pending = iterator.next();
    subscription.end();
    assert((await pending).done, 'expected end of subscription');
    assert(
      subscription.queued === 0 && subscription.consuming === 0,
      'expected an empty subscription',
    );
  });
});

async function run(total: number, window: number) {
  const subscription = Subscription.create<number>();
  const iterator = subscription[Symbol.asyncIterator]();

  for (let offset = 0; offset < total; offset += window) {
    const limit = Math.min(offset + window, total);
    for (let i = offset; i < limit; i++) {
      subscription.push(i);
    }
    if (limit === total) {
      subscription.end();
    }
    for (let i = offset; i < limit; i++) {
      const entry = await iterator.next();
      assert(!entry.done && entry.value === i, 'expected FIFO delivery');
    }
  }

  assert((await iterator.next()).done, 'expected end of subscription');
  assert(
    subscription.queued === 0 && subscription.consuming === 0,
    'expected an empty subscription',
  );
}
