import {describe, expect, test} from 'vitest';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {createSubscriber} from './test-utils.ts';

describe('change-streamer/subscriber', () => {
  const messages = new ReplicationMessages({issues: 'id'});

  test('catchup and backlog', () => {
    const [sub, stream] = createSubscriber('00');

    // Send some messages while it is catching up.
    sub.send(['11', ['begin', messages.begin(), {commitWatermark: '12'}]]);
    sub.send(['12', ['commit', messages.commit(), {watermark: '12'}]]);

    // Send catchup messages.
    sub.catchup(['01', ['begin', messages.begin(), {commitWatermark: '02'}]]);
    sub.catchup(['02', ['commit', messages.commit(), {watermark: '02'}]]);

    sub.setCaughtUp();

    // Send some messages after catchup.
    sub.send(['21', ['begin', messages.begin(), {commitWatermark: '22'}]]);
    sub.send(['22', ['commit', messages.commit(), {watermark: '22'}]]);

    sub.close();

    expect(stream).toMatchInlineSnapshot(`
      [
        [
          "status",
          {
            "tag": "status",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "02",
          },
        ],
        [
          "commit",
          {
            "tag": "commit",
          },
          {
            "watermark": "02",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "12",
          },
        ],
        [
          "commit",
          {
            "tag": "commit",
          },
          {
            "watermark": "12",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "22",
          },
        ],
        [
          "commit",
          {
            "tag": "commit",
          },
          {
            "watermark": "22",
          },
        ],
      ]
    `);
  });

  test('watermark filtering', () => {
    const [sub, stream] = createSubscriber('123');

    // Technically, catchup should never send any messages if the subscriber
    // is ahead, since the watermark query would return no results. But pretend it
    // does just to ensure that catchup messages are subject to the filter.
    sub.catchup(['01', ['begin', messages.begin(), {commitWatermark: '02'}]]);
    sub.catchup(['02', ['commit', messages.commit(), {watermark: '02'}]]);
    sub.setCaughtUp();

    // Still lower than the watermark ...
    sub.send(['121', ['begin', messages.begin(), {commitWatermark: '123'}]]);
    sub.send(['123', ['commit', messages.commit(), {watermark: '123'}]]);

    // These should be sent.
    sub.send(['124', ['begin', messages.begin(), {commitWatermark: '125'}]]);
    sub.send(['125', ['commit', messages.commit(), {watermark: '125'}]]);

    // Replays should be ignored.
    sub.send(['124', ['begin', messages.begin(), {commitWatermark: '125'}]]);
    sub.send(['125', ['commit', messages.commit(), {watermark: '125'}]]);

    sub.close();
    expect(stream).toMatchInlineSnapshot(`
      [
        [
          "status",
          {
            "tag": "status",
          },
        ],
        [
          "begin",
          {
            "tag": "begin",
          },
          {
            "commitWatermark": "125",
          },
        ],
        [
          "commit",
          {
            "tag": "commit",
          },
          {
            "watermark": "125",
          },
        ],
      ]
    `);
  });

  test('ack tracking', async () => {
    const [sub, _, receiver] = createSubscriber('00');

    // Send some messages while it is catching up.
    sub.send(['11', ['begin', messages.begin(), {commitWatermark: '12'}]]);
    sub.send(['12', ['commit', messages.commit(), {watermark: '12'}]]);

    // Send catchup messages.
    sub.catchup(['01', ['begin', messages.begin(), {commitWatermark: '02'}]]);
    sub.catchup(['02', ['commit', messages.commit(), {watermark: '02'}]]);

    sub.setCaughtUp();

    // Send some messages after catchup.
    sub.send(['21', ['begin', messages.begin(), {commitWatermark: '22'}]]);
    sub.send(['22', ['commit', messages.commit(), {watermark: '22'}]]);

    sub.send(['31', ['begin', messages.begin(), {commitWatermark: '31'}]]);

    expect(sub.acked).toBe('00');

    let txNum = 0;
    for await (const msg of receiver) {
      if (msg[0] === 'begin') {
        txNum++;
      }
      switch (txNum) {
        case 1:
          expect(sub.acked).toBe('00');
          break;
        case 2:
          expect(sub.acked).toBe('02');
          break;
        case 3:
          expect(sub.acked).toBe('12');
          break;
        case 4:
          expect(sub.acked).toBe('22');
          sub.close();
          break;
      }
    }
  });
});
