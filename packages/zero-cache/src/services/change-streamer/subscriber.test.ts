import {describe, expect, test} from 'vitest';
import {ReplicationMessages} from '../replicator/test-utils.js';
import {createSubscriber} from './test-utils.js';

describe('change-streamer/subscriber', () => {
  const messages = new ReplicationMessages({issues: 'id'});

  test('catchup and backlog', () => {
    const [sub, stream] = createSubscriber('00');

    // Send some messages while it is catching up.
    sub.send(['11', ['begin', messages.begin()]]);
    sub.send(['12', ['commit', messages.commit(), {watermark: '12'}]]);

    // Send catchup messages.
    sub.catchup(['01', ['begin', messages.begin()]]);
    sub.catchup(['02', ['commit', messages.commit(), {watermark: '02'}]]);

    sub.setCaughtUp();

    // Send some messages after catchup.
    sub.send(['21', ['begin', messages.begin()]]);
    sub.send(['22', ['commit', messages.commit(), {watermark: '22'}]]);

    sub.close();

    expect(stream).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "tag": "begin",
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
    sub.catchup(['01', ['begin', messages.begin()]]);
    sub.catchup(['02', ['commit', messages.commit(), {watermark: '02'}]]);
    sub.setCaughtUp();

    // Still lower than the watermark ...
    sub.send(['121', ['begin', messages.begin()]]);
    sub.send(['123', ['commit', messages.commit(), {watermark: '123'}]]);

    // These should be sent.
    sub.send(['124', ['begin', messages.begin()]]);
    sub.send(['125', ['commit', messages.commit(), {watermark: '125'}]]);

    // Replays should be ignored.
    sub.send(['124', ['begin', messages.begin()]]);
    sub.send(['125', ['commit', messages.commit(), {watermark: '125'}]]);

    sub.close();
    expect(stream).toMatchInlineSnapshot(`
      [
        [
          "begin",
          {
            "tag": "begin",
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
    sub.send(['11', ['begin', messages.begin()]]);
    sub.send(['12', ['commit', messages.commit(), {watermark: '12'}]]);

    // Send catchup messages.
    sub.catchup(['01', ['begin', messages.begin()]]);
    sub.catchup(['02', ['commit', messages.commit(), {watermark: '02'}]]);

    sub.setCaughtUp();

    // Send some messages after catchup.
    sub.send(['21', ['begin', messages.begin()]]);
    sub.send(['22', ['commit', messages.commit(), {watermark: '22'}]]);

    sub.send(['31', ['begin', messages.begin()]]);

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
