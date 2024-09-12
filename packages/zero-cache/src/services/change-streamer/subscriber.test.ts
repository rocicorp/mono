import {describe, expect, test} from 'vitest';
import {ReplicationMessages} from '../replicator/test-utils.js';
import {createSubscriber} from './test-utils.js';

describe('change-streamer/subscriber', () => {
  const messages = new ReplicationMessages({issues: 'id'});

  test('catchup and backlog', () => {
    const [sub, stream] = createSubscriber('00');

    // Send some messages while it is catching up.
    sub.send({watermark: '11', change: messages.begin()});
    sub.send({watermark: '12', change: messages.commit()});

    // Send catchup messages.
    sub.catchup({watermark: '01', change: messages.begin()});
    sub.catchup({watermark: '02', change: messages.commit()});

    sub.setCaughtUp();

    // Send some messages after catchup.
    sub.send({watermark: '21', change: messages.begin()});
    sub.send({watermark: '22', change: messages.commit()});

    sub.close();

    expect(stream).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "tag": "begin",
            },
            "watermark": "01",
          },
        ],
        [
          "change",
          {
            "change": {
              "tag": "commit",
            },
            "watermark": "02",
          },
        ],
        [
          "change",
          {
            "change": {
              "tag": "begin",
            },
            "watermark": "11",
          },
        ],
        [
          "change",
          {
            "change": {
              "tag": "commit",
            },
            "watermark": "12",
          },
        ],
        [
          "change",
          {
            "change": {
              "tag": "begin",
            },
            "watermark": "21",
          },
        ],
        [
          "change",
          {
            "change": {
              "tag": "commit",
            },
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
    sub.catchup({watermark: '01', change: messages.begin()});
    sub.catchup({watermark: '02', change: messages.begin()});
    sub.setCaughtUp();

    // Still lower than the watermark ...
    sub.send({watermark: '121', change: messages.begin()});
    sub.send({watermark: '123', change: messages.begin()});

    // These should be sent.
    sub.send({watermark: '124', change: messages.begin()});
    sub.send({watermark: '125', change: messages.begin()});

    // Replays should be ignored.
    sub.send({watermark: '124', change: messages.begin()});
    sub.send({watermark: '125', change: messages.begin()});

    sub.close();
    expect(stream).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "tag": "begin",
            },
            "watermark": "124",
          },
        ],
        [
          "change",
          {
            "change": {
              "tag": "begin",
            },
            "watermark": "125",
          },
        ],
      ]
    `);
  });
});
