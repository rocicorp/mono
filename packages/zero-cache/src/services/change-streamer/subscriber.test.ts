import {describe, expect, test} from 'vitest';
import {ReplicationMessages} from '../replicator/test-utils.js';
import {createSubscriber} from './test-utils.js';

describe('change-streamer/subscriber', () => {
  const messages = new ReplicationMessages({issues: 'id'});

  test('catchup and backlog', () => {
    const [sub, stream] = createSubscriber('00');

    // Send some messages while it is catching up.
    sub.send({watermark: '11', change: messages.begin('123')});
    sub.send({watermark: '12', change: messages.commit('124')});

    // Send catchup messages.
    sub.catchup({watermark: '01', change: messages.begin('012')});
    sub.catchup({watermark: '02', change: messages.commit('013')});

    sub.setCaughtUp();

    // Send some messages after catchup.
    sub.send({watermark: '21', change: messages.begin('321')});
    sub.send({watermark: '22', change: messages.commit('322')});

    sub.close();

    expect(stream).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "commitLsn": "012",
              "commitTime": 0n,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "01",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitEndLsn": "013",
              "commitLsn": null,
              "commitTime": 0n,
              "flags": 0,
              "tag": "commit",
            },
            "watermark": "02",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitLsn": "123",
              "commitTime": 0n,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "11",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitEndLsn": "124",
              "commitLsn": null,
              "commitTime": 0n,
              "flags": 0,
              "tag": "commit",
            },
            "watermark": "12",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitLsn": "321",
              "commitTime": 0n,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "21",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitEndLsn": "322",
              "commitLsn": null,
              "commitTime": 0n,
              "flags": 0,
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
    sub.catchup({watermark: '01', change: messages.begin('01')});
    sub.catchup({watermark: '02', change: messages.begin('02')});
    sub.setCaughtUp();

    // Still lower than the watermark ...
    sub.send({watermark: '121', change: messages.begin('12')});
    sub.send({watermark: '123', change: messages.begin('13')});

    // These should be sent.
    sub.send({watermark: '124', change: messages.begin('23')});
    sub.send({watermark: '125', change: messages.begin('24')});

    sub.close();
    expect(stream).toMatchInlineSnapshot(`
      [
        [
          "change",
          {
            "change": {
              "commitLsn": "23",
              "commitTime": 0n,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "124",
          },
        ],
        [
          "change",
          {
            "change": {
              "commitLsn": "24",
              "commitTime": 0n,
              "tag": "begin",
              "xid": 0,
            },
            "watermark": "125",
          },
        ],
      ]
    `);
  });
});
