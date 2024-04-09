import {expect, test} from 'vitest';
import {DifferenceStream} from './difference-stream.js';
import {Materialite} from '../materialite.js';
import {createPullMessage, createPullResponseMessage} from './message.js';
import {DebugOperator} from './operators/debug-operator.js';

type Elem = {x: number};
test('map', () => {
  const s = new DifferenceStream<Elem>();
  let expectRan = 0;
  s.map(x => ({
    x: x.x * 2,
  })).effect(x => {
    expectRan++;
    expect(x).toEqual({x: 4});
  });

  s.newData(1, [
    [{x: 2}, 1],
    [{x: 2}, 1],
    [{x: 2}, 1],
  ]);
  s.commit(1);

  expect(expectRan).toBe(3);
});

test('filter', () => {
  const s = new DifferenceStream<Elem>();
  let expectRan = 0;
  s.filter(x => x.x % 2 === 0).effect(x => {
    expectRan++;
    expect(x).toEqual({x: 2});
  });

  s.newData(1, [
    [{x: 1}, 1],
    [{x: 2}, 1],
    [{x: 3}, 1],
  ]);
  s.commit(1);

  expect(expectRan).toBe(1);
});

test('count', () => {
  const s = new DifferenceStream<Elem>();
  let expectRan = 0;
  let expectedCount = 3;
  s.count('count').effect((x, mult) => {
    if (mult > 0) {
      expectRan++;
      expect(x.count).toBe(expectedCount);
    }
  });

  s.newData(1, [
    [{x: 1}, 1],
    [{x: 2}, 1],
    [{x: 3}, 1],
  ]);
  s.commit(1);

  expect(expectRan).toBe(1);

  s.newData(2, [
    [{x: 1}, 1],
    [{x: 2}, 1],
    [{x: 3}, 1],
    [{x: 3}, 1],
  ]);
  expectedCount = 7;
  s.commit(2);

  expect(expectRan).toBe(2);
});

test('map, filter, linearCount', () => {
  const s = new DifferenceStream<Elem>();
  let expectRan = 0;
  let expectedCount = 1;
  s.map(x => ({
    x: x.x * 2,
  }))
    .filter(x => x.x % 2 === 0)
    .count('count')
    .effect((x, mult) => {
      if (mult > 0) {
        expectRan++;
        expect(x.count).toBe(expectedCount);
      }
    });

  s.newData(1, [[{x: 1}, 1]]);
  s.commit(1);

  expect(expectRan).toBe(1);

  s.newData(2, [
    [{x: 1}, 1],
    [{x: 2}, 1],
  ]);
  expectedCount = 3;
  s.commit(2);

  expect(expectRan).toBe(2);
});

test('cleaning up the only user of a stream cleans up the entire pipeline', () => {
  const materialite = new Materialite();
  const set = materialite.newSetSource<Elem>((l, r) => l.x - r.x);

  let notifyCount = 0;
  const final = set.stream
    .effect(_ => notifyCount++)
    .effect(_ => notifyCount++)
    .effect(_ => notifyCount++);

  set.add({x: 1});
  expect(notifyCount).toBe(3);
  final.destroy();
  set.add({x: 2});
  // stream was cleaned up, all the way to the root
  // so no more notifications.
  expect(notifyCount).toBe(3);
});

test('cleaning up the only user of a stream cleans up the entire pipeline but stops at a used fork', () => {
  const materialite = new Materialite();
  const set = materialite.newSetSource<Elem>((l, r) => l.x - r.x);

  let notifyCount = 0;
  const stream1 = set.stream.effect(_ => notifyCount++);
  const stream2 = stream1.effect(_ => notifyCount++);
  const stream3 = stream1.effect(_ => notifyCount++);
  // Forked stream which creates this graph:
  /*
      stream1
      /      \
  stream2   stream3
  */

  set.add({x: 1});
  expect(notifyCount).toBe(3);
  stream3.destroy();
  set.add({x: 2});
  // stream was cleaned up to fork, so still 2 notification
  expect(notifyCount).toBe(5);
  stream2.destroy();
  set.add({x: 3});
  // stream was cleaned up, all the way to the root
  // so no more notifications.
  expect(notifyCount).toBe(5);
});

test('adding data runs the operator', () => {
  let ran = false;
  const stream = new DifferenceStream();
  stream.debug((_, _data) => {
    ran = true;
  });
  expect(ran).toBe(false);
  stream.newData(1, []);
  expect(ran).toBe(true);
});

test('commit notifies the operator', () => {
  let ran = false;
  const stream = new DifferenceStream();
  stream.effect(() => {
    ran = true;
  });
  stream.newData(1, [[{}, 1]]);
  expect(ran).toBe(false);
  stream.commit(1);
  expect(ran).toBe(true);
});

test('replying to a message only notifies along the requesting path', () => {
  /*
  Graph:
       s0
     / | \
    d  d  d
    |  |  |
    d  d  d
  */

  const stream = new DifferenceStream();
  const notified: number[] = [];

  const s1 = stream.debug(() => notified.push(1));
  const s2 = stream.debug(() => notified.push(2));
  const s3 = stream.debug(() => notified.push(3));

  s1.debug(() => notified.push(4));
  const x = new DifferenceStream();
  const s2Dbg = new DebugOperator(s2, x, () => notified.push(5));
  x.setUpstream(s2Dbg);
  s3.debug(() => notified.push(6));

  const msg = createPullMessage([[], 'asc'], 'select');

  s2Dbg.messageUpstream(msg);

  expect(notified).toEqual([]);

  stream.newData(1, [], createPullResponseMessage(msg));

  expect(notified).toEqual([2, 5]);
});
