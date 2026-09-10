import {expect, test, describe} from 'vitest';
import {
  drainPull,
  drainPullMap,
  filterPull,
  forEachPull,
  limitedScan,
  mapPull,
  takeWhilePull,
  withPull,
  type PullStream,
} from './stream.ts';

/** A stream that records whether it was closed, and how often. */
function tracked<T>(values: readonly T[]) {
  let i = 0;
  const state = {
    closes: 0,
    get closed() {
      return state.closes > 0;
    },
  };
  const stream: PullStream<T> = {
    next: () => (i < values.length ? values[i++] : undefined),
    close: () => {
      state.closes++;
    },
  };
  return {stream, state};
}

const boom = new Error('boom');
const throwing = () => {
  throw boom;
};

describe('withPull', () => {
  test('closes on the normal path and returns the value', () => {
    const {stream, state} = tracked([1, 2]);
    expect(withPull(stream, s => s.next())).toBe(1);
    expect(state.closes).toBe(1);
  });

  test('closes when fn throws, and raises fn’s error', () => {
    const {stream, state} = tracked([1]);
    expect(() => withPull(stream, throwing)).toThrow(boom);
    expect(state.closes).toBe(1);
  });

  test('a failing close does not mask the original error', () => {
    const stream: PullStream<number> = {
      next: () => undefined,
      close: () => {
        throw new Error('close failed');
      },
    };
    expect(() => withPull(stream, throwing)).toThrow(boom);
  });
});

describe('forEachPull', () => {
  test('visits every value then closes', () => {
    const {stream, state} = tracked([1, 2, 3]);
    const seen: number[] = [];
    forEachPull(stream, v => seen.push(v));
    expect(seen).toEqual([1, 2, 3]);
    expect(state.closes).toBe(1);
  });

  test('closes when the body throws', () => {
    const {stream, state} = tracked([1, 2, 3]);
    expect(() => forEachPull(stream, throwing)).toThrow(boom);
    expect(state.closes).toBe(1);
  });
});

describe('drains own their stream', () => {
  test('drainPull closes after a full read', () => {
    const {stream, state} = tracked([1, 2]);
    expect(drainPull(stream)).toEqual([1, 2]);
    expect(state.closes).toBe(1);
  });

  test('drainPullMap closes when map throws', () => {
    const {stream, state} = tracked([1, 2]);
    expect(() => drainPullMap(stream, throwing)).toThrow(boom);
    expect(state.closes).toBe(1);
  });
});

describe('combinators release the source when a callback throws', () => {
  test('filterPull', () => {
    const {stream, state} = tracked([1, 2]);
    expect(() => filterPull(stream, throwing).next()).toThrow(boom);
    expect(state.closed).toBe(true);
  });

  test('takeWhilePull', () => {
    const {stream, state} = tracked([1, 2]);
    expect(() => takeWhilePull(stream, throwing).next()).toThrow(boom);
    expect(state.closed).toBe(true);
  });

  test('mapPull', () => {
    const {stream, state} = tracked([1, 2]);
    expect(() => mapPull(stream, throwing).next()).toThrow(boom);
    expect(state.closed).toBe(true);
  });
});

test('limitedScan releases the source when the scan throws', () => {
  let closed = false;
  const stream: PullStream<number> = {
    next: throwing,
    close: () => {
      closed = true;
    },
  };
  const scan = limitedScan(
    stream,
    10,
    () => true,
    () => {},
    () => {},
    () => {},
  );
  expect(() => scan.next()).toThrow(boom);
  // The caller's close() sees `done` and skips the source, so the scan itself
  // must have released it -- this is the shape of the FilterStartPull bug.
  scan.close();
  expect(closed).toBe(true);
});

describe('forEachPull early exit', () => {
  test("'break' stops the scan and still closes", () => {
    const {stream, state} = tracked([1, 2, 3]);
    const seen: number[] = [];
    forEachPull(stream, v => {
      seen.push(v);
      return v === 2 ? 'break' : undefined;
    });
    expect(seen).toEqual([1, 2]);
    expect(state.closes).toBe(1);
  });

  test('a shorthand arrow returning a value is not a break', () => {
    const {stream} = tracked([1, 2, 3]);
    const seen: number[] = [];
    // `push` returns a number; only the exact string 'break' stops the scan.
    forEachPull(stream, v => seen.push(v));
    expect(seen).toEqual([1, 2, 3]);
  });

  test('a return that is not exactly “break” does not stop the scan', () => {
    const {stream} = tracked([1, 2, 3]);
    const seen: number[] = [];
    forEachPull(stream, v => {
      seen.push(v);
      return 'brake';
    });
    expect(seen).toEqual([1, 2, 3]);
  });
});
