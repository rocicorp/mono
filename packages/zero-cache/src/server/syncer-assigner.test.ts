import {describe, expect, test} from 'vitest';
import {SyncerAssigner} from './syncer-assigner.ts';

function fill(assigner: SyncerAssigner, prefix: string, count: number) {
  for (let i = 0; i < count; i++) {
    assigner.assign(`${prefix}${i}`);
  }
}

describe('SyncerAssigner affinity', () => {
  test('sends groups with the same key to the same worker', () => {
    const assigner = new SyncerAssigner('task', 8);
    const {worker} = assigner.assign('server', 'alice');
    expect(assigner.assign('browser', 'alice').worker).toBe(worker);
    expect(assigner.assign('phone', 'alice').worker).toBe(worker);
  });

  test('still balances groups without a key or with other keys', () => {
    const assigner = new SyncerAssigner('task', 2);
    const a = assigner.assign('a', 'alice').worker;
    const b = assigner.assign('b', 'bob').worker;
    const c = assigner.assign('c').worker;
    expect(b).not.toBe(a);
    expect(
      [assigner.getWorkerLoad(0), assigner.getWorkerLoad(1)].sort(),
    ).toEqual([1, 2]);
    expect(c === a || c === b).toBe(true);
  });

  test('ignores affinity once the worker is too far above the least loaded', () => {
    const assigner = new SyncerAssigner('task', 2);
    const {worker} = assigner.assign('first', 'popular');
    const other = 1 - worker;

    const placed: number[] = [];
    for (let i = 0; i < 20; i++) {
      placed.push(assigner.assign(`g${i}`, 'popular').worker);
    }
    expect(placed).toContain(other);
    expect(
      Math.abs(assigner.getWorkerLoad(0) - assigner.getWorkerLoad(1)),
    ).toBeLessThanOrEqual(5);
  });

  test('allows proportionally more skew on busy workers', () => {
    const assigner = new SyncerAssigner('task', 2);
    fill(assigner, 'background', 200);
    expect(assigner.getWorkerLoad(0)).toBe(100);

    const {worker} = assigner.assign('first', 'popular');
    let stayed = 0;
    for (let i = 0; i < 10; i++) {
      if (assigner.assign(`g${i}`, 'popular').worker === worker) {
        stayed++;
      }
    }
    expect(stayed).toBe(10);
  });

  test('a reconnecting group keeps its worker whatever its key', () => {
    const assigner = new SyncerAssigner('task', 4);
    const {worker} = assigner.assign('group', 'alice');
    assigner.assign('other', 'bob');
    expect(assigner.assign('group', 'bob').worker).toBe(worker);
    expect(assigner.assign('group').worker).toBe(worker);
  });

  test('forgets a key once its last group on that worker is released', () => {
    const assigner = new SyncerAssigner('task', 2);
    const {worker} = assigner.assign('server', 'alice');
    assigner.assign('browser', 'alice');
    assigner.release('server', worker);
    fill(assigner, 'load', 1);

    // The browser group still anchors the key.
    expect(assigner.assign('phone', 'alice').worker).toBe(worker);

    assigner.release('browser', worker);
    assigner.release('phone', worker);
    fill(assigner, 'more', 4);
    const least =
      assigner.getWorkerLoad(0) <= assigner.getWorkerLoad(1) ? 0 : 1;
    expect(assigner.assign('new', 'alice').worker).toBe(least);
  });

  test('releasing an overflow group does not release the anchor', () => {
    const assigner = new SyncerAssigner('task', 2);
    const {worker} = assigner.assign('first', 'popular');
    const onAnchor: string[] = [];
    const overflow: string[] = [];
    for (let i = 0; overflow.length === 0 && i < 50; i++) {
      const placed = assigner.assign(`g${i}`, 'popular').worker;
      (placed === worker ? onAnchor : overflow).push(`g${i}`);
    }
    expect(overflow).toHaveLength(1);

    for (const group of onAnchor) {
      assigner.release(group, worker);
    }
    assigner.release(overflow[0], 1 - worker);

    // Only 'first' still holds the anchor, and the other worker is emptier.
    expect(assigner.getWorkerLoad(worker)).toBe(1);
    expect(assigner.getWorkerLoad(1 - worker)).toBe(0);
    expect(assigner.assign('after', 'popular').worker).toBe(worker);
  });

  /** Places groups for `key` until one lands off `worker`, and returns where. */
  function overflow(assigner: SyncerAssigner, key: string, worker: number) {
    for (let i = 0; i < 50; i++) {
      const placed = assigner.assign(`${key}-${i}`, key).worker;
      if (placed !== worker) {
        return placed;
      }
    }
    throw new Error('never overflowed');
  }

  test('sends overflow to a worker that already has the key', () => {
    const assigner = new SyncerAssigner('task', 3);
    const {worker} = assigner.assign('first', 'popular');
    const second = overflow(assigner, 'popular', worker);
    // A worker that has none of the key is emptier, but has nothing to share.
    expect(assigner.assign('next', 'popular').worker).toBe(second);
  });

  test('prefers the least loaded of the workers that have the key', () => {
    const assigner = new SyncerAssigner('task', 3);
    const {worker} = assigner.assign('first', 'popular');
    const second = overflow(assigner, 'popular', worker);
    const third = 3 - worker - second;
    assigner.assign('other', 'other');
    expect(assigner.getWorkerLoad(third)).toBe(1);
    // Both workers with the key are within the skew, and the second is emptier.
    expect(assigner.assign('next', 'popular').worker).toBe(second);
  });

  test('spreads a key over as few workers as the skew allows', () => {
    const assigner = new SyncerAssigner('task', 4);
    const workers = new Set<number>();
    for (let i = 0; i < 17; i++) {
      workers.add(assigner.assign(`g${i}`, 'popular').worker);
    }
    expect(workers.size).toBe(4);
    const loads = [0, 1, 2, 3].map(w => assigner.getWorkerLoad(w));
    // Each worker the key reaches fills to the skew before the next is used.
    expect(loads.toSorted((a, b) => b - a)).toEqual([5, 5, 5, 2]);
  });

  test('forgets a worker for a key once its last group there is released', () => {
    const assigner = new SyncerAssigner('task', 3);
    const {worker} = assigner.assign('first', 'popular');
    const second = overflow(assigner, 'popular', worker);
    const overflowed = `popular-${assigner.getWorkerLoad(worker) - 1}`;
    assigner.release(overflowed, second);
    expect(assigner.getWorkerLoad(second)).toBe(0);
    // The two empty workers are tied, so either may be picked: only the
    // anchor's worker is ruled out.
    expect(assigner.assign('next', 'popular').worker).not.toBe(worker);
  });

  test('destroy forgets every key', () => {
    const assigner = new SyncerAssigner('task', 2);
    const {worker} = assigner.assign('a', 'alice');
    assigner.destroy();

    // A load only the other worker can relieve shows the key no longer applies.
    const other = 1 - worker;
    const placed = assigner.assign('b').worker;
    if (placed === worker) {
      expect(assigner.assign('c', 'alice').worker).toBe(other);
    } else {
      assigner.assign('c');
      expect(assigner.getWorkerLoad(other)).toBeGreaterThan(0);
      expect(assigner.assign('d', 'alice').worker).toBe(worker);
    }
  });
});
