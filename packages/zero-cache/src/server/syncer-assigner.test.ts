import {describe, expect, test} from 'vitest';
import {SyncerAssigner} from './syncer-assigner.ts';

describe('SyncerAssigner', () => {
  test('sticky routing for the same client group', () => {
    const assigner = new SyncerAssigner('task-1', 4, 30_000, false);
    const worker1 = assigner.assign('cg-1');
    assigner.confirm('cg-1', worker1);

    // Repeated calls return the same worker
    for (let i = 0; i < 10; i++) {
      expect(assigner.assign('cg-1')).toBe(worker1);
    }
    expect(assigner.getWorkerLoad(worker1)).toBe(1);
  });

  test('sticky routing for unconfirmed assignment within timeout window', () => {
    const assigner = new SyncerAssigner('task-1', 4, 30_000, false);
    const worker1 = assigner.assign('cg-1', 1000);

    // Within timeout window, stays sticky even if unconfirmed
    expect(assigner.assign('cg-1', 5000)).toBe(worker1);
    expect(assigner.getWorkerLoad(worker1)).toBe(1);
  });

  test('unconfirmed assignment expires lazily on re-assign after timeout', () => {
    const assigner = new SyncerAssigner('task-1', 4, 30_000, false);
    const worker1 = assigner.assign('cg-1', 1000);
    expect(assigner.getWorkerLoad(worker1)).toBe(1);

    // After timeout, next assign call for cg-1 treats it as new and re-assigns
    const worker2 = assigner.assign('cg-1', 32_000);
    expect(assigner.getWorkerLoad(worker1)).toBe(1); // since cg-1 re-assigned to least-loaded (worker1 load decremented, then re-incremented or placed on another)
    expect(assigner.getWorkerLoad(worker2)).toBe(1);
  });

  test('confirmed assignment does not expire after timeout', () => {
    const assigner = new SyncerAssigner('task-1', 4, 30_000, false);
    const worker1 = assigner.assign('cg-1', 1000);
    assigner.confirm('cg-1', worker1);

    // Way past timeout window, still sticky
    expect(assigner.assign('cg-1', 100_000)).toBe(worker1);
    expect(assigner.getWorkerLoad(worker1)).toBe(1);
  });

  test('least-loaded distribution across workers', () => {
    const assigner = new SyncerAssigner('task-1', 4, 30_000, false);
    const numGroups = 120;

    for (let i = 0; i < numGroups; i++) {
      const worker = assigner.assign(`cg-${i}`);
      assigner.confirm(`cg-${i}`, worker);
    }

    // With 120 groups across 4 workers, each worker must have exactly 30
    for (let w = 0; w < 4; w++) {
      expect(assigner.getWorkerLoad(w)).toBe(30);
    }
  });

  test('dynamic re-balancing when client groups are released', () => {
    const assigner = new SyncerAssigner('task-1', 4, 30_000, false);
    const assignments = new Map<string, number>();

    // Assign 40 groups (10 per worker)
    for (let i = 0; i < 40; i++) {
      const id = `cg-${i}`;
      const worker = assigner.assign(id);
      assigner.confirm(id, worker);
      assignments.set(id, worker);
    }

    for (let w = 0; w < 4; w++) {
      expect(assigner.getWorkerLoad(w)).toBe(10);
    }

    // Release all groups assigned to worker 0
    let releasedCount = 0;
    for (const [id, worker] of assignments) {
      if (worker === 0) {
        assigner.release(id, worker);
        releasedCount++;
      }
    }
    expect(releasedCount).toBe(10);
    expect(assigner.getWorkerLoad(0)).toBe(0);

    // The next 10 new client groups should all be assigned to worker 0
    for (let i = 0; i < 10; i++) {
      const id = `new-cg-${i}`;
      const worker = assigner.assign(id);
      expect(worker).toBe(0);
      assigner.confirm(id, worker);
    }

    for (let w = 0; w < 4; w++) {
      expect(assigner.getWorkerLoad(w)).toBe(10);
    }
  });

  test('sweepExpired cleans up unconfirmed assignments', () => {
    const assigner = new SyncerAssigner('task-1', 4, 30_000, false);
    const w1 = assigner.assign('cg-1', 1000);
    const w2 = assigner.assign('cg-2', 1000);
    assigner.confirm('cg-1', w1); // cg-1 confirmed

    expect(assigner.getWorkerLoad(w1)).toBe(1);
    expect(assigner.getWorkerLoad(w2)).toBe(1);

    // Sweep before expiration: no change
    assigner.sweepExpired(20_000);
    expect(assigner.getWorkerLoad(w2)).toBe(1);

    // Sweep after expiration: unconfirmed cg-2 is cleared, confirmed cg-1 remains
    assigner.sweepExpired(32_000);
    expect(assigner.getWorkerLoad(w2)).toBe(0);
    expect(assigner.getWorkerLoad(w1)).toBe(1);
  });

  test('workerCrashed clears all assignments for that worker', () => {
    const assigner = new SyncerAssigner('task-1', 4, 30_000, false);
    for (let i = 0; i < 20; i++) {
      const id = `cg-${i}`;
      const w = assigner.assign(id);
      assigner.confirm(id, w);
    }

    const worker1LoadBefore = assigner.getWorkerLoad(1);
    expect(worker1LoadBefore).toBeGreaterThan(0);

    assigner.workerCrashed(1);
    expect(assigner.getWorkerLoad(1)).toBe(0);
  });

  test('destroy clears sweep timer', () => {
    const assigner = new SyncerAssigner('task-1', 4, 30_000, true);
    expect(() => assigner.destroy()).not.toThrow();
  });
});
