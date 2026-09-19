import {describe, expect, test} from 'vitest';
import {SyncerAssigner} from './syncer-assigner.ts';

describe('SyncerAssigner', () => {
  test('sticky routing for the same client group', () => {
    const assigner = new SyncerAssigner('task-1', 4);
    const worker1 = assigner.assign('cg-1');

    // Repeated calls return the same worker
    for (let i = 0; i < 10; i++) {
      expect(assigner.assign('cg-1')).toBe(worker1);
    }
    // Load count is only incremented once per unique client group
    expect(assigner.getWorkerLoad(worker1)).toBe(1);
    expect(assigner.getAssignment('cg-1')).toBe(worker1);
  });

  test('least-loaded distribution across workers', () => {
    const assigner = new SyncerAssigner('task-1', 4);
    const numGroups = 120;

    for (let i = 0; i < numGroups; i++) {
      assigner.assign(`cg-${i}`);
    }

    // With 120 groups across 4 workers, each worker must have exactly 30
    for (let w = 0; w < 4; w++) {
      expect(assigner.getWorkerLoad(w)).toBe(30);
    }
  });

  test('dynamic re-balancing when client groups are released', () => {
    const assigner = new SyncerAssigner('task-1', 4);
    const assignments = new Map<string, number>();

    // Assign 40 groups (10 per worker)
    for (let i = 0; i < 40; i++) {
      const id = `cg-${i}`;
      const worker = assigner.assign(id);
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
    }

    for (let w = 0; w < 4; w++) {
      expect(assigner.getWorkerLoad(w)).toBe(10);
    }
  });

  test('release only applies if worker index matches', () => {
    const assigner = new SyncerAssigner('task-1', 4);
    const worker = assigner.assign('cg-1');
    const wrongWorker = (worker + 1) % 4;

    assigner.release('cg-1', wrongWorker);
    expect(assigner.getAssignment('cg-1')).toBe(worker);
    expect(assigner.getWorkerLoad(worker)).toBe(1);

    assigner.release('cg-1', worker);
    expect(assigner.getAssignment('cg-1')).toBeUndefined();
    expect(assigner.getWorkerLoad(worker)).toBe(0);
  });

  test('handles 0 or 1 worker gracefully', () => {
    const zeroWorkers = new SyncerAssigner('task-1', 0);
    expect(zeroWorkers.assign('cg-1')).toBe(0);
    expect(zeroWorkers.getWorkerLoad(0)).toBe(0);

    const oneWorker = new SyncerAssigner('task-1', 1);
    expect(oneWorker.assign('cg-1')).toBe(0);
    expect(oneWorker.assign('cg-2')).toBe(0);
    expect(oneWorker.getWorkerLoad(0)).toBe(2);
    oneWorker.release('cg-1', 0);
    expect(oneWorker.getWorkerLoad(0)).toBe(1);
  });

  test('destroy clears all assignments', () => {
    const assigner = new SyncerAssigner('task-1', 4);
    assigner.assign('cg-1');
    assigner.assign('cg-2');
    expect(assigner.getAssignment('cg-1')).toBeDefined();
    assigner.destroy();
    expect(assigner.getAssignment('cg-1')).toBeUndefined();
  });
});
