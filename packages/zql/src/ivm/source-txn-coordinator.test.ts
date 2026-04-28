/* oxlint-disable @typescript-eslint/no-explicit-any */
import {describe, expect, test, vi} from 'vitest';
import {ChangeType} from './change-type.ts';
import {SourceTxnCoordinator} from './source-txn-coordinator.ts';
import type {Source, SourceTxnListener} from './source.ts';

function mockSource(): Source & {
  emitBegin: () => void;
  emitEnd: (t: ChangeType) => void;
} {
  const listeners = new Set<SourceTxnListener>();
  return {
    tableSchema: {} as any,
    connect: () => ({}) as any,
    push: () => ({}) as any,
    genPush: () => ({}) as any,
    addTxnListener(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    emitBegin() {
      for (const l of listeners) l.beginPush();
    },
    emitEnd(changeType) {
      for (const l of listeners) {
        // Drain the endPush stream synchronously.
        for (const _ of l.endPush(changeType)) {
          // no-op
        }
      }
    },
  };
}

describe('SourceTxnCoordinator', () => {
  test('forwards begin/end signals to the registered fan-in', () => {
    const fanIn = {
      fanOutStartedPushing: vi.fn(),
      fanOutDonePushing: vi.fn(function* (_t: ChangeType) {
        yield 'yield' as const;
      }),
    };
    const coordinator = new SourceTxnCoordinator();
    coordinator.setFanIn(fanIn);

    const source = mockSource();
    coordinator.attachSource(source);

    source.emitBegin();
    expect(fanIn.fanOutStartedPushing).toHaveBeenCalledTimes(1);

    source.emitEnd(ChangeType.ADD);
    expect(fanIn.fanOutDonePushing).toHaveBeenCalledWith(ChangeType.ADD);
  });

  test('attaches to multiple sources independently', () => {
    const fanIn = {
      fanOutStartedPushing: vi.fn(),
      fanOutDonePushing: vi.fn(function* () {}),
    };
    const coordinator = new SourceTxnCoordinator();
    coordinator.setFanIn(fanIn);

    const sourceA = mockSource();
    const sourceB = mockSource();
    coordinator.attachSource(sourceA);
    coordinator.attachSource(sourceB);

    sourceA.emitBegin();
    sourceA.emitEnd(ChangeType.ADD);
    sourceB.emitBegin();
    sourceB.emitEnd(ChangeType.REMOVE);

    expect(fanIn.fanOutStartedPushing).toHaveBeenCalledTimes(2);
    expect(fanIn.fanOutDonePushing).toHaveBeenNthCalledWith(1, ChangeType.ADD);
    expect(fanIn.fanOutDonePushing).toHaveBeenNthCalledWith(
      2,
      ChangeType.REMOVE,
    );
  });

  test('destroy unsubscribes from all attached sources', () => {
    const fanIn = {
      fanOutStartedPushing: vi.fn(),
      fanOutDonePushing: vi.fn(function* () {}),
    };
    const coordinator = new SourceTxnCoordinator();
    coordinator.setFanIn(fanIn);

    const source = mockSource();
    coordinator.attachSource(source);

    coordinator.destroy();
    source.emitBegin();
    source.emitEnd(ChangeType.ADD);

    expect(fanIn.fanOutStartedPushing).not.toHaveBeenCalled();
    expect(fanIn.fanOutDonePushing).not.toHaveBeenCalled();
  });

  test('throws when a source emits begin before setFanIn is called', () => {
    const coordinator = new SourceTxnCoordinator();
    const source = mockSource();
    coordinator.attachSource(source);

    expect(() => source.emitBegin()).toThrow();
  });
});
