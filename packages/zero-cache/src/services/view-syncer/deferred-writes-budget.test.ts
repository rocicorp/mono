import {getHeapStatistics} from 'node:v8';
import {describe, expect, test} from 'vitest';
import {BYTES_PER_ROW, DeferredWritesBudget} from './deferred-writes-budget.ts';

describe('view-syncer/deferred-writes-budget', () => {
  test('reserves rows while they fit', () => {
    const budget = new DeferredWritesBudget(10, Infinity);
    expect(budget.tryReserve(6)).toBe(true);
    expect(budget.tryReserve(5)).toBe(false);
    expect(budget.tryReserve(4)).toBe(true);
    expect(budget.reservedRows).toBe(10);

    budget.release(6, 0);
    expect(budget.reservedRows).toBe(4);
    expect(() => budget.release(5, 0)).toThrow(
      'Releasing 5 rows but only 4 are reserved',
    );
  });

  test('reserves more rows from the same pool', () => {
    const budget = new DeferredWritesBudget(3, Infinity);
    expect(budget.tryReserve(2)).toBe(true);
    expect(budget.tryReserveMore(1)).toBe(true);
    expect(budget.tryReserveMore(1)).toBe(false);
    expect(budget.reservedRows).toBe(3);
    budget.release(3, 0);
    expect(budget.reservedRows).toBe(0);
  });

  test('holds bytes until they are released', () => {
    const budget = new DeferredWritesBudget(Infinity, 100);
    expect(budget.tryReserve(1)).toBe(true);
    expect(budget.holdBytes(60)).toBe(true);
    expect(budget.tryReserve(1)).toBe(true);
    expect(budget.holdBytes(40)).toBe(true);
    expect(budget.holdBytes(1)).toBe(false);
    expect(budget.heldBytes).toBe(101);
    expect(budget.holdBytes(-11)).toBe(true);

    budget.release(1, 60);
    expect([budget.reservedRows, budget.heldBytes]).toEqual([1, 30]);
    expect(() => budget.release(1, 31)).toThrow(
      'Releasing 31 bytes but only 30 are held',
    );
    budget.release(1, 30);
    expect([budget.reservedRows, budget.heldBytes]).toEqual([0, 0]);
  });

  test('a budget of bytes holds the rows that fit at the assumed width', () => {
    const budget = DeferredWritesBudget.forBytes(2 * BYTES_PER_ROW + 1);
    expect(budget.maxRows).toBe(2);
    expect(budget.maxBytes).toBe(2 * BYTES_PER_ROW + 1);
    expect(budget.tryReserve(3)).toBe(false);
    expect(budget.tryReserve(2)).toBe(true);
  });

  test('a budget of a share of the heap', () => {
    const {heap_size_limit: limit} = getHeapStatistics();
    const budget = DeferredWritesBudget.forHeapProportion(0.25);
    expect(budget.maxBytes).toBe(Math.floor(limit * 0.25));
    expect(budget.maxRows).toBe(
      Math.floor(Math.floor(limit * 0.25) / BYTES_PER_ROW),
    );
  });
});
