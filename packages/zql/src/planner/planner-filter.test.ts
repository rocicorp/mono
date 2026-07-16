import {expect, suite, test, vi} from 'vitest';
import type {SimpleCondition} from '../../../zero-protocol/src/ast.ts';
import {PlannerFanIn} from './planner-fan-in.ts';
import {PlannerFanOut} from './planner-fan-out.ts';
import {PlannerFilter} from './planner-filter.ts';
import {PlannerJoin} from './planner-join.ts';
import {CONSTRAINTS, createConnection} from './test/helpers.ts';

const cmpAEq1: SimpleCondition = {
  type: 'simple',
  op: '=',
  left: {type: 'column', name: 'a'},
  right: {type: 'literal', value: 1},
};

/**
 * Real PlannerFanIn used as the `from` argument, since PlannerFilter
 * reads both `from.kind` and `from.type` and PlannerNode is a discriminated
 * union of concrete classes (no structural-stub option).
 */
function makeFanIn(type: 'FI' | 'UFI'): PlannerFanIn {
  const f = new PlannerFanIn([createConnection('stub')]);
  if (type === 'UFI') f.convertToUFI();
  return f;
}

suite('PlannerFilter', () => {
  suite('propagateConstraints', () => {
    test('FI mode: does NOT register per-branch filter on the connection', () => {
      // FI shares one source scan across branches, so per-branch filters
      // would mis-credit the scan cost — registration must be suppressed.
      const conn = createConnection();
      const fanOut = new PlannerFanOut(conn);
      const filter = new PlannerFilter(fanOut, cmpAEq1);
      const spy = vi.spyOn(conn, 'setPerBranchFilter');

      filter.propagateConstraints([0], undefined, makeFanIn('FI'));

      expect(spy).not.toHaveBeenCalled();
    });

    test('UFI mode: registers per-branch filter with the propagated branchPattern', () => {
      const conn = createConnection();
      const fanOut = new PlannerFanOut(conn);
      const filter = new PlannerFilter(fanOut, cmpAEq1);
      const spy = vi.spyOn(conn, 'setPerBranchFilter');

      filter.propagateConstraints([2, 1], undefined, makeFanIn('UFI'));

      expect(spy).toHaveBeenCalledTimes(1);
      expect(spy).toHaveBeenCalledWith([2, 1], cmpAEq1);
    });

    test('UFI mode with no condition: does NOT register', () => {
      const conn = createConnection();
      const fanOut = new PlannerFanOut(conn);
      const filter = new PlannerFilter(fanOut, undefined);
      const spy = vi.spyOn(conn, 'setPerBranchFilter');

      filter.propagateConstraints([0], undefined, makeFanIn('UFI'));

      expect(spy).not.toHaveBeenCalled();
    });

    test('non-fan-in `from`: does NOT register (the UFI signal only comes from FanIn)', () => {
      const conn = createConnection();
      const fanOut = new PlannerFanOut(conn);
      const filter = new PlannerFilter(fanOut, cmpAEq1);
      const spy = vi.spyOn(conn, 'setPerBranchFilter');

      filter.propagateConstraints([0], undefined, /*from*/ undefined);

      expect(spy).not.toHaveBeenCalled();
    });

    test('UFI mode, chain through a PlannerJoin: walks join.parent to find the connection', () => {
      // connection → join (parent side) → fanOut → filter
      // findParentConnection should walk fanOut → join.parent → connection.
      const conn = createConnection('parent');
      const child = createConnection('child');
      const join = new PlannerJoin(
        conn,
        child,
        CONSTRAINTS.userId,
        CONSTRAINTS.id,
        false,
        0,
      );
      const fanOut = new PlannerFanOut(join);
      const filter = new PlannerFilter(fanOut, cmpAEq1);
      const spy = vi.spyOn(conn, 'setPerBranchFilter');

      filter.propagateConstraints([1], undefined, makeFanIn('UFI'));

      expect(spy).toHaveBeenCalledWith([1], cmpAEq1);
    });

    test('UFI mode, nested fan-in: bails (deeply nested OR carve-out)', () => {
      // outerFilter ← outerFanOut ← innerFanIn ← (something further down)
      // The connection further down is reachable in the graph, but
      // findParentConnection stops at the inner fan-in. So no registration.
      const conn = createConnection();
      const innerFanOut = new PlannerFanOut(conn);
      const innerBranch = new PlannerFilter(innerFanOut, undefined);
      const innerFanIn = new PlannerFanIn([innerBranch]);
      const outerFanOut = new PlannerFanOut(innerFanIn);
      const outerFilter = new PlannerFilter(outerFanOut, cmpAEq1);
      const spy = vi.spyOn(conn, 'setPerBranchFilter');

      outerFilter.propagateConstraints([0], undefined, makeFanIn('UFI'));

      expect(spy).not.toHaveBeenCalled();
    });

    test('forwards the constraint down to the input (regardless of mode)', () => {
      // PlannerFilter is a pass-through for the standard constraint
      // propagation — independent of the FI/UFI per-branch filter logic.
      const conn = createConnection();
      const fanOut = new PlannerFanOut(conn);
      const filter = new PlannerFilter(fanOut, cmpAEq1);

      filter.propagateConstraints([0], CONSTRAINTS.userId, makeFanIn('FI'));

      expect(conn.getConstraintsForDebug()).toEqual({
        '0': CONSTRAINTS.userId,
      });
    });
  });

  suite('estimateCost', () => {
    test('passes through input cost unchanged', () => {
      const conn = createConnection();
      const fanOut = new PlannerFanOut(conn);
      const filter = new PlannerFilter(fanOut, cmpAEq1);

      const filterCost = filter.estimateCost(1, [0]);
      const inputCost = fanOut.estimateCost(1, [0]);
      expect(filterCost).toEqual(inputCost);
    });
  });
});
