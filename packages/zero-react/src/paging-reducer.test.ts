import {describe, expect, test} from 'vitest';
import {
  pagingReducer,
  type PagingAction,
  type PagingState,
} from './paging-reducer.ts';
import type {Anchor} from './use-rows.ts';

type TestListContextParams = {filter: string};
type TestStartRow = {id: string; createdAt: number};

function createInitialState(
  overrides?: Partial<PagingState<TestListContextParams, TestStartRow>>,
): PagingState<TestListContextParams, TestStartRow> {
  return {
    estimatedTotal: 100,
    hasReachedStart: false,
    hasReachedEnd: false,
    queryAnchor: {
      anchor: {
        index: 0,
        kind: 'forward',
        startRow: undefined,
      },
      listContextParams: {filter: 'default'},
    },
    pagingPhase: 'idle',
    pendingScrollAdjustment: 0,
    ...overrides,
  };
}

describe('pagingReducer', () => {
  describe('UPDATE_ESTIMATED_TOTAL', () => {
    test('updates estimatedTotal when newTotal is greater', () => {
      const state = createInitialState({estimatedTotal: 100});
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'UPDATE_ESTIMATED_TOTAL',
        newTotal: 150,
      };

      const result = pagingReducer(state, action);

      expect(result.estimatedTotal).toBe(150);
      expect(result).not.toBe(state); // immutability
    });

    test('keeps estimatedTotal when newTotal is smaller', () => {
      const state = createInitialState({estimatedTotal: 100});
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'UPDATE_ESTIMATED_TOTAL',
        newTotal: 50,
      };

      const result = pagingReducer(state, action);

      expect(result.estimatedTotal).toBe(100);
      expect(result).toBe(state); // Same object when no change
    });

    test('keeps estimatedTotal when newTotal is equal', () => {
      const state = createInitialState({estimatedTotal: 100});
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'UPDATE_ESTIMATED_TOTAL',
        newTotal: 100,
      };

      const result = pagingReducer(state, action);

      expect(result.estimatedTotal).toBe(100);
      expect(result).toBe(state); // Same object when no change
    });
  });

  describe('REACHED_START', () => {
    test('sets hasReachedStart to true', () => {
      const state = createInitialState({hasReachedStart: false});
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'REACHED_START',
      };

      const result = pagingReducer(state, action);

      expect(result.hasReachedStart).toBe(true);
      expect(result).not.toBe(state);
    });

    test('keeps hasReachedStart true if already true', () => {
      const state = createInitialState({hasReachedStart: true});
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'REACHED_START',
      };

      const result = pagingReducer(state, action);

      expect(result.hasReachedStart).toBe(true);
    });
  });

  describe('REACHED_END', () => {
    test('sets hasReachedEnd to true', () => {
      const state = createInitialState({hasReachedEnd: false});
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'REACHED_END',
      };

      const result = pagingReducer(state, action);

      expect(result.hasReachedEnd).toBe(true);
      expect(result).not.toBe(state);
    });

    test('keeps hasReachedEnd true if already true', () => {
      const state = createInitialState({hasReachedEnd: true});
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'REACHED_END',
      };

      const result = pagingReducer(state, action);

      expect(result.hasReachedEnd).toBe(true);
    });
  });

  describe('UPDATE_ANCHOR', () => {
    test('updates the anchor within queryAnchor', () => {
      const state = createInitialState();
      const newAnchor: Anchor<TestStartRow> = {
        index: 50,
        kind: 'forward',
        startRow: {id: 'row-50', createdAt: 1000},
      };
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'UPDATE_ANCHOR',
        anchor: newAnchor,
      };

      const result = pagingReducer(state, action);

      expect(result.queryAnchor.anchor).toBe(newAnchor);
      expect(result.queryAnchor.listContextParams).toBe(
        state.queryAnchor.listContextParams,
      );
      expect(result).not.toBe(state);
    });

    test('updates anchor to backward direction', () => {
      const state = createInitialState();
      const newAnchor: Anchor<TestStartRow> = {
        index: 75,
        kind: 'backward',
        startRow: {id: 'row-75', createdAt: 2000},
      };
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'UPDATE_ANCHOR',
        anchor: newAnchor,
      };

      const result = pagingReducer(state, action);

      expect(result.queryAnchor.anchor).toEqual(newAnchor);
      expect(result.queryAnchor.anchor.kind).toBe('backward');
    });

    test('updates anchor to permalink', () => {
      const state = createInitialState();
      const newAnchor = {
        id: 'permalink-123',
        index: 10,
        kind: 'permalink',
      } as const;
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'UPDATE_ANCHOR',
        anchor: newAnchor,
      };

      const result = pagingReducer(state, action);

      expect(result.queryAnchor.anchor).toEqual(newAnchor);
      expect(result.queryAnchor.anchor.kind).toBe('permalink');
    });
  });

  describe('SHIFT_ANCHOR_DOWN', () => {
    test('updates anchor, sets pendingScrollAdjustment, and sets phase to adjusting', () => {
      const state = createInitialState({
        pagingPhase: 'idle',
        pendingScrollAdjustment: 0,
      });
      const newAnchor: Anchor<TestStartRow> = {
        index: 60,
        kind: 'forward',
        startRow: {id: 'row-60', createdAt: 3000},
      };
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'SHIFT_ANCHOR_DOWN',
        offset: 10,
        newAnchor,
      };

      const result = pagingReducer(state, action);

      expect(result.queryAnchor.anchor).toBe(newAnchor);
      expect(result.pendingScrollAdjustment).toBe(10);
      expect(result.pagingPhase).toBe('adjusting');
      expect(result).not.toBe(state);
    });

    test('handles negative offset', () => {
      const state = createInitialState();
      const newAnchor: Anchor<TestStartRow> = {
        index: 40,
        kind: 'forward',
        startRow: {id: 'row-40', createdAt: 4000},
      };
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'SHIFT_ANCHOR_DOWN',
        offset: -5,
        newAnchor,
      };

      const result = pagingReducer(state, action);

      expect(result.pendingScrollAdjustment).toBe(-5);
      expect(result.pagingPhase).toBe('adjusting');
    });
  });

  describe('RESET_TO_TOP', () => {
    test('resets anchor to index 0 forward, sets scroll adjustment, and phase to adjusting', () => {
      const state = createInitialState({
        queryAnchor: {
          anchor: {
            index: 50,
            kind: 'backward',
            startRow: {id: 'row-50', createdAt: 5000},
          },
          listContextParams: {filter: 'test'},
        },
        pagingPhase: 'idle',
      });
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'RESET_TO_TOP',
        offset: -50,
      };

      const result = pagingReducer(state, action);

      expect(result.queryAnchor.anchor).toEqual({
        index: 0,
        kind: 'forward',
        startRow: undefined,
      });
      expect(result.pendingScrollAdjustment).toBe(-50);
      expect(result.pagingPhase).toBe('adjusting');
      expect(result.queryAnchor.listContextParams).toBe(
        state.queryAnchor.listContextParams,
      );
      expect(result).not.toBe(state);
    });

    test('handles positive offset', () => {
      const state = createInitialState();
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'RESET_TO_TOP',
        offset: 20,
      };

      const result = pagingReducer(state, action);

      expect(result.queryAnchor.anchor.index).toBe(0);
      expect(result.queryAnchor.anchor.kind).toBe('forward');
      if (result.queryAnchor.anchor.kind === 'forward') {
        expect(result.queryAnchor.anchor.startRow).toBeUndefined();
      }
      expect(result.pendingScrollAdjustment).toBe(20);
    });
  });

  describe('SCROLL_ADJUSTED', () => {
    test('updates estimatedTotal by adding pendingScrollAdjustment, clears adjustment, sets phase to skipping', () => {
      const state = createInitialState({
        estimatedTotal: 100,
        pendingScrollAdjustment: 10,
        pagingPhase: 'adjusting',
      });
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'SCROLL_ADJUSTED',
      };

      const result = pagingReducer(state, action);

      expect(result.estimatedTotal).toBe(110);
      expect(result.pendingScrollAdjustment).toBe(0);
      expect(result.pagingPhase).toBe('skipping');
      expect(result).not.toBe(state);
    });

    test('handles negative pendingScrollAdjustment', () => {
      const state = createInitialState({
        estimatedTotal: 100,
        pendingScrollAdjustment: -15,
        pagingPhase: 'adjusting',
      });
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'SCROLL_ADJUSTED',
      };

      const result = pagingReducer(state, action);

      expect(result.estimatedTotal).toBe(85);
      expect(result.pendingScrollAdjustment).toBe(0);
      expect(result.pagingPhase).toBe('skipping');
    });

    test('handles zero pendingScrollAdjustment', () => {
      const state = createInitialState({
        estimatedTotal: 100,
        pendingScrollAdjustment: 0,
        pagingPhase: 'adjusting',
      });
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'SCROLL_ADJUSTED',
      };

      const result = pagingReducer(state, action);

      expect(result.estimatedTotal).toBe(100);
      expect(result.pendingScrollAdjustment).toBe(0);
      expect(result.pagingPhase).toBe('skipping');
    });
  });

  describe('PAGING_COMPLETE', () => {
    test('sets pagingPhase to idle', () => {
      const state = createInitialState({pagingPhase: 'skipping'});
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'PAGING_COMPLETE',
      };

      const result = pagingReducer(state, action);

      expect(result.pagingPhase).toBe('idle');
      expect(result).not.toBe(state);
    });

    test('keeps pagingPhase idle if already idle', () => {
      const state = createInitialState({pagingPhase: 'idle'});
      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'PAGING_COMPLETE',
      };

      const result = pagingReducer(state, action);

      expect(result.pagingPhase).toBe('idle');
    });
  });

  describe('RESET_STATE', () => {
    test('resets all state fields atomically', () => {
      const state = createInitialState({
        estimatedTotal: 200,
        hasReachedStart: true,
        hasReachedEnd: true,
        queryAnchor: {
          anchor: {
            index: 50,
            kind: 'backward',
            startRow: {id: 'old-row', createdAt: 1000},
          },
          listContextParams: {filter: 'old'},
        },
        pagingPhase: 'idle',
      });

      const newAnchor: Anchor<TestStartRow> = {
        index: 10,
        kind: 'forward',
        startRow: {id: 'new-row', createdAt: 2000},
      };
      const newListContextParams = {filter: 'new'};

      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'RESET_STATE',
        estimatedTotal: 150,
        hasReachedStart: false,
        hasReachedEnd: false,
        anchor: newAnchor,
        listContextParams: newListContextParams,
      };

      const result = pagingReducer(state, action);

      expect(result.estimatedTotal).toBe(150);
      expect(result.hasReachedStart).toBe(false);
      expect(result.hasReachedEnd).toBe(false);
      expect(result.queryAnchor.anchor).toBe(newAnchor);
      expect(result.queryAnchor.listContextParams).toBe(newListContextParams);
      expect(result.pagingPhase).toBe('skipping');
      expect(result).not.toBe(state);
    });

    test('resets to top anchor with permalink context', () => {
      const state = createInitialState();
      const permalinkAnchor = {
        id: 'item-123',
        index: 1,
        kind: 'permalink',
      } as const;
      const newListContextParams = {filter: 'permalink'};

      const action: PagingAction<TestListContextParams, TestStartRow> = {
        type: 'RESET_STATE',
        estimatedTotal: 1,
        hasReachedStart: false,
        hasReachedEnd: false,
        anchor: permalinkAnchor,
        listContextParams: newListContextParams,
      };

      const result = pagingReducer(state, action);

      expect(result.queryAnchor.anchor).toEqual(permalinkAnchor);
      expect(result.estimatedTotal).toBe(1);
    });
  });

  describe('state machine transitions', () => {
    test('SHIFT_ANCHOR_DOWN -> SCROLL_ADJUSTED -> PAGING_COMPLETE', () => {
      let state: PagingState<TestListContextParams, TestStartRow | undefined> =
        createInitialState({
          estimatedTotal: 100,
          pagingPhase: 'idle',
          pendingScrollAdjustment: 0,
        });

      const newAnchor: Anchor<TestStartRow> = {
        index: 60,
        kind: 'forward',
        startRow: {id: 'row-60', createdAt: 3000},
      };

      // Step 1: SHIFT_ANCHOR_DOWN
      state = pagingReducer(state, {
        type: 'SHIFT_ANCHOR_DOWN',
        offset: 10,
        newAnchor,
      });

      expect(state.pagingPhase).toBe('adjusting');
      expect(state.pendingScrollAdjustment).toBe(10);
      expect(state.estimatedTotal).toBe(100);

      // Step 2: SCROLL_ADJUSTED
      state = pagingReducer(state, {
        type: 'SCROLL_ADJUSTED',
      });

      expect(state.pagingPhase).toBe('skipping');
      expect(state.pendingScrollAdjustment).toBe(0);
      expect(state.estimatedTotal).toBe(110);

      // Step 3: PAGING_COMPLETE
      state = pagingReducer(state, {
        type: 'PAGING_COMPLETE',
      });

      expect(state.pagingPhase).toBe('idle');
      expect(state.estimatedTotal).toBe(110);
    });

    test('RESET_TO_TOP -> SCROLL_ADJUSTED -> PAGING_COMPLETE', () => {
      let state = createInitialState({
        estimatedTotal: 100,
        pagingPhase: 'idle',
        pendingScrollAdjustment: 0,
      });

      // Step 1: RESET_TO_TOP
      state = pagingReducer(state, {
        type: 'RESET_TO_TOP',
        offset: -50,
      });

      expect(state.pagingPhase).toBe('adjusting');
      expect(state.pendingScrollAdjustment).toBe(-50);
      expect(state.queryAnchor.anchor.index).toBe(0);

      // Step 2: SCROLL_ADJUSTED
      state = pagingReducer(state, {
        type: 'SCROLL_ADJUSTED',
      });

      expect(state.pagingPhase).toBe('skipping');
      expect(state.pendingScrollAdjustment).toBe(0);
      expect(state.estimatedTotal).toBe(50);

      // Step 3: PAGING_COMPLETE
      state = pagingReducer(state, {
        type: 'PAGING_COMPLETE',
      });

      expect(state.pagingPhase).toBe('idle');
    });

    test('UPDATE_ESTIMATED_TOTAL can happen during any phase', () => {
      const phases: Array<'idle' | 'adjusting' | 'skipping'> = [
        'idle',
        'adjusting',
        'skipping',
      ];

      phases.forEach(phase => {
        const state = createInitialState({
          estimatedTotal: 100,
          pagingPhase: phase,
        });

        const result = pagingReducer(state, {
          type: 'UPDATE_ESTIMATED_TOTAL',
          newTotal: 150,
        });

        expect(result.estimatedTotal).toBe(150);
        expect(result.pagingPhase).toBe(phase);
      });
    });
  });

  describe('immutability', () => {
    test('does not mutate original state for UPDATE_ESTIMATED_TOTAL', () => {
      const state = createInitialState({estimatedTotal: 100});
      const originalEstimatedTotal = state.estimatedTotal;

      pagingReducer(state, {
        type: 'UPDATE_ESTIMATED_TOTAL',
        newTotal: 200,
      });

      expect(state.estimatedTotal).toBe(originalEstimatedTotal);
    });

    test('does not mutate original queryAnchor for UPDATE_ANCHOR', () => {
      const state = createInitialState();
      const originalAnchor = state.queryAnchor.anchor;

      pagingReducer(state, {
        type: 'UPDATE_ANCHOR',
        anchor: {
          index: 99,
          kind: 'forward',
          startRow: {id: 'new', createdAt: 9999},
        },
      });

      expect(state.queryAnchor.anchor).toBe(originalAnchor);
    });

    test('returns new state object for all actions', () => {
      const state = createInitialState();

      const actions: Array<PagingAction<TestListContextParams, TestStartRow>> =
        [
          {type: 'REACHED_START'},
          {type: 'REACHED_END'},
          {
            type: 'UPDATE_ANCHOR',
            anchor: {
              index: 5,
              kind: 'forward',
              startRow: {id: 'test', createdAt: 1000},
            },
          },
          {
            type: 'SHIFT_ANCHOR_DOWN',
            offset: 10,
            newAnchor: {
              index: 10,
              kind: 'forward',
              startRow: {id: 'test', createdAt: 1000},
            },
          },
          {type: 'RESET_TO_TOP', offset: -5},
          {type: 'SCROLL_ADJUSTED'},
          {type: 'PAGING_COMPLETE'},
          {
            type: 'RESET_STATE',
            estimatedTotal: 50,
            hasReachedStart: true,
            hasReachedEnd: true,
            anchor: {index: 0, kind: 'forward', startRow: undefined},
            listContextParams: {filter: 'new'},
          },
        ];

      actions.forEach(action => {
        const result = pagingReducer(state, action);
        expect(result).not.toBe(state);
      });
    });
  });
});
