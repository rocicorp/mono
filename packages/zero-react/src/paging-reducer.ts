import {deepEqual, type ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {Anchor} from './use-rows.ts';

type QueryAnchor<TListContextParams, TStartRow> = {
  readonly anchor: Anchor<TStartRow>;
  /**
   * Associates an anchor with list query params to coordinate state during
   * navigation. When list context params change (e.g., filter/sort changes or
   * browser back/forward navigation), the anchor and scroll position must be
   * updated atomically with the new query results.
   *
   * When `listContextParams !== queryAnchor.listContextParams`:
   * - Use history state to restore previous scroll position and anchor if
   *   navigating back
   * - Use permalink anchor if loading a specific item
   * - Otherwise reset to top
   *
   * During the transition (while `!isListContextCurrent`), skip paging logic
   * and count updates to avoid querying with mismatched anchor/params or
   * calculating counts from inconsistent state.
   */
  readonly listContextParams: TListContextParams;
};

export type PagingState<TListContextParams, TStartRow> = {
  estimatedTotal: number;
  hasReachedStart: boolean;
  hasReachedEnd: boolean;
  queryAnchor: QueryAnchor<TListContextParams, TStartRow>;
  pagingPhase: 'idle' | 'skipping';
};

export function pagingReducer<TListContextParams, TStartRow>(
  state: PagingState<TListContextParams, TStartRow>,
  action: PagingAction<TListContextParams, TStartRow>,
): PagingState<TListContextParams, TStartRow> {
  console.log('Paging action:', action);
  switch (action.type) {
    case 'UPDATE_ESTIMATED_TOTAL': {
      const newTotal = Math.max(state.estimatedTotal, action.newTotal);
      if (newTotal === state.estimatedTotal) {
        return state;
      }
      return {
        ...state,
        estimatedTotal: newTotal,
      };
    }

    case 'REACHED_START':
      return {...state, hasReachedStart: true};

    case 'REACHED_END':
      return {...state, hasReachedEnd: true};

    case 'UPDATE_ANCHOR': {
      const newState = {
        ...state,
        queryAnchor: {
          ...state.queryAnchor,
          anchor: action.anchor,
        },
      };
      if (
        deepEqual(state as ReadonlyJSONValue, newState as ReadonlyJSONValue)
      ) {
        return state;
      }
      return newState;
    }

    case 'SHIFT_ANCHOR_DOWN':
      return {
        ...state,
        queryAnchor: {
          ...state.queryAnchor,
          anchor: action.newAnchor,
        },
        estimatedTotal: state.estimatedTotal + action.offset,
        pagingPhase: 'skipping',
      };

    case 'RESET_TO_TOP':
      return {
        ...state,
        queryAnchor: {
          ...state.queryAnchor,
          anchor: {index: 0, kind: 'forward', startRow: undefined},
        },
        estimatedTotal: state.estimatedTotal + action.offset,
        pagingPhase: 'skipping',
      };

    case 'PAGING_COMPLETE':
      return {
        ...state,
        pagingPhase: 'idle',
      };

    case 'RESET_STATE':
      return {
        ...state,
        estimatedTotal: action.estimatedTotal,
        hasReachedStart: action.hasReachedStart,
        hasReachedEnd: action.hasReachedEnd,
        queryAnchor: {
          listContextParams: action.listContextParams,
          anchor: action.anchor,
        },
        pagingPhase: 'skipping',
      };

    default: {
      action satisfies never;
      return state;
    }
  }
}

export type PagingAction<TListContextParams, TStartRow> =
  | {type: 'UPDATE_ESTIMATED_TOTAL'; newTotal: number}
  | {type: 'REACHED_START'}
  | {type: 'REACHED_END'}
  | {type: 'UPDATE_ANCHOR'; anchor: Anchor<TStartRow>}
  | {
      type: 'SHIFT_ANCHOR_DOWN';
      offset: number;
      newAnchor: Anchor<TStartRow>;
    }
  | {type: 'RESET_TO_TOP'; offset: number}
  | {type: 'PAGING_COMPLETE'}
  | {
      type: 'RESET_STATE';
      estimatedTotal: number;
      hasReachedStart: boolean;
      hasReachedEnd: boolean;
      anchor: Anchor<TStartRow>;
      listContextParams: TListContextParams;
    };
