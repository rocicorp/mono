import {useRef} from 'react';
import {useHistoryState} from 'wouter/use-browser-location';
import {
  deepEqual,
  type ReadonlyJSONValue,
} from '../../../../packages/shared/src/json.ts';
import type {ScrollRestorationState} from './use-array-virtualizer.ts';

/**
 * Custom hook that integrates the array virtualizer's scroll state with wouter's history state.
 * Returns the scroll state and a setter function that works with useArrayVirtualizer.
 */
export function useArrayPermalinkState<TStartRow>(): [
  ScrollRestorationState<TStartRow> | null,
  (state: ScrollRestorationState<TStartRow>) => void,
] {
  const rawScrollState =
    useHistoryState<ScrollRestorationState<TStartRow> | null>();

  // Stabilize the reference - only return a new object if the values actually changed
  const prevStateRef = useRef<ScrollRestorationState<TStartRow> | null>(
    rawScrollState,
  );

  if (rawScrollState !== prevStateRef.current) {
    // Check if values actually differ
    if (
      !rawScrollState ||
      !prevStateRef.current ||
      rawScrollState.anchorIndex !== prevStateRef.current.anchorIndex ||
      rawScrollState.anchorKind !== prevStateRef.current.anchorKind ||
      !deepEqual(
        rawScrollState.startRow as ReadonlyJSONValue | undefined,
        prevStateRef.current.startRow as ReadonlyJSONValue | undefined,
      ) ||
      rawScrollState.permalinkID !== prevStateRef.current.permalinkID ||
      rawScrollState.scrollOffset !== prevStateRef.current.scrollOffset
    ) {
      // Values differ, update the ref
      prevStateRef.current = rawScrollState;
    }
  }

  return [prevStateRef.current, setScrollState];
}

function setScrollState<TStartRow>(state: ScrollRestorationState<TStartRow>) {
  window.history.replaceState(state, '', window.location.href);
}
