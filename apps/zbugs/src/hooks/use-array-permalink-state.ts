import {useRef} from 'react';
import {useHistoryState} from 'wouter/use-browser-location';
import type {ScrollRestorationState} from './use-array-virtualizer.ts';

/**
 * Custom hook that integrates the array virtualizer's scroll state with wouter's history state.
 * Returns the scroll state and a setter function that works with useArrayVirtualizer.
 */
export function useArrayPermalinkState(): [
  ScrollRestorationState | null,
  (state: ScrollRestorationState) => void,
] {
  const rawScrollState = useHistoryState<ScrollRestorationState | null>();

  // Stabilize the reference - only return a new object if the values actually changed
  const prevStateRef = useRef<ScrollRestorationState | null>(rawScrollState);

  if (rawScrollState !== prevStateRef.current) {
    // Check if values actually differ
    if (
      !rawScrollState ||
      !prevStateRef.current ||
      rawScrollState.scrollAnchorID !== prevStateRef.current.scrollAnchorID ||
      rawScrollState.index !== prevStateRef.current.index ||
      rawScrollState.scrollOffset !== prevStateRef.current.scrollOffset
    ) {
      // Values differ, update the ref
      prevStateRef.current = rawScrollState;
    }
  }

  return [prevStateRef.current, setScrollState];
}

function setScrollState(state: ScrollRestorationState) {
  window.history.replaceState(state, '', window.location.href);
}
