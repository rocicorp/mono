import {useEffect, useRef, useState} from 'react';
import {
  deepEqual,
  type ReadonlyJSONValue,
} from '../../../../packages/shared/src/json.ts';
import type {ScrollRestorationState} from './use-array-virtualizer.ts';

// Navigation API declarations
declare global {
  interface Window {
    navigation: Navigation;
  }

  interface Navigation extends EventTarget {
    addEventListener(type: 'navigate', listener: () => void): void;
    removeEventListener(type: 'navigate', listener: () => void): void;
  }
}

const currentHistoryState = () =>
  // oxlint-disable-next-line no-explicit-any
  window.history.state as ScrollRestorationState<any> | null;

/**
 * Custom hook that integrates the array virtualizer's scroll state with browser history state.
 * Returns the scroll state and a setter function that works with useArrayVirtualizer.
 */
export function useArrayPermalinkState<TStartRow>(): [
  ScrollRestorationState<TStartRow> | null,
  (state: ScrollRestorationState<TStartRow>) => void,
] {
  const [rawScrollState, setRawScrollState] =
    useState<ScrollRestorationState<TStartRow> | null>(currentHistoryState);

  // Listen to navigation events to update when user navigates back/forward or hash changes
  useEffect(() => {
    const updateState = () => {
      console.log(
        'Navigation event detected, updating scroll state from history',
        {currentState: currentHistoryState()},
      );
      setRawScrollState(currentHistoryState());
    };

    window.navigation.addEventListener('navigate', updateState);
    return () => window.navigation.removeEventListener('navigate', updateState);
  }, []);

  // Stabilize the reference - only return a new object if the values actually changed
  const prevStateRef = useRef<ScrollRestorationState<TStartRow> | null>(
    rawScrollState,
  );

  if (rawScrollState !== prevStateRef.current) {
    // Check if values actually differ
    if (
      !rawScrollState ||
      !prevStateRef.current ||
      rawScrollState.anchor.index !== prevStateRef.current.anchor.index ||
      rawScrollState.anchor.kind !== prevStateRef.current.anchor.kind ||
      (rawScrollState.anchor.kind === 'permalink' &&
        prevStateRef.current.anchor.kind === 'permalink' &&
        rawScrollState.anchor.id !== prevStateRef.current.anchor.id) ||
      ((rawScrollState.anchor.kind === 'forward' ||
        rawScrollState.anchor.kind === 'backward') &&
        (prevStateRef.current.anchor.kind === 'forward' ||
          prevStateRef.current.anchor.kind === 'backward') &&
        !deepEqual(
          rawScrollState.anchor.startRow as ReadonlyJSONValue | undefined,
          prevStateRef.current.anchor.startRow as ReadonlyJSONValue | undefined,
        )) ||
      rawScrollState.scrollOffset !== prevStateRef.current.scrollOffset
    ) {
      // Values differ, update the ref
      prevStateRef.current = rawScrollState;
    }
  }

  return [prevStateRef.current, setScrollState];
}

function setScrollState<TStartRow>(state: ScrollRestorationState<TStartRow>) {
  console.log('Setting scroll state', state);
  window.history.replaceState(state, '', window.location.href);
}
