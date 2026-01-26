import type {PermalinkHistoryState} from '@rocicorp/zero/react';
import {useHistoryState} from 'wouter/use-browser-location';

/**
 * Custom hook that integrates Zero's permalink state with wouter's history state.
 * Returns the permalink state and a setter function that works with useZeroVirtualizer.
 */
export function useWouterPermalinkState<TStartRow>(): [
  PermalinkHistoryState<TStartRow> | null,
  (state: PermalinkHistoryState<TStartRow>) => void,
] {
  const permalinkState =
    useHistoryState<PermalinkHistoryState<TStartRow> | null>();

  return [permalinkState, setPermalinkState];
}

function setPermalinkState<TStartRow>(state: PermalinkHistoryState<TStartRow>) {
  window.history.replaceState(state, '', window.location.href);
}
