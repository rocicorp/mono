import type {ScrollHistoryState} from '@rocicorp/zero-virtual/react';
import {useHistoryState} from 'wouter/use-browser-location';

/**
 * Integrates the virtualizer's scroll state with wouter's history state, so it
 * persists across back/forward navigation. Uses wouter (like the rest of the
 * app) rather than the library's Navigation-API `useHistoryScrollState`.
 *
 * Returns `[state, setState]` to pass to `useZeroVirtualizer`'s `scrollState`
 * and `onScrollStateChange` options.
 */
export function useWouterScrollState<TStartRow>(): [
  ScrollHistoryState<TStartRow> | null,
  (state: ScrollHistoryState<TStartRow>) => void,
] {
  const scrollState = useHistoryState<ScrollHistoryState<TStartRow> | null>();

  return [scrollState, setScrollState];
}

function setScrollState<TStartRow>(state: ScrollHistoryState<TStartRow>) {
  window.history.replaceState(state, '', window.location.href);
}
