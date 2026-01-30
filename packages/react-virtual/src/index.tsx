/* eslint-disable */
/**
 * Forked from @tanstack/react-virtual v3.13.18
 * https://github.com/TanStack/virtual
 * MIT License
 *
 * Modifications by Rocicorp:
 * - Enhanced scroll stability for insert/delete/resize operations above viewport
 */

import * as React from 'react';
import {flushSync} from 'react-dom';
import type {PartialKeys, VirtualizerOptions} from './virtualizer.ts';
import {
  Virtualizer,
  elementScroll,
  observeElementOffset,
  observeElementRect,
  observeWindowOffset,
  observeWindowRect,
  windowScroll,
} from './virtualizer.ts';

export * from './virtualizer.ts';

const useIsomorphicLayoutEffect =
  typeof document !== 'undefined' ? React.useLayoutEffect : React.useEffect;

export type ReactVirtualizerOptions<
  TScrollElement extends Element | Window,
  TItemElement extends Element,
> = VirtualizerOptions<TScrollElement, TItemElement> & {
  useFlushSync?: boolean;
};

function useVirtualizerBase<
  TScrollElement extends Element | Window,
  TItemElement extends Element,
>({
  useFlushSync = true,
  ...options
}: ReactVirtualizerOptions<TScrollElement, TItemElement>): Virtualizer<
  TScrollElement,
  TItemElement
> {
  const rerender = React.useReducer(() => ({}), {})[1];

  const resolvedOptions: VirtualizerOptions<TScrollElement, TItemElement> = {
    ...options,
    onChange: (instance, sync) => {
      if (useFlushSync && sync) {
        flushSync(rerender);
      } else {
        rerender();
      }
      options.onChange?.(instance, sync);
    },
  };

  const [instance] = React.useState(
    () => new Virtualizer<TScrollElement, TItemElement>(resolvedOptions),
  );

  instance.setOptions(resolvedOptions);

  useIsomorphicLayoutEffect(() => {
    return instance._didMount();
  }, []);

  useIsomorphicLayoutEffect(() => {
    return instance._willUpdate();
  });

  return instance;
}

export function useVirtualizer<
  TScrollElement extends Element,
  TItemElement extends Element,
>(
  options: PartialKeys<
    ReactVirtualizerOptions<TScrollElement, TItemElement>,
    'observeElementRect' | 'observeElementOffset' | 'scrollToFn'
  >,
): Virtualizer<TScrollElement, TItemElement> {
  return useVirtualizerBase<TScrollElement, TItemElement>({
    observeElementRect: observeElementRect,
    observeElementOffset: observeElementOffset,
    scrollToFn: elementScroll,
    ...options,
  });
}

export function useWindowVirtualizer<TItemElement extends Element>(
  options: PartialKeys<
    ReactVirtualizerOptions<Window, TItemElement>,
    | 'getScrollElement'
    | 'observeElementRect'
    | 'observeElementOffset'
    | 'scrollToFn'
  >,
): Virtualizer<Window, TItemElement> {
  return useVirtualizerBase<Window, TItemElement>({
    getScrollElement: () => (typeof document !== 'undefined' ? window : null),
    observeElementRect: observeWindowRect,
    observeElementOffset: observeWindowOffset,
    scrollToFn: windowScroll,
    initialOffset: () => (typeof document !== 'undefined' ? window.scrollY : 0),
    ...options,
  });
}
