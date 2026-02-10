// Navigation API type declarations.
// These can be removed once TypeScript's lib.dom.d.ts includes them.
// See https://developer.mozilla.org/en-US/docs/Web/API/Navigation_API

interface NavigationCurrentEntryChangeEvent extends Event {
  readonly navigationType: 'push' | 'replace' | 'reload' | 'traverse' | null;
  readonly from: NavigationHistoryEntry;
}

interface NavigationHistoryEntry extends EventTarget {
  readonly key: string;
  readonly id: string;
  readonly url: string | null;
  readonly index: number;
  readonly sameDocument: boolean;
  getState(): unknown;
}

interface Navigation extends EventTarget {
  readonly currentEntry: NavigationHistoryEntry | null;
  entries(): NavigationHistoryEntry[];

  addEventListener(
    type: 'currententrychange',
    listener: (event: NavigationCurrentEntryChangeEvent) => void,
    options?: boolean | AddEventListenerOptions,
  ): void;
  removeEventListener(
    type: 'currententrychange',
    listener: (event: NavigationCurrentEntryChangeEvent) => void,
    options?: boolean | EventListenerOptions,
  ): void;
}

interface Window {
  readonly navigation: Navigation | undefined;
}
