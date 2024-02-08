import {resolver} from '@rocicorp/resolver';

type PartialDocument = Pick<
  Document,
  'visibilityState' | 'addEventListener' | 'removeEventListener'
>;

export function getDocumentVisibilityWatcher(
  doc: PartialDocument | undefined,
  hiddenIntervalMS: number,
  signal: AbortSignal,
): DocumentVisibilityWatcher {
  return doc
    ? new DocumentVisibilityWatcherImpl(doc, hiddenIntervalMS, signal)
    : new DocumentVisibilityWatcherNoDoc();
}

export interface DocumentVisibilityWatcher {
  readonly visibilityState: DocumentVisibilityState;
  waitForVisible(): Promise<unknown>;
  waitForHidden(): Promise<unknown>;
}

class DocumentVisibilityWatcherImpl implements DocumentVisibilityWatcher {
  readonly #doc: PartialDocument;
  readonly #hiddenIntervalMS: number;
  #timeoutID: ReturnType<typeof setTimeout> | 0 = 0;

  // This trails doc.visibilityState by hiddenIntervalMS when being hidden. This
  // is because we want to wait for the tab to be hidden for a while before
  // considering as hidden.
  visibilityState: DocumentVisibilityState;

  readonly #promises = new Set<{
    resolve: () => void;
    state: DocumentVisibilityState;
  }>();

  constructor(
    doc: PartialDocument,
    hiddenIntervalMS: number,
    signal: AbortSignal,
  ) {
    this.#doc = doc;
    this.#hiddenIntervalMS = hiddenIntervalMS;
    this.visibilityState = doc.visibilityState;
    // Safari got support for abort signal in addEventListener in version
    // 15 (Released 2021-09-20)
    this.#doc.addEventListener('visibilitychange', this.#onVisibilityChange, {
      signal,
    });
  }

  #onVisibilityChange = () => {
    if (this.#doc.visibilityState === 'visible') {
      clearTimeout(this.#timeoutID);
      this.#setVisibilityState('visible');
    } else {
      this.#timeoutID = setTimeout(() => {
        this.#setVisibilityState('hidden');
      }, this.#hiddenIntervalMS);
    }
  };

  #setVisibilityState(visibilityState: DocumentVisibilityState) {
    if (visibilityState === this.visibilityState) {
      return;
    }
    this.visibilityState = visibilityState;
    for (const entry of this.#promises) {
      const {resolve, state} = entry;
      if (state === visibilityState) {
        resolve();
        this.#promises.delete(entry);
      }
    }
  }

  waitForVisible(): Promise<unknown> {
    return this.#waitFor('visible');
  }

  waitForHidden(): Promise<unknown> {
    return this.#waitFor('hidden');
  }

  #waitFor(state: DocumentVisibilityState): Promise<unknown> {
    if (this.visibilityState === state) {
      return Promise.resolve();
    }

    const {promise, resolve} = resolver();
    this.#promises.add({resolve, state});
    return promise;
  }
}

const resolvedPromise = Promise.resolve();
const promiseThatNeverResolves = new Promise(() => undefined);

class DocumentVisibilityWatcherNoDoc implements DocumentVisibilityWatcher {
  readonly visibilityState: DocumentVisibilityState = 'visible';
  waitForVisible(): Promise<unknown> {
    return resolvedPromise;
  }

  waitForHidden(): Promise<unknown> {
    return promiseThatNeverResolves;
  }
}
