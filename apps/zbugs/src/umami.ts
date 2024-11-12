/* eslint-disable @typescript-eslint/no-explicit-any */

// Based on
// https://github.com/DefinitelyTyped/DefinitelyTyped/blob/4677b2523d7cdb6545907896b7dcaba098f22ce1/types/umami/index.d.ts
// But we do not want to add @types/umami because browsers block tracking
// scripts so the global might be undefined

interface Umami {
  track(eventName: string, eventData?: {[key: string]: any}): void;
}

export const umami: Umami = (globalThis as any).umami ?? {
  track() {
    // no op
  },
};
