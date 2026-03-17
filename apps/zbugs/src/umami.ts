// Based on:
//
// https://github.com/umami-software/umami/blob/master/src/tracker/index.js
// https://github.com/umami-software/umami/blob/master/src/tracker/index.d.ts
//
// We do not want to add @types/umami because browsers block tracking
// scripts so the global might be undefined

interface Umami {
  track(eventName: string, eventData?: {[key: string]: unknown}): void;
}

export const umami: Umami = (globalThis as {umami?: Umami}).umami ?? {
  track() {
    // no op
  },
};
