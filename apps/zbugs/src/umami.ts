// Based on:
//
// https://github.com/umami-software/umami/blob/master/src/tracker/index.js
// https://github.com/umami-software/umami/blob/master/src/tracker/index.d.ts
//
// We do not want to add @types/umami because browsers block tracking
// scripts so the global might be undefined

interface Umami {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  track(eventName: string, eventData?: {[key: string]: any}): void;
}

// eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-member-access
export const umami: Umami = (globalThis as any).umami ?? {
  track() {
    // no op
  },
};
