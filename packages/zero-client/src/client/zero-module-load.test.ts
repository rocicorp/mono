import {afterEach, expect, test, vi} from 'vitest';

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetModules();
});

// Cloudflare Workers refuse to generate random values outside of a request
// handler, so nothing may draw randomness while the client's modules load.
test('loading the client does not generate random values', async () => {
  vi.stubGlobal('crypto', {
    getRandomValues: () => {
      throw new Error(
        'Some functionality, such as asynchronous I/O, timeouts, and generating random values, can only be performed while handling a request.',
      );
    },
  });
  vi.resetModules();
  const mod = await import('./zero.ts');
  expect(typeof mod.Zero).toBe('function');
});
