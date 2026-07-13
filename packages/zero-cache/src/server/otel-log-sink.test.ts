import {resolver} from '@rocicorp/resolver';
import {describe, expect, test, vi} from 'vitest';
import {OtelLogSink} from './otel-log-sink.ts';

describe('OtelLogSink', () => {
  test('force-flushes the logger provider', async () => {
    const forceFlushDone = resolver();
    const forceFlush = vi.fn(() => forceFlushDone.promise);
    const sink = new OtelLogSink({
      forceFlush,
      getLogger: vi.fn(() => ({
        emit: vi.fn(),
        enabled: vi.fn(() => true),
      })),
    });

    const flushing = sink.flush();
    let flushed = false;
    void flushing.then(() => (flushed = true));
    await Promise.resolve();

    expect(forceFlush).toHaveBeenCalledTimes(1);
    expect(flushed).toBe(false);

    forceFlushDone.resolve();
    await flushing;
    expect(flushed).toBe(true);
  });
});
