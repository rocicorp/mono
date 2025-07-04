import {expect, test} from 'vitest';
import {LogarithmicHistogram} from './logarithmic-histogram.ts';

test('initializes correctly with default values', () => {
  const h = new LogarithmicHistogram();
  expect(h.counts.length).toBe(2); // underflow + one bucket
  expect(h.counts.every(c => c === 0)).toBe(true);
});

test('adds values and resizes dynamically', () => {
  const h = new LogarithmicHistogram();
  h.add(0.5); // should go to underflow
  h.add(0.9); // should go to underflow
  h.add(4); // log2(4) + 1 === 3
  h.add(4.1); // log2(4.1) + 1 === 3
  h.add(1024);
  expect(h.counts).toEqual(
    new Uint32Array([2, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1]),
  );
});

test('calculates bucket ranges dynamically', () => {
  const h = new LogarithmicHistogram();
  h.add(1024); // Trigger resizing
  const ranges = h.getBucketRanges();
  expect(ranges.length).toBe(12); // Ensure ranges reflect resizing
  expect(ranges[0]).toEqual([0, 1]);
  expect(ranges[1]).toEqual([1, 2]);
  expect(ranges[ranges.length - 1]).toEqual([1024, 2048]);
});

test('serializes and deserializes dynamically resized histogram', () => {
  const original = new LogarithmicHistogram();
  original.add(0.5);
  original.add(4);
  original.add(32);
  original.add(1024); // Trigger resizing

  const hex = original.toHexString();
  const deserialized = LogarithmicHistogram.fromHexString(hex);

  expect(deserialized.counts).toEqual(original.counts);
  expect(deserialized.toHexString()).toBe(hex);
});

test('handles serialization of large counts', () => {
  const hist = new LogarithmicHistogram();
  // Add a large number to one bucket
  for (let i = 0; i < 1_000_000; i++) {
    hist.add(1);
  }

  const hex = hist.toHexString();
  const deserialized = LogarithmicHistogram.fromHexString(hex);

  expect(deserialized.counts).toEqual(hist.counts);
});

test('throws error on invalid hex string', () => {
  expect(() => LogarithmicHistogram.fromHexString('invalid')).toThrow(
    'Invalid hex string: invalid',
  );
});

test('throws on negative values', () => {
  const h = new LogarithmicHistogram();
  expect(() => h.add(-1)).toThrow('Value must not be negative, got: -1');
});
