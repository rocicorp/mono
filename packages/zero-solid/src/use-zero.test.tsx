import {vi, test, expect} from 'vitest';
import {renderHook} from '@solidjs/testing-library';
import {useZero, ZeroProvider} from './use-zero.tsx';
import {createSignal, type JSX} from 'solid-js';
import type {Schema, Zero, ZeroOptions} from '../../zero/src/zero.ts';

vi.mock('../../zero/src/zero.ts', async importOriginal => ({
  ...(await importOriginal<typeof import('../../zero/src/zero.ts')>()),
  // eslint-disable-next-line @typescript-eslint/naming-convention
  Zero: class {
    closed = false;

    constructor() {}

    close() {
      this.closed = true;
    }
  },
}));

class FakeZero {
  closed = false;

  constructor() {}

  close() {
    this.closed = true;
  }
}

test('if zeroSignal change ZeroProvider closes previous instance if it created it', () => {
  const [zeroOptions, setZeroOptions] = createSignal({
    server: 'foo',
    userID: 'u',
    schema: {tables: {}, relationships: {}},
  });
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const MockZeroProvider = (props: {children: JSX.Element}) => (
    <ZeroProvider zeroSignal={zeroOptions}>{props.children}</ZeroProvider>
  );
  const {result} = renderHook(useZero, {
    initialProps: [],
    wrapper: MockZeroProvider,
  });

  const zero0 = result();
  expect(zero0?.closed).toBe(false);

  setZeroOptions({
    server: 'bar',
    userID: 'u',
    schema: {tables: {}, relationships: {}},
  });

  expect(zero0?.closed).toBe(true);

  const zero1 = result();
  expect(zero0).not.toBe(zero1);
});

test('if signal is set with same options instance ZeroProvider does not recreate Zero, but does if options are deep equal but different instance', () => {
  const options = {
    server: 'foo',
    userID: 'u',
    schema: {tables: {}, relationships: {}},
  } as const;
  const [zeroOptions, setZeroOptions] = createSignal(options);
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const MockZeroProvider = (props: {children: JSX.Element}) => (
    <ZeroProvider zeroSignal={zeroOptions}>{props.children}</ZeroProvider>
  );
  const {result} = renderHook(useZero, {
    initialProps: [],
    wrapper: MockZeroProvider,
  });

  const zero0 = result();
  expect(zero0?.closed).toBe(false);

  setZeroOptions(options);

  expect(zero0?.closed).toBe(false);

  const zero1 = result();
  expect(zero1).toBe(zero0);

  setZeroOptions({...options});

  expect(zero0?.closed).toBe(true);

  const zero2 = result();
  expect(zero2).not.toBe(zero0);
});

test('if zeroSignal changes ZeroProvider closes previous instance if it created it', () => {
  const fakeZero0 = new FakeZero() as unknown as Zero<Schema>;
  const [zeroSignal, setZeroSignal] = createSignal<
    ZeroOptions<Schema, undefined> | {zero: Zero<Schema, undefined>}
  >({
    zero: fakeZero0,
  });
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const MockZeroProvider = (props: {children: JSX.Element}) => (
    <ZeroProvider zeroSignal={zeroSignal}>{props.children}</ZeroProvider>
  );
  const {result} = renderHook(useZero, {
    initialProps: [],
    wrapper: MockZeroProvider,
  });

  const zero0 = result();
  expect(zero0).toBe(fakeZero0);
  expect(zero0?.closed).toBe(false);

  setZeroSignal({
    server: 'bar',
    userID: 'u',
    schema: {tables: {}, relationships: {}},
  });

  expect(zero0?.closed).toBe(false);

  const zero1 = result();
  expect(zero0).not.toBe(zero1);

  expect(zero1?.closed).toBe(false);

  const fakeZero1 = new FakeZero() as unknown as Zero<Schema>;

  setZeroSignal({
    zero: fakeZero1,
  });

  const zero2 = result();
  expect(zero1).not.toBe(zero2);

  expect(zero1?.closed).toBe(true);
});
