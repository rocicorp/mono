import {renderHook} from '@solidjs/testing-library';
import {createSignal, type JSX} from 'solid-js';
import {beforeEach, describe, expect, test, vi, type Mock} from 'vitest';

import {createUseZero, useZero, ZeroProvider} from './use-zero.ts';
import {
  Zero as ZeroConstructor,
  type Schema,
  type Zero,
  type ZeroOptions,
} from './zero.ts';

vi.mock('./zero.ts', () => ({
  Zero: vi.fn(),
}));

const ZeroMock = vi.mocked(ZeroConstructor);

type MockZero = ReturnType<typeof createMockZero>;

function createMockZero(clientID = 'test-client'): Zero<Schema> & {
  close: Mock;
  connection: {connect: Mock};
} {
  const close = vi.fn().mockResolvedValue(undefined);
  const connect = vi.fn().mockResolvedValue(undefined);

  return {
    clientID,
    close,
    connection: {
      connect,
    },
  } as unknown as Zero<Schema> & {
    close: typeof close;
    connection: {connect: typeof connect};
  };
}

beforeEach(() => {
  ZeroMock.mockReset();
});

describe('useZero', () => {
  test('throws error when used outside ZeroProvider', () => {
    expect(() =>
      renderHook(() => useZero(), {
        initialProps: [],
      }),
    ).toThrow('useZero must be used within a ZeroProvider');
  });

  test('returns zero instance when used inside ZeroProvider', () => {
    const externalZero = createMockZero();
    const wrapper = (props: {children: JSX.Element}) => (
      <ZeroProvider zero={externalZero}>{props.children}</ZeroProvider>
    );

    const {result} = renderHook(() => useZero<Schema>(), {
      initialProps: [],
      wrapper,
    });

    expect(result()).toBe(externalZero);
  });
});

describe('createUseZero', () => {
  test('returns a typed hook', () => {
    const externalZero = createMockZero();
    const useTypedZero = createUseZero<Schema>();

    const wrapper = (props: {children: JSX.Element}) => (
      <ZeroProvider zero={externalZero}>{props.children}</ZeroProvider>
    );

    const {result} = renderHook(useTypedZero, {
      initialProps: [],
      wrapper,
    });

    expect(result()).toBe(externalZero);
  });
});

describe('ZeroProvider', () => {
  describe('internal zero lifecycle', () => {
    test('closes previous instance it created when options change', () => {
      const zero1 = createMockZero('client-1');
      const zero2 = createMockZero('client-2');

      ZeroMock.mockImplementationOnce(() => zero1).mockImplementationOnce(
        () => zero2,
      );

      const [server, setServer] = createSignal('foo');
      const schema = {} as Schema;

      const wrapper = (props: {children: JSX.Element}) => (
        <ZeroProvider cacheURL={server()} schema={schema} userID="u">
          {props.children}
        </ZeroProvider>
      );

      const {result} = renderHook(() => useZero<Schema>(), {
        initialProps: [],
        wrapper,
      });

      expect(result()).toBe(zero1);

      setServer('bar');

      expect(zero1.close).toHaveBeenCalledTimes(1);
      expect(zero2.close).not.toHaveBeenCalled();
      expect(result()).toBe(zero2);
      expect(ZeroMock).toHaveBeenCalledTimes(2);
    });

    test('does not recreate zero when only children change', () => {
      const zero = createMockZero();
      ZeroMock.mockReturnValue(zero);

      const [wrap, setWrap] = createSignal(false);
      const schema = {} as Schema;

      const wrapper = (props: {children: JSX.Element}) => (
        <ZeroProvider cacheURL="foo" schema={schema} userID="u">
          {wrap() ? <div>{props.children}</div> : props.children}
        </ZeroProvider>
      );

      const {result} = renderHook(() => useZero<Schema>(), {
        initialProps: [],
        wrapper,
      });

      expect(result()).toBe(zero);
      expect(ZeroMock).toHaveBeenCalledTimes(1);

      setWrap(true);

      expect(result()).toBe(zero);
      expect(ZeroMock).toHaveBeenCalledTimes(1);
      expect(zero.close).not.toHaveBeenCalled();
    });

    test('does not recreate zero for identical options but does for new references', () => {
      const createdZeros: MockZero[] = [];
      ZeroMock.mockImplementation(() => {
        const zero = createMockZero(`client-${createdZeros.length + 1}`);
        createdZeros.push(zero);
        return zero;
      });

      const [server, setServer] = createSignal('foo');
      const baseSchema = {tables: {}, relationships: {}} as Schema;
      const [schema, setSchema] = createSignal<Schema>(baseSchema);

      const wrapper = (props: {children: JSX.Element}) => (
        <ZeroProvider cacheURL={server()} schema={schema()} userID="u">
          {props.children}
        </ZeroProvider>
      );

      const {result} = renderHook(() => useZero<Schema>(), {
        initialProps: [],
        wrapper,
      });

      expect(result()).toBe(createdZeros[0]);
      expect(ZeroMock).toHaveBeenCalledTimes(1);

      setServer('foo');
      expect(result()).toBe(createdZeros[0]);
      expect(ZeroMock).toHaveBeenCalledTimes(1);

      setSchema(schema());
      expect(result()).toBe(createdZeros[0]);
      expect(ZeroMock).toHaveBeenCalledTimes(1);

      setSchema({...schema()});

      expect(result()).toBe(createdZeros[1]);
      expect(ZeroMock).toHaveBeenCalledTimes(2);
      expect(createdZeros[0].close).toHaveBeenCalledTimes(1);
      expect(createdZeros[1].close).not.toHaveBeenCalled();
    });

    test('calls init callback with constructed zero', () => {
      const zero = createMockZero();
      ZeroMock.mockReturnValue(zero);
      const init = vi.fn();

      const schema = {} as Schema;

      renderHook(() => useZero<Schema>(), {
        initialProps: [],
        wrapper: (props: {children: JSX.Element}) => (
          <ZeroProvider
            cacheURL="https://example.com"
            schema={schema}
            userID="u"
            init={init}
          >
            {props.children}
          </ZeroProvider>
        ),
      });

      expect(init).toHaveBeenCalledWith(zero);
      expect(init).toHaveBeenCalledTimes(1);
    });
  });

  describe('external zero lifecycle', () => {
    test('does not close externally provided zero when it changes', () => {
      const zero1 = createMockZero('client-1');
      const zero2 = createMockZero('client-2');

      const [zero, setZero] = createSignal<Zero<Schema>>(zero1);

      const wrapper = (props: {children: JSX.Element}) => (
        <ZeroProvider zero={zero()}>{props.children}</ZeroProvider>
      );

      const {result} = renderHook(() => useZero<Schema>(), {
        initialProps: [],
        wrapper,
      });

      expect(result()).toBe(zero1);
      expect(zero1.close).not.toHaveBeenCalled();

      setZero(zero2);

      expect(result()).toBe(zero2);
      expect(zero1.close).not.toHaveBeenCalled();
      expect(zero2.close).not.toHaveBeenCalled();
    });
  });

  describe('auth handling', () => {
    test('passes auth to Zero constructor when provided', () => {
      const zero = createMockZero();
      const capturedOptions: ZeroOptions<Schema>[] = [];
      ZeroMock.mockImplementation(options => {
        capturedOptions.push(options as ZeroOptions<Schema>);
        return zero;
      });

      const schema = {} as Schema;

      renderHook(() => useZero<Schema>(), {
        initialProps: [],
        wrapper: (props: {children: JSX.Element}) => (
          <ZeroProvider
            cacheURL="https://example.com"
            schema={schema}
            auth="token-1"
            userID="u"
          >
            {props.children}
          </ZeroProvider>
        ),
      });

      expect(capturedOptions).toHaveLength(1);
      expect(capturedOptions[0]?.auth).toBe('token-1');
    });

    test('omits auth from constructor args when not provided', () => {
      const zero = createMockZero();
      // oxlint-disable-next-line no-explicit-any
      const capturedOptions: ZeroOptions<any, any, any>[] = [];
      ZeroMock.mockImplementation(options => {
        capturedOptions.push(options);
        return zero;
      });

      const schema = {} as Schema;

      renderHook(() => useZero<Schema>(), {
        initialProps: [],
        wrapper: (props: {children: JSX.Element}) => (
          <ZeroProvider
            cacheURL="https://example.com"
            schema={schema}
            userID="u"
          >
            {props.children}
          </ZeroProvider>
        ),
      });

      expect(capturedOptions).toHaveLength(1);
      expect(capturedOptions[0]).not.toHaveProperty('auth');
    });

    test('calls connection.connect when auth changes', () => {
      const zero = createMockZero();
      ZeroMock.mockReturnValue(zero);

      const schema = {} as Schema;
      const [auth, setAuth] = createSignal('token-1');

      const wrapper = (props: {children: JSX.Element}) => (
        <ZeroProvider
          cacheURL="https://example.com"
          schema={schema}
          auth={auth()}
          userID="u"
        >
          {props.children}
        </ZeroProvider>
      );

      renderHook(() => useZero<Schema>(), {
        initialProps: [],
        wrapper,
      });

      expect(zero.connection.connect).not.toHaveBeenCalled();
      expect(ZeroMock).toHaveBeenCalledTimes(1);

      setAuth('token-2');

      expect(zero.connection.connect).toHaveBeenCalledWith({auth: 'token-2'});
      expect(zero.connection.connect).toHaveBeenCalledTimes(1);
      expect(ZeroMock).toHaveBeenCalledTimes(1);
      expect(zero.close).not.toHaveBeenCalled();

      zero.connection.connect.mockClear();

      setAuth('token-2');

      expect(zero.connection.connect).not.toHaveBeenCalled();
    });

    test('calls connection.connect when auth changes from undefined to a value', () => {
      const zero = createMockZero();
      ZeroMock.mockReturnValue(zero);

      const schema = {} as Schema;
      const [auth, setAuth] = createSignal<string | undefined>(undefined);

      const wrapper = (props: {children: JSX.Element}) => (
        <ZeroProvider
          cacheURL="https://example.com"
          schema={schema}
          auth={auth()}
          userID="u"
        >
          {props.children}
        </ZeroProvider>
      );

      renderHook(() => useZero<Schema>(), {
        initialProps: [],
        wrapper,
      });

      setAuth('token-new');

      expect(zero.connection.connect).toHaveBeenCalledWith({
        auth: 'token-new',
      });
      expect(zero.connection.connect).toHaveBeenCalledTimes(1);
      expect(ZeroMock).toHaveBeenCalledTimes(1);
    });

    test('calls connection.connect with undefined when auth prop is changed to undefined', () => {
      const zero = createMockZero();
      ZeroMock.mockReturnValue(zero);

      const schema = {} as Schema;
      const [auth, setAuth] = createSignal<string | undefined>('token-initial');

      const wrapper = (props: {children: JSX.Element}) => (
        <ZeroProvider
          cacheURL="https://example.com"
          schema={schema}
          userID="u"
          auth={auth()}
        >
          {props.children}
        </ZeroProvider>
      );

      renderHook(() => useZero<Schema>(), {
        initialProps: [],
        wrapper,
      });

      setAuth(undefined);

      expect(zero.connection.connect).toHaveBeenCalledWith({auth: undefined});
      expect(zero.connection.connect).toHaveBeenCalledTimes(1);
      expect(ZeroMock).toHaveBeenCalledTimes(1);

      zero.connection.connect.mockClear();

      setAuth('token-new');

      expect(zero.connection.connect).toHaveBeenCalledWith({auth: 'token-new'});
      expect(zero.connection.connect).toHaveBeenCalledTimes(1);
    });

    test('calls connection.connect when zero is provided externally and auth changes', () => {
      const zero = createMockZero();
      const [auth, setAuth] = createSignal<string | undefined>('token-1');

      const wrapper = (props: {children: JSX.Element}) => (
        <ZeroProvider zero={zero} auth={auth()}>
          {props.children}
        </ZeroProvider>
      );

      renderHook(() => useZero<Schema>(), {
        initialProps: [],
        wrapper,
      });

      expect(zero.connection.connect).not.toHaveBeenCalled();

      setAuth('token-2');

      expect(zero.connection.connect).toHaveBeenCalledWith({auth: 'token-2'});
      expect(zero.connection.connect).toHaveBeenCalledTimes(1);

      zero.connection.connect.mockClear();

      setAuth(undefined);

      expect(zero.connection.connect).toHaveBeenCalledWith({auth: undefined});
    });
  });
});
