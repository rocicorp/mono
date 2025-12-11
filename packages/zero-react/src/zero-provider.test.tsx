import {act} from 'react';
import {createRoot, type Root} from 'react-dom/client';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createUseZero, useZero, ZeroProvider} from './zero-provider.tsx';
import type * as ZeroModule from './zero.ts';
import type {Schema, Zero, ZeroOptions} from './zero.ts';

// Mock the Zero constructor
vi.mock('./zero.ts', async importOriginal => {
  const original = await importOriginal<typeof ZeroModule>();
  return {
    ...original,
    Zero: vi.fn(),
  };
});

import {Zero as ZeroConstructor} from './zero.ts';

function createMockZero(clientID = 'test-client'): Zero<Schema> {
  const closeMock = vi.fn().mockResolvedValue(undefined);
  const connectMock = vi.fn().mockResolvedValue(undefined);

  return {
    clientID,
    close: closeMock,
    connection: {
      connect: connectMock,
    },
  } as unknown as Zero<Schema>;
}

function renderWithRoot(children: React.ReactElement): Root {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);
  act(() => {
    root.render(children);
  });
  return root;
}

describe('useZero', () => {
  test('throws error when used outside ZeroProvider', () => {
    function TestComponent() {
      useZero();
      return null;
    }

    expect(() => {
      renderWithRoot(<TestComponent />);
    }).toThrow('useZero must be used within a ZeroProvider');
  });

  test('returns zero instance when used inside ZeroProvider', () => {
    const mockZero = createMockZero();
    let capturedZero: Zero<Schema> | undefined;

    function TestComponent() {
      capturedZero = useZero<Schema>();
      return <div>test</div>;
    }

    const root = renderWithRoot(
      <ZeroProvider zero={mockZero}>
        <TestComponent />
      </ZeroProvider>,
    );

    expect(capturedZero).toBe(mockZero);

    act(() => {
      root.unmount();
    });
  });
});

describe('createUseZero', () => {
  test('creates a typed version of useZero', () => {
    const mockZero = createMockZero();
    let capturedZero: Zero<Schema> | undefined;

    const useTypedZero = createUseZero<Schema>();

    function TestComponent() {
      capturedZero = useTypedZero();
      return <div>test</div>;
    }

    const root = renderWithRoot(
      <ZeroProvider zero={mockZero}>
        <TestComponent />
      </ZeroProvider>,
    );

    expect(capturedZero).toBe(mockZero);

    act(() => {
      root.unmount();
    });
  });
});

describe('ZeroProvider', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('with pre-constructed zero', () => {
    test('provides the zero instance to children', () => {
      const mockZero = createMockZero();
      let capturedZero: Zero<Schema> | undefined;

      function TestComponent() {
        capturedZero = useZero<Schema>();
        return <div>test</div>;
      }

      const root = renderWithRoot(
        <ZeroProvider zero={mockZero}>
          <TestComponent />
        </ZeroProvider>,
      );

      expect(capturedZero).toBe(mockZero);

      act(() => {
        root.unmount();
      });
    });

    test('calls init callback with zero instance', () => {
      const mockZero = createMockZero();
      const initMock = vi.fn();

      const root = renderWithRoot(
        <ZeroProvider zero={mockZero} init={initMock}>
          <div>test</div>
        </ZeroProvider>,
      );

      expect(initMock).not.toHaveBeenCalled();

      act(() => {
        root.unmount();
      });
    });

    test('does not close zero on unmount when provided externally', () => {
      const mockZero = createMockZero();

      const root = renderWithRoot(
        <ZeroProvider zero={mockZero}>
          <div>test</div>
        </ZeroProvider>,
      );

      act(() => {
        root.unmount();
      });

      expect(mockZero.close).not.toHaveBeenCalled();
    });

    test('updates zero instance when zero prop changes', () => {
      const mockZero1 = createMockZero('client-1');
      const mockZero2 = createMockZero('client-2');
      let capturedZero: Zero<Schema> | undefined;

      function TestComponent() {
        capturedZero = useZero<Schema>();
        return <div>test</div>;
      }

      const root = renderWithRoot(
        <ZeroProvider zero={mockZero1}>
          <TestComponent />
        </ZeroProvider>,
      );

      expect(capturedZero).toBe(mockZero1);

      act(() => {
        root.render(
          <ZeroProvider zero={mockZero2}>
            <TestComponent />
          </ZeroProvider>,
        );
      });

      expect(capturedZero).toBe(mockZero2);

      act(() => {
        root.unmount();
      });
    });
  });

  describe('with ZeroOptions', () => {
    test('constructs zero instance from options', () => {
      const mockZero = createMockZero();
      const ZeroMock = vi.mocked(ZeroConstructor);
      ZeroMock.mockImplementation(function () {
        return mockZero;
      });

      let capturedZero: Zero<Schema> | undefined;

      function TestComponent() {
        capturedZero = useZero<Schema>();
        return <div>test</div>;
      }

      const options: ZeroOptions<Schema> = {
        cacheURL: 'https://example.com',
        userID: 'test-user',
        schema: {} as Schema,
      };

      const root = renderWithRoot(
        <ZeroProvider {...options}>
          <TestComponent />
        </ZeroProvider>,
      );

      expect(ZeroMock).toHaveBeenCalledWith(
        expect.objectContaining({
          cacheURL: 'https://example.com',
          schema: {},
        }),
      );
      const zeroArgs = ZeroMock.mock.calls[0]![0];
      expect(zeroArgs).not.toHaveProperty('auth');
      expect(capturedZero).toBe(mockZero);

      act(() => {
        root.unmount();
      });
    });

    test('calls init callback with constructed zero instance', () => {
      const mockZero = createMockZero();
      const ZeroMock = vi.mocked(ZeroConstructor);
      ZeroMock.mockImplementation(function () {
        return mockZero;
      });

      const initMock = vi.fn();

      const options: ZeroOptions<Schema> & {
        init: (zero: Zero<Schema>) => void;
      } = {
        cacheURL: 'https://example.com',
        userID: 'test-user',
        schema: {} as Schema,
        init: initMock,
      };

      const root = renderWithRoot(
        <ZeroProvider {...options}>
          <div>test</div>
        </ZeroProvider>,
      );

      expect(initMock).toHaveBeenCalledWith(mockZero);
      expect(initMock).toHaveBeenCalledTimes(1);

      act(() => {
        root.unmount();
      });
    });

    test('closes zero on unmount when constructed internally', () => {
      const mockZero = createMockZero();
      const ZeroMock = vi.mocked(ZeroConstructor);
      ZeroMock.mockImplementation(function () {
        return mockZero;
      });

      const options: ZeroOptions<Schema> = {
        cacheURL: 'https://example.com',
        schema: {} as Schema,
        userID: 'test-user',
      };

      const root = renderWithRoot(
        <ZeroProvider {...options}>
          <div>test</div>
        </ZeroProvider>,
      );

      act(() => {
        root.unmount();
      });

      expect(mockZero.close).toHaveBeenCalledTimes(1);
    });
  });

  describe('auth handling', () => {
    test('passes auth to Zero constructor', () => {
      const mockZero = createMockZero();
      const ZeroMock = vi.mocked(ZeroConstructor);
      ZeroMock.mockImplementation(function () {
        return mockZero;
      });

      const auth = 'test-token';

      const options: ZeroOptions<Schema> = {
        cacheURL: 'https://example.com',
        schema: {} as Schema,
        auth,
        userID: 'test-user',
      };

      const root = renderWithRoot(
        <ZeroProvider {...options}>
          <div>test</div>
        </ZeroProvider>,
      );

      expect(ZeroMock).toHaveBeenCalledWith(
        expect.objectContaining({
          auth,
        }),
      );

      act(() => {
        root.unmount();
      });
    });

    test('calls connection.connect when auth changes', () => {
      const mockZero = createMockZero();
      const ZeroMock = vi.mocked(ZeroConstructor);
      ZeroMock.mockImplementation(function () {
        return mockZero;
      });

      const schema = {} as Schema;

      const root = renderWithRoot(
        <ZeroProvider
          cacheURL="https://example.com"
          schema={schema}
          auth="token-1"
          userID="test-user"
        >
          <div>test</div>
        </ZeroProvider>,
      );

      // Initial connection should not be called
      expect(mockZero.connection.connect).toHaveBeenCalledTimes(0);

      vi.clearAllMocks();

      act(() => {
        root.render(
          <ZeroProvider
            cacheURL="https://example.com"
            schema={schema}
            auth="token-2"
            userID="test-user"
          >
            <div>test</div>
          </ZeroProvider>,
        );
      });

      expect(mockZero.connection.connect).toHaveBeenCalledWith({
        auth: 'token-2',
      });
      expect(mockZero.connection.connect).toHaveBeenCalledTimes(1);

      // Zero should not be closed or reconstructed
      expect(mockZero.close).not.toHaveBeenCalled();
      expect(ZeroMock).toHaveBeenCalledTimes(0);

      vi.clearAllMocks();

      // Change auth again
      act(() => {
        root.render(
          <ZeroProvider
            cacheURL="https://example.com"
            schema={schema}
            auth="token-3"
            userID="test-user"
          >
            <div>test</div>
          </ZeroProvider>,
        );
      });
      expect(mockZero.connection.connect).toHaveBeenCalledWith({
        auth: 'token-3',
      });
      expect(mockZero.connection.connect).toHaveBeenCalledTimes(1);

      act(() => {
        root.unmount();
      });
    });

    test('does not call connection.connect when auth is the same value', () => {
      const mockZero = createMockZero();
      const ZeroMock = vi.mocked(ZeroConstructor);
      ZeroMock.mockImplementation(function () {
        return mockZero;
      });

      const schema = {} as Schema;

      const root = renderWithRoot(
        <ZeroProvider
          cacheURL="https://example.com"
          schema={schema}
          auth="token-same"
          userID="test-user"
        >
          <div>test</div>
        </ZeroProvider>,
      );

      act(() => {
        root.render(
          <ZeroProvider
            cacheURL="https://example.com"
            schema={schema}
            auth="token-same"
            userID="test-user"
          >
            <div>test</div>
          </ZeroProvider>,
        );
      });

      expect(mockZero.connection.connect).toHaveBeenCalledTimes(0);
      expect(mockZero.close).not.toHaveBeenCalled();
      expect(ZeroMock).toHaveBeenCalledTimes(1);

      act(() => {
        root.unmount();
      });
    });

    test('calls connection.connect when null auth is provided and then changes', () => {
      const mockZero = createMockZero();
      const ZeroMock = vi.mocked(ZeroConstructor);
      ZeroMock.mockImplementation(function () {
        return mockZero;
      });

      const schema = {} as Schema;

      const root = renderWithRoot(
        <ZeroProvider
          cacheURL="https://example.com"
          schema={schema}
          auth={null}
          userID="test-user"
        >
          <div>test</div>
        </ZeroProvider>,
      );

      act(() => {
        root.render(
          <ZeroProvider
            cacheURL="https://example.com"
            schema={schema}
            auth="token-new"
            userID="test-user"
          >
            <div>test</div>
          </ZeroProvider>,
        );
      });

      expect(mockZero.connection.connect).toHaveBeenCalledWith({
        auth: 'token-new',
      });
      expect(mockZero.connection.connect).toHaveBeenCalledTimes(1);
      expect(mockZero.close).not.toHaveBeenCalled();

      act(() => {
        root.unmount();
      });
    });

    test('calls connection.connect when no auth value is provided and then changes', () => {
      const mockZero = createMockZero();
      const ZeroMock = vi.mocked(ZeroConstructor);
      ZeroMock.mockImplementation(function () {
        return mockZero;
      });

      const schema = {} as Schema;

      const root = renderWithRoot(
        <ZeroProvider
          cacheURL="https://example.com"
          schema={schema}
          userID="test-user"
        >
          <div>test</div>
        </ZeroProvider>,
      );

      act(() => {
        root.render(
          <ZeroProvider
            cacheURL="https://example.com"
            schema={schema}
            auth="token-new"
            userID="test-user"
          >
            <div>test</div>
          </ZeroProvider>,
        );
      });

      expect(mockZero.connection.connect).toHaveBeenCalledWith({
        auth: 'token-new',
      });
      expect(mockZero.connection.connect).toHaveBeenCalledTimes(1);
      expect(mockZero.close).not.toHaveBeenCalled();

      act(() => {
        root.unmount();
      });
    });

    test('calls connection.connect when zero is provided externally and auth changes', () => {
      const mockZero = createMockZero();

      const root = renderWithRoot(
        <ZeroProvider zero={mockZero} auth="token-1">
          <div>test</div>
        </ZeroProvider>,
      );

      // Initial connection should not be called
      expect(mockZero.connection.connect).toHaveBeenCalledTimes(0);

      // Change auth
      act(() => {
        root.render(
          <ZeroProvider zero={mockZero} auth="token-2">
            <div>test</div>
          </ZeroProvider>,
        );
      });

      // Should call connect again with new auth
      expect(mockZero.connection.connect).toHaveBeenCalledWith({
        auth: 'token-2',
      });
      expect(mockZero.connection.connect).toHaveBeenCalledTimes(1);

      act(() => {
        root.unmount();
      });
    });

    test('calls connection.connect with undefined when auth prop is removed', () => {
      const mockZero = createMockZero();

      const root = renderWithRoot(
        <ZeroProvider zero={mockZero} auth="token-initial">
          <div>test</div>
        </ZeroProvider>,
      );

      act(() => {
        root.render(
          <ZeroProvider zero={mockZero}>
            <div>test</div>
          </ZeroProvider>,
        );
      });

      expect(mockZero.connection.connect).toHaveBeenCalledWith({
        auth: undefined,
      });
      expect(mockZero.connection.connect).toHaveBeenCalledTimes(1);
      expect(mockZero.close).not.toHaveBeenCalled();

      act(() => {
        root.unmount();
      });
    });
  });

  describe('children rendering', () => {
    test('does not render children until zero is ready', () => {
      const mockZero = createMockZero();
      const ZeroMock = vi.mocked(ZeroConstructor);
      ZeroMock.mockImplementation(function () {
        return mockZero;
      });

      const childMock = vi.fn(() => <div>child</div>);

      function ChildComponent() {
        childMock();
        return <div>child</div>;
      }

      const root = renderWithRoot(
        <ZeroProvider
          cacheURL="https://example.com"
          schema={{} as Schema}
          userID="test-user"
        >
          <ChildComponent />
        </ZeroProvider>,
      );

      // Children should be rendered after zero is created
      expect(childMock).toHaveBeenCalled();

      act(() => {
        root.unmount();
      });
    });
  });

  describe('prop changes', () => {
    test('recreates zero when server changes', () => {
      const mockZero1 = createMockZero('client-1');
      const mockZero2 = createMockZero('client-2');
      const ZeroMock = vi.mocked(ZeroConstructor);

      let callCount = 0;
      ZeroMock.mockImplementation(function () {
        callCount++;
        return callCount === 1 ? mockZero1 : mockZero2;
      });

      let capturedZero1: Zero<Schema> | undefined;
      let capturedZero2: Zero<Schema> | undefined;

      function TestComponent({label}: {label: string}) {
        const zero = useZero<Schema>();
        if (label === '1') {
          capturedZero1 = zero;
        } else {
          capturedZero2 = zero;
        }
        return <div>{label}</div>;
      }

      const root = renderWithRoot(
        <ZeroProvider
          cacheURL="https://example1.com"
          schema={{} as Schema}
          userID="test-user"
        >
          <TestComponent label="1" />
        </ZeroProvider>,
      );

      expect(ZeroMock).toHaveBeenCalledTimes(1);
      expect(capturedZero1).toBe(mockZero1);

      // Change server
      act(() => {
        root.render(
          <ZeroProvider
            cacheURL="https://example2.com"
            schema={{} as Schema}
            userID="test-user"
          >
            <TestComponent label="2" />
          </ZeroProvider>,
        );
      });

      // Zero should be recreated
      expect(ZeroMock).toHaveBeenCalledTimes(2);
      expect(mockZero1.close).toHaveBeenCalledTimes(1);
      expect(capturedZero2).toBe(mockZero2);
      expect(capturedZero2).not.toBe(capturedZero1);

      act(() => {
        root.unmount();
      });
    });
  });
});
