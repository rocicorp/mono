import {afterEach, describe, expect, test, vi} from 'vitest';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import type {Change} from '../ivm/change.ts';
import {makeAddChange} from '../ivm/change.ts';
import type {Node} from '../ivm/data.ts';
import type {FetchRequest, Input, Output} from '../ivm/operator.ts';
import type {SourceSchema} from '../ivm/schema.ts';
import {MeasurePushOperator} from './measure-push-operator.ts';
import type {MetricsDelegate} from './metrics-delegate.ts';

describe('MeasurePushOperator', () => {
  test('should pass through fetch calls', () => {
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: vi.fn(() => []),
      getSchema: vi.fn(() => ({}) as SourceSchema),
      destroy: vi.fn(),
    };

    const mockMetricsDelegate: MetricsDelegate = {
      addMetric: vi.fn(),
    };

    const measurePushOperator = new MeasurePushOperator(
      mockInput,
      'test-query-id',
      mockMetricsDelegate,
      'query-update-client',
    );
    const req = {} as FetchRequest;

    measurePushOperator.fetch(req);

    expect(mockInput.fetch).toHaveBeenCalledWith(req);
  });

  test('should pass through getSchema calls', () => {
    const schema = {} as SourceSchema;
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: vi.fn(() => []),
      getSchema: vi.fn(() => schema),
      destroy: vi.fn(),
    };

    const mockMetricsDelegate: MetricsDelegate = {
      addMetric: vi.fn(),
    };

    const measurePushOperator = new MeasurePushOperator(
      mockInput,
      'test-query-id',
      mockMetricsDelegate,
      'query-update-client',
    );

    const result = measurePushOperator.getSchema();

    expect(result).toBe(schema);
    expect(mockInput.getSchema).toHaveBeenCalled();
  });

  test('should pass through destroy calls', () => {
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: vi.fn(() => []),
      getSchema: vi.fn(() => ({}) as SourceSchema),
      destroy: vi.fn(),
    };

    const mockMetricsDelegate: MetricsDelegate = {
      addMetric: vi.fn(),
    };

    const measurePushOperator = new MeasurePushOperator(
      mockInput,
      'test-query-id',
      mockMetricsDelegate,
      'query-update-client',
    );

    measurePushOperator.destroy();

    expect(mockInput.destroy).toHaveBeenCalled();
  });

  test('should measure push timing and record metric', () => {
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: vi.fn(() => []),
      getSchema: vi.fn(() => ({}) as SourceSchema),
      destroy: vi.fn(),
    };

    const mockOutput: Output = {
      push: vi.fn(() => emptyArray),
    };

    const mockMetricsDelegate: MetricsDelegate = {
      addMetric: vi.fn(),
    };

    const measurePushOperator = new MeasurePushOperator(
      mockInput,
      'test-query-id',
      mockMetricsDelegate,
      'query-update-client',
    );
    measurePushOperator.setOutput(mockOutput);

    const change: Change = makeAddChange({} as Node);

    [...measurePushOperator.push(change)];

    expect(mockOutput.push).toHaveBeenCalledWith(change, measurePushOperator);
    expect(mockMetricsDelegate.addMetric).toHaveBeenCalledWith(
      'query-update-client',
      expect.any(Number),
      'test-query-id',
    );
  });

  test('should record the time spent when output.push throws', () => {
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: vi.fn(() => []),
      getSchema: vi.fn(() => ({}) as SourceSchema),
      destroy: vi.fn(),
    };

    const mockOutput: Output = {
      push: vi.fn(() => {
        throw new Error('Test error');
      }),
    };

    const mockMetricsDelegate: MetricsDelegate = {
      addMetric: vi.fn(),
    };

    const measurePushOperator = new MeasurePushOperator(
      mockInput,
      'test-query-id',
      mockMetricsDelegate,
      'query-update-client',
    );
    measurePushOperator.setOutput(mockOutput);

    const change: Change = makeAddChange({} as Node);

    expect(() => [...measurePushOperator.push(change)]).toThrow('Test error');
    // A push aborted for taking too long is accounted to its query.
    expect(mockMetricsDelegate.addMetric).toHaveBeenCalledWith(
      'query-update-client',
      expect.any(Number),
      'test-query-id',
    );
  });

  test('should measure execution time and record metric for reconcile', () => {
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: vi.fn(() => []),
      getSchema: vi.fn(() => ({}) as SourceSchema),
      destroy: vi.fn(),
    };

    const mockOutput: Output = {
      push: vi.fn(() => emptyArray),
      reconcile: vi.fn(() => emptyArray),
    };

    const mockMetricsDelegate: MetricsDelegate = {
      addMetric: vi.fn(),
    };

    const measurePushOperator = new MeasurePushOperator(
      mockInput,
      'test-query-id',
      mockMetricsDelegate,
      'query-update-server',
    );
    measurePushOperator.setOutput(mockOutput);

    [...measurePushOperator.reconcile(mockInput)];

    expect(mockOutput.reconcile).toHaveBeenCalledWith(measurePushOperator);
    expect(mockMetricsDelegate.addMetric).toHaveBeenCalledWith(
      'query-update-server',
      expect.any(Number),
      'test-query-id',
    );
  });

  test('should record the time spent when output.reconcile throws', () => {
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: vi.fn(() => []),
      getSchema: vi.fn(() => ({}) as SourceSchema),
      destroy: vi.fn(),
    };

    const mockOutput: Output = {
      push: vi.fn(() => emptyArray),
      reconcile: vi.fn(() => {
        throw new Error('Reconcile error');
      }),
    };

    const mockMetricsDelegate: MetricsDelegate = {
      addMetric: vi.fn(),
    };

    const measurePushOperator = new MeasurePushOperator(
      mockInput,
      'test-query-id',
      mockMetricsDelegate,
      'query-update-server',
    );
    measurePushOperator.setOutput(mockOutput);

    expect(() => [...measurePushOperator.reconcile(mockInput)]).toThrow(
      'Reconcile error',
    );
    expect(mockMetricsDelegate.addMetric).toHaveBeenCalledWith(
      'query-update-server',
      expect.any(Number),
      'test-query-id',
    );
  });

  describe('time suspended at a yield', () => {
    function setup() {
      let now = 0;
      vi.spyOn(performance, 'now').mockImplementation(() => now);
      const advance = (ms: number) => {
        now += ms;
      };
      const mockOutput: Output = {
        // Works for 10 ms before each of two yields, and 10 ms after.
        *push() {
          advance(10);
          yield 'yield';
          advance(10);
          yield 'yield';
          advance(10);
        },
      };
      const mockMetricsDelegate: MetricsDelegate = {addMetric: vi.fn()};
      const measurePushOperator = new MeasurePushOperator(
        {
          setOutput: vi.fn(),
          fetch: vi.fn(() => []),
          getSchema: vi.fn(() => ({}) as SourceSchema),
          destroy: vi.fn(),
        },
        'test-query-id',
        mockMetricsDelegate,
        'query-update-server',
      );
      measurePushOperator.setOutput(mockOutput);
      return {
        push: () => measurePushOperator.push(makeAddChange({} as Node)),
        advance,
        addMetric: mockMetricsDelegate.addMetric,
      };
    }

    afterEach(() => {
      vi.restoreAllMocks();
    });

    test('is not counted', () => {
      const {push, advance, addMetric} = setup();
      for (const _ of push()) {
        // The caller yields the thread, and other work runs.
        advance(1000);
      }
      expect(addMetric).toHaveBeenCalledExactlyOnceWith(
        'query-update-server',
        30,
        'test-query-id',
      );
    });

    test('is not counted when the push is abandoned there', () => {
      const {push, advance, addMetric} = setup();
      const it = push()[Symbol.iterator]();
      it.next();
      advance(1000);
      it.return?.();
      expect(addMetric).toHaveBeenCalledExactlyOnceWith(
        'query-update-server',
        10,
        'test-query-id',
      );
    });
  });
});
