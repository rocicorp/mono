import {describe, expect, test, vi} from 'vitest';
import type {Change} from '../ivm/change.ts';
import type {Node} from '../ivm/data.ts';
import type {FetchRequest, Input, Output} from '../ivm/operator.ts';
import type {SourceSchema} from '../ivm/schema.ts';
import {MeasurePushOperator} from './measure-push-operator.ts';
import type {MetricsDelegate} from './metrics-delegate.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';

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

    const change: Change = {
      type: 'add',
      node: {} as Node,
    };

    [...measurePushOperator.push(change)];

    expect(mockOutput.push).toHaveBeenCalledWith(change, measurePushOperator);
    expect(mockMetricsDelegate.addMetric).toHaveBeenCalledWith(
      'query-update-client',
      expect.any(Number),
      'test-query-id',
    );
  });

  test('should not record metric when output.push throws', () => {
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

    const change: Change = {
      type: 'add',
      node: {} as Node,
    };

    expect(() => [...measurePushOperator.push(change)]).toThrow('Test error');
    expect(mockMetricsDelegate.addMetric).not.toHaveBeenCalled();
  });

  describe('sampling', () => {
    function makeFixture(delegateOverrides?: Partial<MetricsDelegate> & Record<string, unknown>) {
      const mockInput: Input = {
        setOutput: vi.fn(),
        fetch: vi.fn(() => []),
        getSchema: vi.fn(() => ({}) as SourceSchema),
        destroy: vi.fn(),
      };
      const mockOutput: Output = {
        push: vi.fn(() => emptyArray),
      };
      const mockMetricsDelegate = {
        addMetric: vi.fn(),
        ...delegateOverrides,
      } as MetricsDelegate;
      return {mockInput, mockOutput, mockMetricsDelegate};
    }

    function pushN(operator: MeasurePushOperator, n: number): void {
      const change: Change = {type: 'add', node: {} as Node};
      for (let i = 0; i < n; i++) {
        [...operator.push(change)];
      }
    }

    test('default behavior (sampleRate=1): every push is measured', () => {
      const {mockInput, mockOutput, mockMetricsDelegate} = makeFixture();
      const operator = new MeasurePushOperator(
        mockInput,
        'qid',
        mockMetricsDelegate,
        'query-update-client',
      );
      operator.setOutput(mockOutput);

      pushN(operator, 5);

      expect(mockMetricsDelegate.addMetric).toHaveBeenCalledTimes(5);
      expect(mockOutput.push).toHaveBeenCalledTimes(5);
    });

    test('disableMetrics: true skips all measurement', () => {
      const {mockInput, mockOutput, mockMetricsDelegate} = makeFixture({
        disableMetrics: true,
      } as unknown as MetricsDelegate);
      const operator = new MeasurePushOperator(
        mockInput,
        'qid',
        mockMetricsDelegate,
        'query-update-client',
      );
      operator.setOutput(mockOutput);

      pushN(operator, 10);

      expect(mockMetricsDelegate.addMetric).not.toHaveBeenCalled();
      // output.push is still called for every push even when metrics are disabled
      expect(mockOutput.push).toHaveBeenCalledTimes(10);
    });

    test('sampleRate=0 disables measurement', () => {
      const {mockInput, mockOutput, mockMetricsDelegate} = makeFixture({
        metricsSampleRate: 0,
      } as unknown as MetricsDelegate);
      const operator = new MeasurePushOperator(
        mockInput,
        'qid',
        mockMetricsDelegate,
        'query-update-client',
      );
      operator.setOutput(mockOutput);

      pushN(operator, 10);

      expect(mockMetricsDelegate.addMetric).not.toHaveBeenCalled();
      expect(mockOutput.push).toHaveBeenCalledTimes(10);
    });

    test('fractional sampleRate measures every Nth push', () => {
      // sampleRate=0.5 => sampleEvery = max(2, round(1/0.5)) = 2
      // So every 2nd push is measured
      const {mockInput, mockOutput, mockMetricsDelegate} = makeFixture({
        metricsSampleRate: 0.5,
      } as unknown as MetricsDelegate);
      const operator = new MeasurePushOperator(
        mockInput,
        'qid',
        mockMetricsDelegate,
        'query-update-client',
      );
      operator.setOutput(mockOutput);

      pushN(operator, 6);

      // With sampleEvery=2: pushes 2, 4, 6 are measured (countdown resets each time)
      expect(mockMetricsDelegate.addMetric).toHaveBeenCalledTimes(3);
      expect(mockOutput.push).toHaveBeenCalledTimes(6);
    });

    test('sampleRate=0.25 measures every 4th push', () => {
      // sampleRate=0.25 => sampleEvery = max(2, round(1/0.25)) = 4
      const {mockInput, mockOutput, mockMetricsDelegate} = makeFixture({
        metricsSampleRate: 0.25,
      } as unknown as MetricsDelegate);
      const operator = new MeasurePushOperator(
        mockInput,
        'qid',
        mockMetricsDelegate,
        'query-update-client',
      );
      operator.setOutput(mockOutput);

      pushN(operator, 8);

      // Measured at push 4 and push 8
      expect(mockMetricsDelegate.addMetric).toHaveBeenCalledTimes(2);
      expect(mockOutput.push).toHaveBeenCalledTimes(8);
    });

    test('sampleRate > 1 clamps to 1: every push is measured', () => {
      const {mockInput, mockOutput, mockMetricsDelegate} = makeFixture({
        metricsSampleRate: 5,
      } as unknown as MetricsDelegate);
      const operator = new MeasurePushOperator(
        mockInput,
        'qid',
        mockMetricsDelegate,
        'query-update-client',
      );
      operator.setOutput(mockOutput);

      pushN(operator, 4);

      expect(mockMetricsDelegate.addMetric).toHaveBeenCalledTimes(4);
      expect(mockOutput.push).toHaveBeenCalledTimes(4);
    });

    test('sampleRate < 0 clamps to 0: no measurement', () => {
      const {mockInput, mockOutput, mockMetricsDelegate} = makeFixture({
        metricsSampleRate: -3,
      } as unknown as MetricsDelegate);
      const operator = new MeasurePushOperator(
        mockInput,
        'qid',
        mockMetricsDelegate,
        'query-update-client',
      );
      operator.setOutput(mockOutput);

      pushN(operator, 6);

      expect(mockMetricsDelegate.addMetric).not.toHaveBeenCalled();
      expect(mockOutput.push).toHaveBeenCalledTimes(6);
    });

    test('metricsSampleRate=1 measures every push', () => {
      const {mockInput, mockOutput, mockMetricsDelegate} = makeFixture({
        metricsSampleRate: 1,
      } as unknown as MetricsDelegate);
      const operator = new MeasurePushOperator(
        mockInput,
        'qid',
        mockMetricsDelegate,
        'query-update-client',
      );
      operator.setOutput(mockOutput);

      pushN(operator, 3);

      expect(mockMetricsDelegate.addMetric).toHaveBeenCalledTimes(3);
    });

    test('non-object metricsDelegate uses default (measure every push)', () => {
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

      const operator = new MeasurePushOperator(
        mockInput,
        'qid',
        mockMetricsDelegate,
        'query-update-client',
      );
      operator.setOutput(mockOutput);

      pushN(operator, 4);

      expect(mockMetricsDelegate.addMetric).toHaveBeenCalledTimes(4);
    });
  });
});
