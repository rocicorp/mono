import {bench, describe} from 'vitest';
import type {Change} from '../ivm/change.ts';
import type {Node} from '../ivm/data.ts';
import type {Input, Output} from '../ivm/operator.ts';
import type {SourceSchema} from '../ivm/schema.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import {MeasurePushOperator} from './measure-push-operator.ts';
import type {MetricsDelegate} from './metrics-delegate.ts';

const PUSH_COUNT = 41_000;

const change: Change = {type: 'add', node: {} as Node};

const mockInput: Input = {
	setOutput: () => {},
	fetch: () => [],
	getSchema: () => ({}) as SourceSchema,
	destroy: () => {},
};

const mockOutput: Output = {
	push: () => emptyArray,
};

function makeOperator(delegateOverrides?: Record<string, unknown>) {
	const delegate = {
		addMetric: () => {},
		...delegateOverrides,
	} as MetricsDelegate;
	const op = new MeasurePushOperator(
		mockInput,
		'bench-query-id',
		delegate,
		'query-update-client',
	);
	op.setOutput(mockOutput);
	return op;
}

function pushN(operator: MeasurePushOperator, n: number): void {
	for (let i = 0; i < n; i++) {
		// Drain the generator (push is a generator function)
		// eslint-disable-next-line @typescript-eslint/no-unused-expressions
		for (const _ of operator.push(change)) {
			// consume
		}
	}
}

describe('MeasurePushOperator sampling overhead (41k pushes)', () => {
	bench('sampleRate=1 (measure every push, default)', () => {
		const op = makeOperator();
		pushN(op, PUSH_COUNT);
	});

	bench('disableMetrics: true (no measurement)', () => {
		const op = makeOperator({disableMetrics: true});
		pushN(op, PUSH_COUNT);
	});

	bench('sampleRate=0.01 (measure 1 in 100)', () => {
		const op = makeOperator({metricsSampleRate: 0.01});
		pushN(op, PUSH_COUNT);
	});

	bench('sampleRate=0 (disabled)', () => {
		const op = makeOperator({metricsSampleRate: 0});
		pushN(op, PUSH_COUNT);
	});
});
