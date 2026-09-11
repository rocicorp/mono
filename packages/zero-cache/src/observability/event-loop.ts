import {monitorEventLoopDelay, performance} from 'node:perf_hooks';
import type {ObservableResult} from '@opentelemetry/api';
import {getOrCreateGauge} from './metrics.ts';

/**
 * Sampling resolution of the delay histogram, and therefore the floor of every
 * value it reports: the monitor measures the interval between its own timer
 * fires, so a completely idle loop reads as one resolution rather than zero.
 * 10 ms keeps that floor an order of magnitude below the delays worth acting
 * on, and matches what `prom-client` samples the same API at.
 */
const RESOLUTION_MS = 10;

/**
 * Exports how busy this worker's event loop is, which is what separates
 * "expensive query on an idle worker" from "ordinary query waiting behind N
 * peers". The per-query and per-wave timings cannot answer it: a wave whose
 * wall time is ten times its processing time looks identical whether the loop
 * was saturated or the wave was blocked on I/O.
 *
 * Both instruments are reported per worker process through the
 * `process.worker` / `process.worker_index` resource attributes set in
 * `otel-start.ts`, and carry no other dimension.
 *
 * @returns a function that stops the monitor.
 */
export function startEventLoopMonitor(): () => void {
  const delay = monitorEventLoopDelay({resolution: RESOLUTION_MS});
  delay.enable();
  let lastUtilization = performance.eventLoopUtilization();

  // Reported per collection interval, not cumulatively: the histogram is reset
  // after each observation so a burst of blocking work does not stay in `max`
  // for the lifetime of the process.
  const delayGauge = getOrCreateGauge('server', 'event_loop_delay', {
    description:
      'Delay between when the event loop was scheduled to run and when it ' +
      'actually ran, over the last collection interval. A worker whose p99 ' +
      'is large is saturated: work queued on it waits that long before it ' +
      'starts, no matter how cheap the work is. Values have a floor of the ' +
      '10 ms sampling resolution, so an idle worker reports 10 ms rather ' +
      'than 0.',
    unit: 'millisecond',
  });
  const observeDelay = (result: ObservableResult) => {
    result.observe(delay.mean / 1e6, {stat: 'mean'});
    result.observe(delay.percentile(50) / 1e6, {stat: 'p50'});
    result.observe(delay.percentile(99) / 1e6, {stat: 'p99'});
    result.observe(delay.max / 1e6, {stat: 'max'});
    delay.reset();
  };
  delayGauge.addCallback(observeDelay);

  const utilizationGauge = getOrCreateGauge(
    'server',
    'event_loop_utilization',
    {
      description:
        'Fraction of the last collection interval that the event loop spent ' +
        'active rather than idle. Near 1 means the worker is CPU-bound and ' +
        'adding client groups to it only adds queueing.',
      unit: '1',
    },
  );
  const observeUtilization = (result: ObservableResult) => {
    const current = performance.eventLoopUtilization();
    result.observe(
      performance.eventLoopUtilization(current, lastUtilization).utilization,
    );
    lastUtilization = current;
  };
  utilizationGauge.addCallback(observeUtilization);

  return () => {
    delayGauge.removeCallback(observeDelay);
    utilizationGauge.removeCallback(observeUtilization);
    delay.disable();
  };
}
