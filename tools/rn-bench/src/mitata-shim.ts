/**
 * A React Native stand-in for `packages/shared/src/bench.ts`.
 *
 * That module wraps mitata + Vitest, neither of which runs on Hermes. This
 * exposes the same three names — `bench`, `describe`, `use` — on top of the
 * {@link Benchmark} shape the device harness understands, so a package's
 * existing `.bench.ts` files can be compiled for a device unchanged. The RN
 * esbuild build swaps this in with an `onResolve` hook; see
 * `zqlBenchPlugin` in zql-benchmarks/tool/build-rn.ts.
 *
 * ## How a mitata benchmark maps onto a harness benchmark
 *
 * mitata measures one inner iteration; the harness times a whole `run()` and
 * samples that 9-21 times. Each harness rep therefore runs the benchmark body
 * `max_samples` times (100 when unspecified) inside the timed window, so a
 * reported figure is "N iterations", not "one operation" — fixed per benchmark
 * and comparable across runs, but not comparable to mitata's per-op numbers.
 * Both the plain and the generator form loop, so every benchmark in a run is
 * on the same scale.
 *
 * mitata's generator form —
 *
 * ```js
 * bench('push', function* () {
 *   const view = setup();      // once
 *   yield () => { body(); };   // measured
 *   teardown(view);            // after
 * });
 * ```
 *
 * — keeps that structure: the setup and teardown run once per harness rep,
 * outside the timed window, and only the yielded function is timed. Running
 * teardown every rep also bounds the growth that `max_samples` exists to cap
 * upstream, which matters far more on a phone than on a desktop.
 */
import type {Benchmark} from './benchmark.ts';

/** The subset of mitata's options the benchmarks in this repo actually pass. */
export type MeasureOptions = {
  max_samples?: number | undefined;
  min_samples?: number | undefined;
  min_cpu_time?: number | undefined;
};

type Body = () => unknown;
type MeasureFn = (() => unknown) | (() => Generator<Body, unknown, unknown>);

/**
 * Iterations per harness rep when a benchmark does not say. Small enough that
 * a slow benchmark stays responsive, large enough that a microsecond-scale one
 * is not swamped by `performance.now()` resolution.
 */
const DEFAULT_ITERATIONS = 100;

const collected: Benchmark[] = [];
const describeStack: string[] = [];
let currentFile = 'bench';

/** Prevents a benchmark body's result being optimised away (mitata's `use`). */
let sink: unknown;
export function use<T>(value: T): T {
  sink = value;
  return value;
}
/** Keeps `sink` observably live. */
export function readSink(): unknown {
  return sink;
}

/**
 * Set by the build's `onLoad` hook at the top of each `.bench.ts` module, so
 * benchmarks from different files land in different groups even when they use
 * the same `describe` names (several files declare a `push` suite).
 */
export function __setBenchFile(name: string): void {
  currentFile = name;
  describeStack.length = 0;
}

export function describe(name: string, fn: () => void): void {
  describeStack.push(name);
  try {
    fn();
  } finally {
    describeStack.pop();
  }
}

function isGenerator(v: unknown): v is Generator<Body, unknown, unknown> {
  return (
    typeof v === 'object' &&
    v !== null &&
    typeof (v as Generator).next === 'function'
  );
}

export function bench(
  name: string,
  fn: MeasureFn,
  opts?: MeasureOptions,
): void {
  const iterations = opts?.max_samples ?? DEFAULT_ITERATIONS;
  const fullName = [...describeStack, name].join(' / ');

  collected.push({
    name: fullName,
    group: currentFile,
    async run(bencher) {
      const produced = fn();

      if (!isGenerator(produced)) {
        // Plain function form. `fn()` has already run once, so account for it
        // and run the rest, keeping every benchmark on the same scale.
        bencher.reset();
        await produced;
        for (let i = 1; i < iterations; i++) {
          await (fn as () => unknown)();
        }
        bencher.stop();
        return;
      }

      // Generator form: setup ran up to the `yield`; time only the body.
      const first = produced.next();
      const body = first.value;
      if (typeof body !== 'function') {
        throw new Error(
          `Benchmark "${fullName}" yielded ${typeof body}, expected a function`,
        );
      }
      bencher.reset();
      for (let i = 0; i < iterations; i++) {
        await body();
      }
      bencher.stop();
      // Runs whatever follows the `yield` — typically the cleanup that keeps
      // a push benchmark's source from growing without bound.
      produced.next();
    },
  });
}

/** Every benchmark registered by the `.bench.ts` modules imported so far. */
export function collectedBenchmarks(): Benchmark[] {
  return collected;
}
