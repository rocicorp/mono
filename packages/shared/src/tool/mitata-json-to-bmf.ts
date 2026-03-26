/* oxlint-disable no-console */
// Convert bench JSON output to Bencher Metric Format (BMF)
//
// The bench wrapper outputs two JSON shapes:
// 1. Suite (from describe afterAll): {"benchmarks":[{name, stats}]}
// 2. Standalone (individual bench): {name, stats}
//
// Stats: {min, max, avg, p75, p99} in nanoseconds.

type BMFMetric = {
  [key: string]: {
    throughput: {
      value: number;
      lower_value?: number | undefined;
      upper_value?: number | undefined;
    };
  };
};

interface Benchmark {
  name: string;
  stats: {
    min: number;
    max: number;
    avg: number;
    p75: number;
    p99: number;
  };
}

function benchmarkToBMF(bmf: BMFMetric, {name, stats}: Benchmark) {
  // Convert from nanoseconds to operations per second
  // Note: min latency → max throughput, max latency → min throughput
  bmf[name] = {
    throughput: {
      value: 1e9 / stats.avg,
      lower_value: 1e9 / stats.max,
      upper_value: 1e9 / stats.min,
    },
  };
}

async function main() {
  try {
    const chunks: Buffer[] = [];
    for await (const chunk of process.stdin) {
      chunks.push(chunk);
    }
    const content = Buffer.concat(chunks).toString('utf-8');

    const bmf: BMFMetric = {};
    for (const line of content.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('{')) continue;
      let parsed: unknown;
      try {
        parsed = JSON.parse(trimmed);
      } catch {
        continue;
      }
      if (
        typeof parsed === 'object' &&
        parsed !== null &&
        'benchmarks' in parsed &&
        Array.isArray((parsed as {benchmarks: unknown}).benchmarks)
      ) {
        // Suite output: {"benchmarks": [...]}
        for (const b of (parsed as {benchmarks: Benchmark[]}).benchmarks) {
          benchmarkToBMF(bmf, b);
        }
      } else if (
        typeof parsed === 'object' &&
        parsed !== null &&
        'name' in parsed &&
        'stats' in parsed
      ) {
        // Standalone output: {name, stats}
        benchmarkToBMF(bmf, parsed as Benchmark);
      }
    }

    if (Object.keys(bmf).length === 0) {
      throw new Error('No valid benchmark data found in input');
    }

    process.stdout.write(JSON.stringify(bmf, null, 2));
  } catch (error) {
    console.error('Error converting bench JSON to BMF:', error);
    process.exit(1);
  }
}

void main();
