// Convert mitata JSON output (without samples) to Bencher Metric Format (BMF)

// BMF - Bencher Metric Format
type BMFMetric = {
  [key: string]: {
    latency: {
      value: number;
      // eslint-disable-next-line @typescript-eslint/naming-convention
      lower_value?: number;
      // eslint-disable-next-line @typescript-eslint/naming-convention
      upper_value?: number;
    };
  };
};

// Mitata JSON output format (with samples: false, debug: false)
interface MitataBenchmark {
  name?: string;
  alias?: string;
  stats?: {
    min: number;
    max: number;
    avg: number;
    p50?: number;
    p75?: number;
    p99?: number;
  };
  runs?: Array<{
    stats: {
      min: number;
      max: number;
      avg: number;
      p50?: number;
      p75?: number;
      p99?: number;
    };
  }>;
}

interface MitataJsonOutput {
  benchmarks: MitataBenchmark[];
}

function convertMitataJsonToBMF(mitataOutput: MitataJsonOutput): BMFMetric {
  const bmf: BMFMetric = {};

  for (const benchmark of mitataOutput.benchmarks) {
    // Get the benchmark name
    const name = benchmark.alias || benchmark.name;
    if (!name) continue;

    // Get stats either from top level or from first run
    let {stats} = benchmark;
    if (!stats && benchmark.runs && benchmark.runs[0]) {
      stats = benchmark.runs[0].stats;
    }

    if (stats) {
      bmf[name] = {
        latency: {
          value: stats.avg,
          // eslint-disable-next-line @typescript-eslint/naming-convention
          lower_value: stats.min,
          // eslint-disable-next-line @typescript-eslint/naming-convention
          upper_value: stats.max,
        },
      };
    }
  }

  return bmf;
}

async function main() {
  try {
    // Read all stdin data
    const chunks: Buffer[] = [];
    for await (const chunk of process.stdin) {
      chunks.push(chunk);
    }
    const content = Buffer.concat(chunks).toString('utf-8');

    // Split content by lines and find JSON objects
    // Mitata outputs complete JSON objects, so we can split by lines and look for objects
    const lines = content.split('\n');
    const allBenchmarks: MitataBenchmark[] = [];

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      if (line.startsWith('{') && line.includes('"benchmarks"')) {
        try {
          const parsed = JSON.parse(line) as MitataJsonOutput;
          if (parsed.benchmarks) {
            allBenchmarks.push(...parsed.benchmarks);
          }
        } catch {
          // Skip invalid JSON lines
        }
      }
    }

    if (allBenchmarks.length === 0) {
      throw new Error('No valid mitata benchmark data found in input');
    }

    const mitataOutput: MitataJsonOutput = {benchmarks: allBenchmarks};

    const bmfOutput = convertMitataJsonToBMF(mitataOutput);
    process.stdout.write(JSON.stringify(bmfOutput, null, 2));
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error('Error converting mitata JSON to BMF:', error);
    process.exit(1);
  }
}

void main();
