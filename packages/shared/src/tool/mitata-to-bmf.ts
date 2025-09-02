import fs from 'node:fs';

// BMF - Bencher Metric Format
type BMFMetric = {
  [key: string]: {
    latency: {
      value: number;
      lower_value?: number;
      upper_value?: number;
    };
  };
};

interface MitataBenchmark {
  name: string;
  avg: number;
  min: number;
  max: number;
  p75?: number;
  p99?: number;
}

function parseMitataOutput(output: string): MitataBenchmark[] {
  const benchmarks: MitataBenchmark[] = [];
  const lines = output.split('\n');
  
  let currentBenchmark: Partial<MitataBenchmark> | null = null;
  let i = 0;
  
  while (i < lines.length) {
    const line = lines[i];
    
    // Match benchmark name and avg time with ANSI codes
    // Format: "zql: benchmark_name    99.98 µs/iter ..."
    const cleanLine = line.replace(/\x1b\[[0-9;]*m/g, ''); // Remove ANSI codes
    const nameMatch = cleanLine.match(/^((?:zql|zqlite|zpg):\s+[^▁▂▃▄▅▆▇█]+?)\s+([0-9.]+)\s+(µs|ms|ns|s)\/iter/);
    
    if (nameMatch) {
      const [, name, value, unit] = nameMatch;
      currentBenchmark = {
        name: name.trim(),
        avg: convertToNanoseconds(parseFloat(value), unit),
      };
      
      // Look at the next line for min/max values
      if (i + 1 < lines.length) {
        const nextLine = lines[i + 1].replace(/\x1b\[[0-9;]*m/g, '');
        // Format: "(88.04 µs … 3.06 ms) 144.21 µs"
        const rangeMatch = nextLine.match(/\(([0-9.]+)\s+(µs|ms|ns|s)[^0-9]+([0-9.]+)\s+(µs|ms|ns|s)\)/);
        if (rangeMatch) {
          const [, min, minUnit, max, maxUnit] = rangeMatch;
          currentBenchmark.min = convertToNanoseconds(parseFloat(min), minUnit);
          currentBenchmark.max = convertToNanoseconds(parseFloat(max), maxUnit);
          
          // Also try to get p75 value if present
          const p75Match = nextLine.match(/\)\s+([0-9.]+)\s+(µs|ms|ns|s)/);
          if (p75Match) {
            const [, p75Value, p75Unit] = p75Match;
            currentBenchmark.p75 = convertToNanoseconds(parseFloat(p75Value), p75Unit);
          }
        }
      }
      
      if (currentBenchmark.name && currentBenchmark.avg !== undefined) {
        benchmarks.push(currentBenchmark as MitataBenchmark);
      }
      currentBenchmark = null;
    }
    
    i++;
  }
  
  return benchmarks;
}

function convertToNanoseconds(value: number, unit: string): number {
  switch (unit) {
    case 'ns':
      return value;
    case 'µs':
      return value * 1000;
    case 'ms':
      return value * 1000000;
    case 's':
      return value * 1000000000;
    default:
      throw new Error(`Unknown time unit: ${unit}`);
  }
}

function convertMitataToBMF(benchmarks: MitataBenchmark[]): BMFMetric {
  const bmf: BMFMetric = {};
  
  for (const benchmark of benchmarks) {
    bmf[benchmark.name] = {
      latency: {
        value: benchmark.avg,
        lower_value: benchmark.min,
        upper_value: benchmark.max,
      },
    };
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
    const mitataOutput = Buffer.concat(chunks).toString('utf-8');
    
    const benchmarks = parseMitataOutput(mitataOutput);
    const bmfOutput = convertMitataToBMF(benchmarks);
    
    process.stdout.write(JSON.stringify(bmfOutput, null, 2));
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error('Error converting mitata output to BMF:', error);
    process.exit(1);
  }
}

main();