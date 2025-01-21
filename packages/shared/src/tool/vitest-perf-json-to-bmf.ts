import fs from 'node:fs';

// BMF - Bencher Metric Format
// with json schema:
/*

{
  "$id": "https://bencher.dev/bmf.json",
  "$schema": "http://json-schema.org/draft-07/schema",
  "type": "object",
  "patternProperties": {
    ".+": {
      "type": "object",
      "patternProperties": {
        ".+": {
          "type": "object",
          "properties": {
            "value": {
              "type": "number"
            },
            "lower_value": {
              "type": "number"
            },
            "upper_value": {
              "type": "number"
            }
          },
          "required": ["value"]
        }
      }
    }
  }
}

*/
// vitest bench --run --outputJSON output.json produces:
/*

{
  "files": [
    {
      "filepath": "/Users/arv/src/rocicorp/mono/packages/zero-client/src/client/zero.bench.ts",
      "groups": [
        {
          "fullName": "src/client/zero.bench.ts > basics",
          "benchmarks": [
            {
              "id": "1889292170_0_0",
              "name": "All 1000 rows x 10 columns (numbers)",
              "rank": 1,
              "rme": 22.99615963676964,
              "samples": [],
              "totalTime": 509.60000014305115,
              "min": 16.09999990463257,
              "max": 72.20000004768372,
              "hz": 47.095761368255296,
              "period": 21.233333339293797,
              "mean": 21.233333339293797,
              "variance": 133.67101490532144,
              "sd": 11.561618178495666,
              "sem": 2.360005428183389,
              "df": 23,
              "critical": 2.069,
              "moe": 4.882851230911432,
              "p75": 19.09999990463257,
              "p99": 72.20000004768372,
              "p995": 72.20000004768372,
              "p999": 72.20000004768372,
              "sampleCount": 24,
              "median": 18.40000009536743
            }
          ]
        },
        {
          "fullName": "src/client/zero.bench.ts > with filter",
          "benchmarks": [
            {
              "id": "1889292170_1_0",
              "name": "Lower rows 500 x 10 columns (numbers)",
              "rank": 1,
              "rme": 24.258926361493792,
              "samples": [],
              "totalTime": 516.9000000953674,
              "min": 26.40000009536743,
              "max": 74.09999990463257,
              "hz": 29.01915263538889,
              "period": 34.46000000635783,
              "mean": 34.46000000635783,
              "variance": 227.82971388912205,
              "sd": 15.09402908070347,
              "sem": 3.8972615504489987,
              "df": 14,
              "critical": 2.145,
              "moe": 8.359626025713101,
              "p75": 31.40000009536743,
              "p99": 74.09999990463257,
              "p995": 74.09999990463257,
              "p999": 74.09999990463257,
              "sampleCount": 15,
              "median": 28.100000143051147
            }
          ]
        }
      ]
    }
  ]
}
  */

type VitestBenchmark = {
  id: string;
  name: string;
  rank: number;
  rme: number;
  samples: unknown[];
  totalTime: number;
  min: number;
  max: number;
  hz: number;
  period: number;
  mean: number;
  variance: number;
  sd: number;
  sem: number;
  df: number;
  critical: number;
  moe: number;
  p75: number;
  p99: number;
  p995: number;
  p999: number;
  sampleCount: number;
  median: number;
};

type VitestGroup = {
  fullName: string;
  benchmarks: VitestBenchmark[];
};

type VitestFile = {
  filepath: string;
  groups: VitestGroup[];
};

type VitestOutput = {
  files: VitestFile[];
};

type BMFMetric = {
  [key: string]: {
    throughput: {
      value: number;
      ['lower_value']: number;
      ['upper_value']: number;
    };
  };
};

function convertVitestToBMF(vitestOutput: VitestOutput): BMFMetric {
  const bmf: BMFMetric = {};

  vitestOutput.files.forEach(file => {
    file.groups.forEach(group => {
      group.benchmarks.forEach(benchmark => {
        const metricName = `${group.fullName} > ${benchmark.name}`;

        bmf[metricName] = {
          throughput: {
            value: benchmark.mean,
            ['lower_value']: benchmark.min,
            ['upper_value']: benchmark.max,
          },
        };
      });
    });
  });

  return bmf;
}

try {
  const vitestOutputContent = fs.readFileSync(process.stdin.fd, 'utf-8');
  const vitestOutput: VitestOutput = JSON.parse(vitestOutputContent);
  const bmfOutput = convertVitestToBMF(vitestOutput);
  fs.writeFileSync(
    process.stdout.fd,
    JSON.stringify(bmfOutput, null, 2),
    'utf-8',
  );
} catch (error) {
  // eslint-disable-next-line no-console
  console.error('Error converting Vitest output to BMF:', error);
  process.exit(1);
}
