// Usage:
//
// This download the JSON file from the old benchmarking system at
// https://rocicorp.github.io/mono/bundle-sizes/ and convert it to the bencher.dev format.
// Then it uses the Bencher REST API to upload the data.
//
// BENCHER_API_KEY=... node convert-old-file-sizes.js

const branch = 'main';
const testbed = 'localhost';
const projectSlug = 'replicache-bundle-size';
const jsURL = 'https://rocicorp.github.io/mono/bundle-sizes/data.js';
const header = 'window.BENCHMARK_DATA = ';

function benchesToBencher(benches) {
  const bencher = {};
  for (const {name, value} of benches) {
    bencher[fixName(name)] = {
      'file-size': {value},
    };
  }
  return bencher;
}

function fixName(s) {
  s = s.slice('Size of '.length);
  if (s.endsWith(' (Brotli compressed)')) {
    s = s.slice(0, -' (Brotli compressed)'.length);
  }
  return s;
}

async function downloadOldData(url) {
  const data = await fetch(url);
  let text = await data.text();
  if (!text.startsWith(header)) {
    throw new Error('Unexpected header');
  }
  text = text.slice(header.length);
  return JSON.parse(text);
}

if (process.env.BENCHER_API_KEY === undefined) {
  console.error('Please set BENCHER_API_KEY');
  process.exit(1);
}

const json = await downloadOldData(jsURL);

for (const {commit, benches} of json.entries['Bundle Sizes']) {
  const body = {
    branch,
    hash: commit.id,
    ['start_time']: commit.timestamp,
    ['end_time']: commit.timestamp,
    testbed,
    results: [JSON.stringify(benchesToBencher(benches))],
    settings: {
      adapter: 'json',
    },
  };
  await sendReport(body);
}

async function sendReport(body) {
  process.stdout.write(`Sending report for ${body.hash}...`);
  const r = await fetch(
    `https://api.bencher.dev/v0/projects/${projectSlug}/reports`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.BENCHER_API_KEY}`,
      },
      body: JSON.stringify(body),
    },
  );
  if (!r.ok) {
    process.stdout.write(' FAILED\n\n');
    console.error(await r.text());
    process.exit(1);
  }
  process.stdout.write(' ok\n');
}
