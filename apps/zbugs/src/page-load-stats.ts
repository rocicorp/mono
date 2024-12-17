import {umami} from './umami.js';

function assignLatencyToBucket(latencyMs: number): string {
  const buckets = [
    250, 500, 1000, 1500, 2000, 2500, 3000, 4000, 5000, 6000, 7000, 8000, 10000,
    15000, 20000, 30000,
  ];

  for (let i = 0; i < buckets.length; i++) {
    if (latencyMs <= buckets[i]) {
      return `${i === 0 ? 0 : buckets[i - 1] + 1}-${buckets[i]}`;
    }
  }

  return '20001+';
}

let pageLoadRecorded = false;

export function recordPageLoad(page: string) {
  if (pageLoadRecorded) {
    return;
  }
  pageLoadRecorded = true;
  const loadLatencyMs = performance.now();
  const bucket = assignLatencyToBucket(loadLatencyMs);
  console.log(
    `Page load time for ${page}: ${loadLatencyMs}ms, Bucket: ${bucket}`,
  );
  umami.track('Page load', {latencyMS: bucket, page});
  umami.track(`Page load ${page}`, {latencyMS: bucket});
}
