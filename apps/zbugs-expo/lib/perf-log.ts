let lastMark = {
  event: 'js-loaded',
  time: performance.now(),
};
const start = performance.now();

// if the queryString includes `perf`, log performance events
let enabled = false;
if (location.search.includes('perf')) {
  enabled = true;
  console.info('js-loaded', {
    ...lastMark,
    elapsed: 0,
  });
}

export function mark(event: string) {
  if (!enabled || typeof performance === 'undefined') {
    return;
  }
  const time = performance.now();
  console.info({
    event,
    sinceLast: time - lastMark.time,
    elapsed: time - start,
    time,
  });
  lastMark = {
    event,
    time,
  };
}
