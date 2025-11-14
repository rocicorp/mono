import {Lock} from '@rocicorp/lock';
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {fork} from 'node:child_process';
import {WebSocketServer} from 'ws';
import {randInt} from '../../../packages/shared/src/rand.ts';
import type {Statement} from '../../../packages/zqlite/src/db.ts';
import {Database} from '../../../packages/zqlite/src/db.ts';

const lc = new LogContext('info', {}, consoleLogSink);

const queue = new Lock();

function yieldProcess() {
  // The old yield implementation ends up scheduling time slices
  // for all pipelines in the same iteration of the event loop,
  // potentially starving the I/O and incurring a delay proportional
  // to the number of pipelines.
  //
  // return new Promise(resolve => setTimeout(resolve, 0));

  // This yield implementation runs one time slice per event loop,
  // allowing I/O to be processed between slices, capping the
  // ping delay to the duration of a single time slice.
  return queue.withLock(() => new Promise(setImmediate));
}

class YieldingPipeline {
  readonly lc: LogContext;
  readonly stmt: Statement;

  constructor(id: number) {
    this.lc = lc.withContext('id', id);
    const db = new Database(
      this.lc,
      process.env.DB_FILE ?? '/tmp/zbugs-sync-replica.db',
      undefined,
      Number.MAX_SAFE_INTEGER, // Don't log slow queries
    );
    this.stmt = db.prepare('SELECT * FROM comment');
  }

  async run() {
    const start = performance.now();
    let lapStart = start;
    let i = 0;
    for (const _ of this.stmt.iterate()) {
      if (i++ % 500000 === 0) {
        const lapElapsed = performance.now() - lapStart;
        this.lc.debug?.(`Yielding at ${i} rows ${lapElapsed.toFixed(2)} ms`);
        await yieldProcess();
        lapStart = performance.now();
      }
    }
    const elapsed = performance.now() - start;
    this.lc.info?.(`Finished fetching ${i} rows in ${elapsed.toFixed(2)} ms`);
  }
}

async function runPipelines(numPipelines: number) {
  const pipelines: YieldingPipeline[] = Array.from(
    {length: numPipelines},
    (_, i) => new YieldingPipeline(i),
  );

  await Promise.all(pipelines.map(p => p.run()));
}

const port = randInt(10000, 32767);
const wss = new WebSocketServer({port});
lc.debug?.(`Running server on port ${port}`);
wss.on('connection', ws => {
  lc.debug?.(`Received client connection`);
  ws.on('message', (data: Buffer) => {
    const {ping} = JSON.parse(data.toString()) as {ping: number};
    const pong = Date.now() - ping;
    ws.send(JSON.stringify({ping, pong}));
  });
});

fork(new URL('./pinger.ts', import.meta.url), [String(port)]);

const argv = process.argv.slice(2);
const numPipelines = argv.length ? parseInt(argv[0]) : 20;

lc.info?.(`Simulating ${numPipelines} pipeline(s) ...`);
const start = performance.now();
await runPipelines(numPipelines);
const elapsed = performance.now() - start;
lc.info?.(`Finished ${elapsed.toFixed(2)} ms`);
