/**
 * Hermes CPU profiling over Metro's CDP inspector proxy.
 *
 * React Native's modern inspector does NOT implement the CDP `Profiler` domain
 * (unknown methods there get no response at all, which looks like a hang) — it
 * implements `Tracing`, which wraps Hermes' sampling profiler and emits the
 * samples as Chrome trace events.
 *
 * Four things about this path are easy to get wrong and every one of them fails
 * silently rather than with an error:
 *
 *  - the proxy checks the `Origin` header and rejects with HTTP 401 without one;
 *  - the origin and the URL host must match its own `serverBaseUrl`, which is
 *    `127.0.0.1`. Connecting to `localhost` is accepted and then dropped
 *    immediately with close code 1006;
 *  - `Tracing.start` must name the CPU-profiler category explicitly. Without it
 *    you still get timeline and user-timing events but zero CPU samples, because
 *    RN gates Hermes' sampling profiler on `Category::JavaScriptSampling`
 *    (ReactCommon/jsinspector-modern/RuntimeAgent.cpp);
 *  - `Runtime.evaluate`'s `awaitPromise` does not await React Native's Promise,
 *    which is a polyfill rather than a native promise — it returns the raw
 *    `{_A,_x,_y,_z}` object immediately. Long-running work has to be driven by
 *    setting a flag on `globalThis` and polling it.
 */
import WebSocket from 'ws';

const HOST = '127.0.0.1';

const PACKAGE_PATH_RE = /^.*\/(packages|node_modules)\//;

/**
 * `disabled-by-default-v8.cpu_profiler` is what actually turns on Hermes'
 * sampling profiler. `blink.user_timing` carries the performance.mark/measure
 * pairs that {@link runBenchmark} emits, which is how {@link selfTimeByFrame}
 * narrows the samples to just the region the bencher timed.
 */
const TRACING_CATEGORIES =
  'disabled-by-default-v8.cpu_profiler,blink.user_timing,disabled-by-default-devtools.timeline';

export type TraceEvent = {
  name?: string;
  ph?: string;
  ts?: number;
  cat?: string;
  args?: {
    data?: {
      startTime?: number;
      timeDeltas?: number[];
      cpuProfile?: {
        samples?: number[];
        nodes?: {
          id: number;
          callFrame: {functionName: string; url?: string; lineNumber?: number};
        }[];
      };
    };
  };
};

async function targetWebSocketUrl(
  metroPort: number,
  appId: string,
  deviceMatch: string,
): Promise<string> {
  // The app reaches Metro before it finishes registering with the inspector
  // proxy, so poll rather than failing on the first miss.
  const deadline = Date.now() + 15 * 60_000;
  let seen = '';
  for (;;) {
    try {
      const list = (await fetch(`http://${HOST}:${metroPort}/json/list`).then(
        r => r.json(),
      )) as {
        title?: string;
        appId?: string;
        webSocketDebuggerUrl: string;
      }[];
      const t = list.find(
        t => t.appId === appId && (t.title ?? '').includes(deviceMatch),
      );
      if (t) {
        // Must be the proxy's own base host, not `localhost`.
        return t.webSocketDebuggerUrl.replace('localhost', HOST);
      }
      seen = list.map(t => t.title).join(' | ') || '(none)';
    } catch {
      // Metro is not listening yet; a native build can take minutes.
      seen = '(Metro not up)';
    }
    if (Date.now() > deadline) {
      throw new Error(
        `No CDP target for ${appId} on ${deviceMatch}. Targets: ${seen}`,
      );
    }
    await new Promise(r => setTimeout(r, 1000));
  }
}

export type Session = {
  /** Evaluates an expression in the Hermes runtime and returns its value. */
  evaluate<T>(expression: string): Promise<T>;
  /** Blocks until the app has published its benchmark harness. */
  waitForHarness(): Promise<void>;
  startTracing(): Promise<void>;
  /** Ends tracing and resolves with the collected Chrome trace events. */
  stopTracing(): Promise<TraceEvent[]>;
  close(): void;
};

export async function connectSession(
  metroPort: number,
  appId: string,
  deviceMatch: string,
): Promise<Session> {
  const url = await targetWebSocketUrl(metroPort, appId, deviceMatch);
  const ws = new WebSocket(url, {
    headers: {Origin: `http://${HOST}:${metroPort}`},
    perMessageDeflate: false,
  });
  await new Promise<void>((resolve, reject) => {
    ws.once('open', resolve);
    ws.once('error', reject);
  });

  let id = 0;
  const pending = new Map<
    number,
    {resolve: (v: never) => void; reject: (e: unknown) => void}
  >();
  let events: TraceEvent[] = [];
  let onComplete: (() => void) | undefined;

  ws.on('message', data => {
    const msg = JSON.parse(String(data)) as {
      id?: number;
      error?: unknown;
      result?: unknown;
      method?: string;
      params?: {value?: TraceEvent[]};
    };
    if (msg.id !== undefined && pending.has(msg.id)) {
      const {resolve, reject} = pending.get(msg.id)!;
      pending.delete(msg.id);
      if (msg.error) {
        reject(new Error(JSON.stringify(msg.error)));
      } else {
        resolve(msg.result as never);
      }
      return;
    }
    if (msg.method === 'Tracing.dataCollected' && msg.params?.value) {
      events.push(...msg.params.value);
    } else if (msg.method === 'Tracing.tracingComplete') {
      onComplete?.();
    }
  });

  function send<T>(method: string, params: unknown = {}): Promise<T> {
    const myId = ++id;
    ws.send(JSON.stringify({id: myId, method, params}));
    return new Promise<T>((resolve, reject) => {
      // Profiling polls once a second for the length of a benchmark, so the
      // timer has to be cleared on response rather than left to expire.
      const timer = setTimeout(() => {
        if (pending.delete(myId)) {
          reject(new Error(`${method} timed out`));
        }
      }, 60_000);
      pending.set(myId, {
        resolve: (v: never) => {
          clearTimeout(timer);
          resolve(v);
        },
        reject: (e: unknown) => {
          clearTimeout(timer);
          reject(e);
        },
      });
    });
  }

  async function evaluate<T>(expression: string): Promise<T> {
    const r = await send<{
      result: {value: T};
      exceptionDetails?: unknown;
    }>('Runtime.evaluate', {expression, returnByValue: true});
    if (r.exceptionDetails) {
      throw new Error(
        `evaluate failed: ${JSON.stringify(r.exceptionDetails).slice(0, 400)}`,
      );
    }
    return r.result.value;
  }

  return {
    evaluate,

    async waitForHarness() {
      const deadline = Date.now() + 60_000;
      for (;;) {
        const t = await evaluate<string>(
          'typeof globalThis.__replicachePerf',
        ).catch(() => 'undefined');
        if (t === 'object') {
          return;
        }
        if (Date.now() > deadline) {
          throw new Error(
            'The app never published globalThis.__replicachePerf. It is set in ' +
              "the Expo app's App.tsx; make sure Metro rebundled after any edit " +
              '(a CI=1 Metro does not watch for changes).',
          );
        }
        await new Promise(r => setTimeout(r, 1000));
      }
    },

    async startTracing() {
      events = [];
      await send('Tracing.start', {categories: TRACING_CATEGORIES});
    },

    async stopTracing() {
      const complete = new Promise<void>(resolve => {
        onComplete = resolve;
      });
      await send('Tracing.end', {});
      await Promise.race([complete, new Promise(r => setTimeout(r, 30_000))]);
      return events;
    },

    close: () => ws.close(),
  };
}

/**
 * Runs one benchmark inside the app and resolves with its formatted result.
 *
 * Driven by polling rather than `awaitPromise` because React Native's Promise
 * is a polyfill (see the note at the top of this file).
 */
export async function runBenchmarkInApp(
  session: Session,
  name: string,
  group: string,
  timeoutMs: number,
): Promise<string> {
  await session.evaluate(`(globalThis.__perfDone = false,
    globalThis.__perfOut = null,
    __replicachePerf.runBenchmarkByNameAndGroup(${JSON.stringify(
      name,
    )}, ${JSON.stringify(group)})
      .then(r => { globalThis.__perfOut = r && r[0] === 'result'
        ? __replicachePerf.formatAsReplicache(r[1])
        : 'ERROR ' + String(r && r[1]); })
      .catch(e => { globalThis.__perfOut = 'THREW ' + String((e && e.message) || e); })
      .then(() => { globalThis.__perfDone = true; }), 'started')`);

  const deadline = Date.now() + timeoutMs;
  for (;;) {
    await new Promise(r => setTimeout(r, 1000));
    if (await session.evaluate<boolean>('globalThis.__perfDone === true')) {
      return session.evaluate<string>('globalThis.__perfOut');
    }
    if (Date.now() > deadline) {
      throw new Error(`Benchmark "${name}" did not finish in ${timeoutMs}ms`);
    }
  }
}

export type FrameTime = {frame: string; us: number; pct: number};

export type Attribution = {
  rows: FrameTime[];
  /** Microseconds of samples inside the measured windows. */
  inWindowUs: number;
  /** Microseconds excluded as benchmark setup/teardown. */
  excludedUs: number;
  windows: number;
};

/**
 * Self time per frame, counting only samples inside the benchmark's measured
 * windows.
 *
 * `runBenchmark` brackets exactly the region it times with performance.mark and
 * performance.measure, and those arrive here as `blink.user_timing` b/e pairs
 * named after the benchmark. Without this the profile is dominated by test-data
 * generation, which the benchmark does not count.
 */
export function selfTimeByFrame(
  events: TraceEvent[],
  windowName: string,
): Attribution {
  const windows: [number, number][] = [];
  let open: number | undefined;
  for (const e of events) {
    if (e.cat !== 'blink.user_timing' || e.name !== windowName) {
      continue;
    }
    if (e.ph === 'b') {
      open = e.ts;
    } else if (e.ph === 'e' && open !== undefined) {
      windows.push([open, e.ts!]);
      open = undefined;
    }
  }
  const inWindow = (ts: number) => windows.some(([a, b]) => ts >= a && ts <= b);

  const frames = new Map<number, string>();
  const self = new Map<string, number>();
  let t = 0;
  let inWindowUs = 0;
  let excludedUs = 0;

  for (const e of events) {
    if (e.name === 'Profile' && e.args?.data?.startTime !== undefined) {
      t = e.args.data.startTime;
    }
    if (e.name !== 'ProfileChunk') {
      continue;
    }
    const data = e.args?.data;
    const profile = data?.cpuProfile;
    if (!data || !profile) {
      continue;
    }
    for (const n of profile.nodes ?? []) {
      const f = n.callFrame;
      const url = f.url ?? '';
      // Frames from the Metro bundle carry a useless http URL; keep the line so
      // anonymous frames at least stay distinguishable.
      const where = url.startsWith('http')
        ? f.lineNumber !== undefined && f.lineNumber >= 0
          ? `  bundle:${f.lineNumber + 1}`
          : ''
        : url
          ? `  ${url.replace(PACKAGE_PATH_RE, '$1/')}`
          : '';
      frames.set(n.id, `${f.functionName || '(anonymous)'}${where}`);
    }
    const samples = profile.samples ?? [];
    const deltas = data.timeDeltas ?? [];
    for (let i = 0; i < samples.length; i++) {
      const dt = deltas[i] ?? 0;
      t += dt;
      if (!inWindow(t)) {
        excludedUs += dt;
        continue;
      }
      inWindowUs += dt;
      const key = frames.get(samples[i]) ?? '(unknown)';
      self.set(key, (self.get(key) ?? 0) + dt);
    }
  }

  const rows = Array.from(self.entries(), ([frame, us]) => ({
    frame,
    us,
    pct: inWindowUs ? (100 * us) / inWindowUs : 0,
  })).sort((a, b) => b.us - a.us);

  return {rows, inWindowUs, excludedUs, windows: windows.length};
}
