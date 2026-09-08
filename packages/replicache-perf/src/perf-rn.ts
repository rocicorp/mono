/**
 * Runs the replicache benchmarks on a React Native device.
 *
 * The web runner (runner.ts) serves the harness from Vite and drives it with
 * Playwright. There is no Playwright for React Native, so the roles are
 * inverted: this process serves a small HTTP control API and the Expo app in
 * `replicache-perf-rn` pulls one benchmark at a time from it, posts the result
 * back, and reloads its JS context between benchmarks the way runner.ts calls
 * `page.reload()`.
 *
 * The only platform-specific parts are booting the device, making the host
 * reachable and launching the app; those live behind {@link DeviceDriver}.
 */
import {
  execFile as execFileCb,
  spawn,
  type ChildProcess,
} from 'node:child_process';
import {createReadStream} from 'node:fs';
import * as fs from 'node:fs/promises';
import * as http from 'node:http';
import * as os from 'node:os';
import * as path from 'node:path';
import process from 'node:process';
import {fileURLToPath} from 'node:url';
import {promisify} from 'node:util';
import commandLineArgs from 'command-line-args';
import commandLineUsage from 'command-line-usage';
import {
  type BencherMetricsFormat,
  toBencherMetricFormat,
} from './bencher-metric-format.ts';
import type {BenchmarkResult} from './benchmark.ts';
import {benchmarks as mapLoopBenchmarks} from './benchmarks/map-loop.ts';
import {benchmarks as replicacheBenchmarks} from './benchmarks/replicache.ts';
import {formatAsBenchmarkJS, formatAsReplicache} from './format.ts';
import {createGithubActionBenchmarkJSONEntries} from './github-action-benchmark.ts';
import {
  connectSession,
  runBenchmarkInApp,
  selfTimeByFrame,
} from './hermes-trace.ts';

const execFile = promisify(execFileCb);

const rootDir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');

const allPlatforms = ['android', 'ios'] as const;
type Platform = (typeof allPlatforms)[number];

const allBackends = ['expo', 'op', 'mem'] as const;
type Backend = (typeof allBackends)[number];

type Format = 'benchmarkJS' | 'json' | 'replicache' | 'bmf';

/** Metro's default 8081 is often taken (Docker), so default one above it. */
const DEFAULT_METRO_PORT = 8082;
const DEFAULT_CONTROL_PORT = 9099;
/** The Expo host app, checked in beside the harness it runs. */
const DEFAULT_APP_DIR = path.join(rootDir, 'rn');

class UnknownValueError extends Error {
  name = 'UNKNOWN_VALUE';
  value: string;
  optionName: string;

  constructor(arg: string, optionName: string) {
    super(`Unknown value ${arg}`);
    this.value = arg;
    this.optionName = '--' + optionName;
  }
}

function platform(arg: string): Platform | 'all' {
  arg = arg.toLowerCase();
  if (!['all', ...allPlatforms].includes(arg)) {
    throw new UnknownValueError(arg, 'platform');
  }
  return arg as Platform | 'all';
}

function backend(arg: string): Backend | 'all' {
  arg = arg.toLowerCase();
  if (!['all', ...allBackends].includes(arg)) {
    throw new UnknownValueError(arg, 'backend');
  }
  return arg as Backend | 'all';
}

function format(arg: string): Format {
  if (!['benchmarkJS', 'json', 'replicache', 'bmf'].includes(arg)) {
    throw new UnknownValueError(arg, 'format');
  }
  return arg as Format;
}

// ---------------------------------------------------------------------------
// Device drivers
// ---------------------------------------------------------------------------

type DeviceDriver = {
  readonly platform: Platform;
  /** Boots a device if none is running; resolves to its name. */
  ensureBooted(requested: string | undefined): Promise<string>;
  /** Makes host ports reachable from the device. */
  forwardPorts(ports: number[]): Promise<void>;
  /** Args for `npx`, launching the app and Metro. */
  expoArgs(device: string, metroPort: number, release: boolean): string[];
  /**
   * Retried until the app first contacts the control server. Exists because
   * `expo run:ios` points the dev client at Metro with a deep link, and iOS
   * puts a "Open in ...?" confirmation in front of that whenever some other app
   * happens to be frontmost — which silently strands the run.
   */
  nudge?(appDir: string, metroPort: number): Promise<void>;
};

function adbPath(): string {
  const home =
    process.env.ANDROID_HOME ??
    process.env.ANDROID_SDK_ROOT ??
    path.join(os.homedir(), 'Library/Android/sdk');
  return path.join(home, 'platform-tools', 'adb');
}

const androidDriver: DeviceDriver = {
  platform: 'android',

  async ensureBooted(requested) {
    const avd = requested ?? 'medium_phone';
    const {stdout} = await execFile(adbPath(), ['devices']);
    const booted = stdout
      .split('\n')
      .slice(1)
      .filter(l => l.trim().endsWith('device'))
      .map(l => l.split('\t')[0]);
    if (booted.length === 0) {
      throw new Error(
        `No Android device. Start the emulator first:\n` +
          `  $ANDROID_HOME/emulator/emulator -avd ${avd} &`,
      );
    }
    return avd;
  },

  async forwardPorts(ports) {
    // The emulator's own localhost is not the host's, so every port the app
    // talks to has to be reversed explicitly.
    for (const p of ports) {
      await execFile(adbPath(), ['reverse', `tcp:${p}`, `tcp:${p}`]);
    }
  },

  expoArgs(device, metroPort, release) {
    return [
      'expo',
      'run:android',
      '--device',
      device,
      '--port',
      String(metroPort),
      ...(release ? ['--variant', 'release'] : []),
    ];
  },
};

const iosDriver: DeviceDriver = {
  platform: 'ios',

  async ensureBooted(requested) {
    const {stdout} = await execFile('xcrun', [
      'simctl',
      'list',
      'devices',
      'booted',
      '-j',
    ]);
    const {devices} = JSON.parse(stdout) as {
      devices: Record<string, {name: string; udid: string}[]>;
    };
    const booted = Object.values(devices).flat();
    if (booted.length > 0) {
      const match = requested
        ? booted.find(d => d.name === requested)
        : booted[0];
      if (match) {
        return match.name;
      }
    }
    const name = requested ?? 'iPhone 17 Pro';
    await execFile('xcrun', ['simctl', 'boot', name]);
    await execFile('open', ['-a', 'Simulator']);
    return name;
  },

  // The iOS simulator shares the host's network stack, so localhost already
  // reaches us. Nothing to do.
  forwardPorts: () => Promise.resolve(),

  expoArgs(device, metroPort, release) {
    return [
      'expo',
      'run:ios',
      '--device',
      device,
      '--port',
      String(metroPort),
      ...(release ? ['--configuration', 'Release'] : []),
    ];
  },

  async nudge(appDir, metroPort) {
    const appJson = JSON.parse(
      await fs.readFile(path.join(appDir, 'app.json'), 'utf-8'),
    ) as {expo: {ios: {bundleIdentifier: string}}};
    const id = appJson.expo.ios.bundleIdentifier;
    // Foregrounding the app first means the deep link is delivered to the app
    // that is already frontmost, which iOS does not gate behind a prompt.
    await execFile('xcrun', ['simctl', 'launch', 'booted', id]);
    await execFile('xcrun', [
      'simctl',
      'openurl',
      'booted',
      `${id}://expo-development-client/?url=${encodeURIComponent(
        `http://localhost:${metroPort}`,
      )}`,
    ]);
  },
};

function driverFor(p: Platform): DeviceDriver {
  return p === 'android' ? androidDriver : iosDriver;
}

// ---------------------------------------------------------------------------
// Control server
// ---------------------------------------------------------------------------

type QueueItem = {backend: Backend; name: string; group: string};

type Outcome =
  | {item: QueueItem; result: BenchmarkResult}
  | {item: QueueItem; error: string};

type ControlServer = {
  readonly port: number;
  /** Resolves when every queued benchmark has reported, rejects on error. */
  readonly done: Promise<Outcome[]>;
  /** Resolves the first time the app talks to us. */
  readonly firstContact: Promise<void>;
  close(): Promise<void>;
};

function startControlServer(
  queue: QueueItem[],
  port: number,
  idleTimeoutMs: number,
  onOutcome: (outcome: Outcome, index: number, total: number) => void,
): Promise<ControlServer> {
  const outcomes: Outcome[] = [];
  let index = 0;
  let settle!: (o: Outcome[]) => void;
  let fail!: (e: unknown) => void;
  const done = new Promise<Outcome[]>((res, rej) => {
    settle = res;
    fail = rej;
  });

  let noteContact!: () => void;
  const firstContact = new Promise<void>(res => {
    noteContact = res;
  });

  let lastActivity = Date.now();
  const watchdog = setInterval(() => {
    if (Date.now() - lastActivity > idleTimeoutMs) {
      clearInterval(watchdog);
      fail(
        new Error(
          `The app went quiet for ${Math.round(idleTimeoutMs / 1000)}s ` +
            `(${index}/${queue.length} benchmarks done). ` +
            `Re-run with --verbose to see the Expo/Metro output.`,
        ),
      );
    }
  }, 5_000);

  const json = (res: http.ServerResponse, body: unknown) => {
    const s = JSON.stringify(body);
    res.writeHead(200, {
      'content-type': 'application/json',
      'content-length': Buffer.byteLength(s),
    });
    res.end(s);
  };

  const server = http.createServer((req, res) => {
    lastActivity = Date.now();
    noteContact();
    const url = new URL(req.url ?? '/', `http://localhost:${port}`);

    switch (url.pathname) {
      case '/ping':
        json(res, {ok: true});
        return;

      case '/next': {
        const item = queue[index];
        if (!item) {
          json(res, {done: true});
          clearInterval(watchdog);
          settle(outcomes);
          return;
        }
        json(res, {
          done: false,
          ...item,
          // 9.7 MB: served rather than bundled into the app.
          tmcwUrl: `http://localhost:${port}/tmcw.json`,
          index,
          total: queue.length,
        });
        return;
      }

      case '/result': {
        const chunks: Buffer[] = [];
        req.on('data', c => chunks.push(c as Buffer));
        req.on('end', () => {
          lastActivity = Date.now();
          const item = queue[index];
          if (!item) {
            // A late or duplicate post, e.g. the app reloaded after we had
            // already recorded its result. Nothing left to attribute it to.
            res.writeHead(409, {'content-type': 'application/json'});
            res.end(JSON.stringify({error: 'no benchmark in flight'}));
            return;
          }
          let body: {result: BenchmarkResult} | {error: string};
          try {
            body = JSON.parse(Buffer.concat(chunks).toString());
          } catch (e) {
            // Record it as a failed benchmark rather than taking the runner
            // down with it, so the rest of the queue still runs.
            body = {error: `malformed /result body: ${String(e)}`};
          }
          const outcome: Outcome =
            'result' in body
              ? {item, result: body.result}
              : {item, error: body.error};
          outcomes.push(outcome);
          onOutcome(outcome, index, queue.length);
          index++;
          json(res, {ok: true});
        });
        return;
      }

      case '/tmcw.json': {
        const file = path.join(rootDir, 'resources', 'tmcw.json');
        res.writeHead(200, {'content-type': 'application/json'});
        createReadStream(file).pipe(res);
        return;
      }

      default:
        res.writeHead(404);
        res.end();
    }
  });

  return new Promise((resolve, reject) => {
    server.on('error', reject);
    server.listen(port, () =>
      resolve({
        port,
        done,
        firstContact,
        close: () =>
          new Promise<void>(res => {
            clearInterval(watchdog);
            server.close(() => res());
          }),
      }),
    );
  });
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const optionDefinitions = [
  {
    name: 'platform',
    type: platform,
    defaultValue: 'android',
    description: `Device platform: ${allPlatforms.join(', ')}, or all`,
  },
  {
    name: 'backend',
    type: backend,
    multiple: true,
    defaultValue: ['expo'],
    description: `Key/value backends: ${allBackends.join(', ')}, or all`,
  },
  {
    name: 'device',
    type: String,
    description:
      'AVD name (android, default medium_phone) or simulator name (ios, default the booted one)',
  },
  {
    name: 'run',
    type: RegExp,
    description: 'Run only those benchmarks matching the regular expression.',
  },
  {
    name: 'format',
    alias: 'f',
    type: format,
    defaultValue: 'benchmarkJS',
    description:
      'Format for text output, either benchmarkJS (default), json, replicache or bmf (Bencher Metrics Format)',
  },
  {
    name: 'list',
    alias: 'l',
    type: Boolean,
    description: 'List available benchmarks',
  },
  {
    name: 'app',
    type: String,
    defaultValue: DEFAULT_APP_DIR,
    description:
      'Path to the Expo host app (default: the rn/ directory beside this package)',
  },
  {
    name: 'port',
    type: Number,
    defaultValue: DEFAULT_CONTROL_PORT,
    description: `Control server port (default ${DEFAULT_CONTROL_PORT})`,
  },
  {
    name: 'metro-port',
    type: Number,
    defaultValue: DEFAULT_METRO_PORT,
    description: `Metro bundler port (default ${DEFAULT_METRO_PORT})`,
  },
  {
    name: 'idle-timeout',
    type: Number,
    defaultValue: 900,
    description:
      'Seconds without a request from the app before giving up (default 900; the first native build is slow)',
  },
  {
    name: 'release',
    type: Boolean,
    defaultValue: false,
    description:
      'Build and run the release variant (__DEV__ off, JS minified and precompiled to bytecode, no debugger). Note the app then has no DevSettings.reload, so benchmarks share one JS context, and --profile is unavailable because release builds ship no inspector.',
  },
  {
    name: 'profile',
    type: String,
    description:
      'Capture a Hermes CPU profile of one benchmark and write the Chrome trace to this path. Drives the app directly over CDP (no control server), rebuilds the bundle unminified for readable frames, and reports self time windowed to the region the bencher actually times. Requires a single platform/backend/benchmark.',
  },
  {
    name: 'verbose',
    alias: 'v',
    type: Boolean,
    defaultValue: false,
    description: 'Stream the Expo/Metro output',
  },
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Show this help message',
  },
];

type Options = {
  'platform': Platform | 'all';
  'backend': (Backend | 'all')[];
  'device'?: string;
  'run'?: RegExp;
  'format': Format;
  'list'?: boolean;
  'app': string;
  'port': number;
  'metro-port': number;
  'idle-timeout': number;
  'release'?: boolean;
  'profile'?: string;
  'verbose': boolean;
  'help'?: boolean;
};

function logLine(s: string, options: Options) {
  if (options.format !== 'json' && options.format !== 'bmf') {
    process.stdout.write(s + '\n');
  }
}

/**
 * The app is a plain Expo project outside the workspace; it consumes the bundle
 * as an ordinary local file, so Metro never sees monorepo TypeScript. The
 * sourcemap comes along (renamed) so on-device stack traces stay readable.
 */
async function appIdFor(appDir: string, p: Platform): Promise<string> {
  const {expo} = JSON.parse(
    await fs.readFile(path.join(appDir, 'app.json'), 'utf-8'),
  ) as {expo: {ios: {bundleIdentifier: string}; android: {package: string}}};
  return p === 'ios' ? expo.ios.bundleIdentifier : expo.android.package;
}

async function installBundle(appDir: string): Promise<void> {
  const js = await fs.readFile(path.join(rootDir, 'out', 'rn.js'), 'utf-8');
  await fs.writeFile(
    path.join(appDir, 'benchmarks.js'),
    js.replace(
      '//# sourceMappingURL=rn.js.map',
      '//# sourceMappingURL=benchmarks.js.map',
    ),
  );
  await fs.copyFile(
    path.join(rootDir, 'out', 'rn.js.map'),
    path.join(appDir, 'benchmarks.js.map'),
  );
  await fs.copyFile(
    path.join(rootDir, 'out', 'rn.d.ts'),
    path.join(appDir, 'benchmarks.d.ts'),
  );
}

async function nudgeUntilContact(
  driver: DeviceDriver,
  appDir: string,
  metroPort: number,
  firstContact: Promise<void>,
): Promise<void> {
  let contacted = false;
  void firstContact.then(() => {
    contacted = true;
  });
  while (!contacted) {
    await new Promise(r => setTimeout(r, 15_000));
    if (contacted) {
      return;
    }
    // Expected to fail while the native build is still running.
    await driver.nudge?.(appDir, metroPort).catch(() => {});
  }
}

/** Builds, installs and launches the app, with Metro, in its own process group. */
function spawnExpo(
  driver: DeviceDriver,
  device: string,
  metroPort: number,
  options: Options,
): ChildProcess {
  return spawn(
    'npx',
    driver.expoArgs(device, metroPort, options.release ?? false),
    {
      cwd: options.app,
      env: {
        ...process.env,
        CI: '1',
        EXPO_PUBLIC_PERF_PORT: String(options.port),
        // CocoaPods refuses to run without a UTF-8 locale, and a non-login shell
        // often has none.
        LANG: process.env.LANG ?? 'en_US.UTF-8',
      },
      stdio: options.verbose ? 'inherit' : 'ignore',
      // Its own process group, so killing it takes Metro and gradle down too
      // rather than leaving Metro holding the port.
      detached: true,
    },
  );
}

function killGroup(child: ChildProcess | undefined): void {
  if (child?.pid === undefined) {
    return;
  }
  try {
    process.kill(-child.pid, 'SIGTERM');
  } catch {
    // Already gone.
  }
}

/**
 * Profiling mode. Unlike a normal run this does not use the control server at
 * all: the app sits idle in manual mode and we drive one benchmark straight
 * through `Runtime.evaluate`, so nothing but the benchmark runs inside the
 * traced window. Requires the Expo app to publish `globalThis.__replicachePerf`
 * (see its App.tsx).
 */
async function profileOnDevice(
  p: Platform,
  item: QueueItem,
  device: string,
  metroPort: number,
  options: Options,
): Promise<void> {
  const session = await connectSession(
    metroPort,
    await appIdFor(options.app, p),
    p === 'ios' ? device : 'sdk_gphone',
  );
  try {
    await session.waitForHarness();
    await session.evaluate(
      `__replicachePerf.configure({backend: ${JSON.stringify(
        item.backend,
      )}, tmcwUrl: ${JSON.stringify(
        `http://localhost:${options.port}/tmcw.json`,
      )}})`,
    );

    await session.startTracing();
    const result = await runBenchmarkInApp(
      session,
      item.name,
      item.group,
      options['idle-timeout'] * 1000,
    );
    const events = await session.stopTracing();

    logLine(`${item.backend} ${result}`, options);

    await fs.writeFile(options.profile!, JSON.stringify(events));
    const {rows, inWindowUs, excludedUs, windows} = selfTimeByFrame(
      events,
      item.name,
    );
    logLine(
      `\nHermes CPU profile (${p}): ${windows} measured windows, ` +
        `${(inWindowUs / 1000).toFixed(0)} ms in-window, ` +
        `${(excludedUs / 1000).toFixed(0)} ms of setup/teardown excluded`,
      options,
    );
    for (const r of rows.slice(0, 25)) {
      logLine(
        `${r.pct.toFixed(1).padStart(6)}%  ${(r.us / 1000)
          .toFixed(1)
          .padStart(8)} ms  ${r.frame}`,
        options,
      );
    }
    logLine(`\nfull trace written to ${options.profile}`, options);
  } finally {
    session.close();
  }
}

async function runPlatform(
  p: Platform,
  queue: QueueItem[],
  options: Options,
): Promise<Outcome[]> {
  const driver = driverFor(p);
  const metroPort = options['metro-port'];

  const device = await driver.ensureBooted(options.device);
  logLine(`Running ${queue.length} benchmarks on ${p} (${device})...`, options);

  await driver.forwardPorts([options.port, metroPort]);

  if (options.profile) {
    // An empty queue: the app's first /next gets `done`, so it settles into
    // idle instead of running anything, and we still serve /tmcw.json for the
    // benchmarks that need the fixture.
    const idle = await startControlServer(
      [],
      options.port,
      options['idle-timeout'] * 1000,
      () => {},
    );
    let expo: ChildProcess | undefined;
    try {
      expo = spawnExpo(driver, device, metroPort, options);
      await profileOnDevice(p, queue[0], device, metroPort, options);
      return [];
    } finally {
      killGroup(expo);
      await idle.close();
    }
  }

  const server = await startControlServer(
    queue,
    options.port,
    options['idle-timeout'] * 1000,
    (outcome, index, total) => {
      if ('error' in outcome) {
        return;
      }
      const label = `[${index + 1}/${total}] ${outcome.item.backend}`;
      switch (options.format) {
        case 'replicache':
          logLine(`${label} ${formatAsReplicache(outcome.result)}`, options);
          break;
        case 'benchmarkJS':
          logLine(`${label} ${formatAsBenchmarkJS(outcome.result)}`, options);
          break;
        default:
          if (options.verbose) {
            logLine(`${label} ${outcome.item.name}`, options);
          }
      }
    },
  );

  let expo: ChildProcess | undefined;
  try {
    expo = spawnExpo(driver, device, metroPort, options);
    const expoExited = new Promise<never>((_, reject) => {
      expo?.on('exit', code => {
        // Metro staying up is normal; only a non-zero exit is a failure, and
        // only before the run finished.
        if (code !== 0 && code !== null) {
          reject(
            new Error(
              `expo run:${p} exited with code ${code}. Re-run with --verbose.`,
            ),
          );
        }
      });
      expo?.on('error', reject);
    });

    if (driver.nudge) {
      void nudgeUntilContact(
        driver,
        options.app,
        metroPort,
        server.firstContact,
      );
    }

    return await Promise.race([server.done, expoExited]);
  } finally {
    killGroup(expo);
    await server.close();
  }
}

async function main() {
  const options = commandLineArgs(optionDefinitions) as Options;

  if (options.help) {
    // oxlint-disable-next-line no-console
    console.log(
      commandLineUsage([
        {content: 'Usage: perf:rn [options...]'},
        {optionList: optionDefinitions},
      ]),
    );
    process.exit();
  }

  // Must match the harness in rn.ts.
  let names = [...replicacheBenchmarks(), ...mapLoopBenchmarks()].map(b => ({
    name: b.name,
    group: b.group,
  }));
  if (options.run !== undefined) {
    names = names.filter(({name}) => options.run!.test(name));
  }

  if (options.list) {
    // oxlint-disable-next-line no-console
    console.log(
      'Available benchmarks (group / name):\n' +
        names
          .map(({name, group}) => `${group} / ${name}`)
          .sort()
          .join('\n'),
    );
    return;
  }

  if (names.length === 0) {
    // oxlint-disable-next-line no-console
    console.error('No benchmarks matched --run.');
    process.exit(1);
  }

  const backends: Backend[] = options.backend.includes('all')
    ? [...allBackends]
    : (options.backend as Backend[]);
  const platforms: Platform[] =
    options.platform === 'all' ? [...allPlatforms] : [options.platform];

  if (options.profile) {
    // Hermes frames are just `Ys`/`ie`/`dr` against the shipped bundle, so
    // rebuild without minification before installing it.
    logLine(
      'Rebuilding the RN bundle unminified for readable frames...',
      options,
    );
    await new Promise<void>((resolve, reject) => {
      const build = spawn('node', [path.join(rootDir, 'tool', 'build.ts')], {
        cwd: rootDir,
        env: {...process.env, PERF_RN_NO_MINIFY: '1'},
        stdio: options.verbose ? 'inherit' : 'ignore',
      });
      build.on('exit', code =>
        code === 0 ? resolve() : reject(new Error(`build exited ${code}`)),
      );
      build.on('error', reject);
    });
  }

  await installBundle(options.app);

  const queue: QueueItem[] = backends.flatMap(backend =>
    names.map(({name, group}) => ({backend, name, group})),
  );

  if (options.release && options.port !== DEFAULT_CONTROL_PORT) {
    // Release bundles are built by gradle/Xcode, which do not inherit this
    // process's environment, so EXPO_PUBLIC_PERF_PORT never reaches the app and
    // it falls back to the default port.
    // oxlint-disable-next-line no-console
    console.error(
      `--release cannot use a custom --port (the app is stuck on ${DEFAULT_CONTROL_PORT}).`,
    );
    process.exit(1);
  }

  if (options.profile && options.release) {
    // oxlint-disable-next-line no-console
    console.error(
      '--profile needs the debug variant: release builds ship no CDP inspector.',
    );
    process.exit(1);
  }

  if (options.profile && (queue.length !== 1 || platforms.length !== 1)) {
    // oxlint-disable-next-line no-console
    console.error(
      `--profile needs exactly one benchmark on one platform with one backend; ` +
        `got ${queue.length} benchmark(s) on ${platforms.length} platform(s). ` +
        `Narrow it with --run, --backend and --platform.`,
    );
    process.exit(1);
  }

  const jsonEntries: unknown[] = [];
  let bmf: BencherMetricsFormat = {};
  let failed = false;
  let first = true;

  for (const p of platforms) {
    if (!first) {
      logLine('', options);
    }
    first = false;

    const outcomes = await runPlatform(p, [...queue], options);

    for (const outcome of outcomes) {
      if ('error' in outcome) {
        failed = true;
        process.stderr.write(
          `${p} / ${outcome.item.backend} / ${outcome.item.name}: ${outcome.error}\n`,
        );
        continue;
      }
      // Keep platform and backend in the machine-readable names so a matrix run
      // does not collapse into indistinguishable keys.
      const result = {
        ...outcome.result,
        name: `${p} ${outcome.item.backend} ${outcome.result.name}`,
      };
      switch (options.format) {
        case 'json':
          jsonEntries.push(...createGithubActionBenchmarkJSONEntries(result));
          break;
        case 'bmf':
          bmf = {...bmf, ...toBencherMetricFormat(result)};
          break;
      }
    }
  }

  if (options.format === 'json') {
    process.stdout.write(JSON.stringify(jsonEntries, null, 2) + '\n');
  } else if (options.format === 'bmf') {
    process.stdout.write(JSON.stringify(bmf, null, 2) + '\n');
  } else {
    logLine('Done!', options);
  }

  if (failed) {
    process.exit(1);
  }
}

main().catch(err => {
  // oxlint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
