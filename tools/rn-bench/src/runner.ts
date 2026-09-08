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
import * as fs from 'node:fs/promises';
import * as http from 'node:http';
import * as os from 'node:os';
import * as path from 'node:path';
import process from 'node:process';
import {promisify} from 'node:util';
import commandLineArgs from 'command-line-args';
import commandLineUsage from 'command-line-usage';
import {
  type BencherMetricsFormat,
  toBencherMetricFormat,
} from './bencher-metric-format.ts';
import type {BenchmarkResult} from './benchmark.ts';
import {formatAsBenchmarkJS, formatAsReplicache} from './format.ts';
import {createGithubActionBenchmarkJSONEntries} from './github-action-benchmark.ts';
import {
  connectSession,
  runBenchmarkInApp,
  selfTimeByFrame,
} from './hermes-trace.ts';

const execFile = promisify(execFileCb);

const allPlatforms = ['android', 'ios'] as const;
type Platform = (typeof allPlatforms)[number];

type Format = 'benchmarkJS' | 'json' | 'replicache' | 'bmf';

/**
 * Everything a package must supply to run its own benchmarks on a device. The
 * rest of this file — device drivers, the control server, Expo launching,
 * Hermes profiling — is the same for every package.
 */
export type RnBenchConfig = {
  /** Package root. `<rootDir>/out/rn.js` is installed into the app and
   *  `<rootDir>/tool/build.ts` rebuilds it unminified for profiling. */
  rootDir: string;
  /** Name shown in `--help`, e.g. `perf:rn`. */
  cliName: string;
  /**
   * Script that rebuilds the RN bundle, relative to `rootDir`. Profiling reruns
   * it with PERF_RN_NO_MINIFY=1, without which every Hermes frame is a mangled
   * one-or-two-letter name.
   */
  buildScript?: string;
  /** The global the host app publishes its harness on, e.g. `__zqlPerf`. */
  globalName: string;
  /**
   * Benchmarks available for `--list` and the queue. Must match what the RN
   * bundle registers — importing the built bundle is the surest way to keep the
   * two in step.
   */
  listBenchmarks: () =>
    | {name: string; group: string}[]
    | Promise<{name: string; group: string}[]>;
  /**
   * An optional second axis to run every benchmark across — Replicache uses it
   * for its key/value backends. Omitted for a single-variant suite.
   */
  variants?: {
    /** CLI flag name, e.g. `backend`. */
    flag: string;
    values: readonly string[];
    describe: string;
  };
  /** Extra fields merged into each `/next` payload. */
  nextExtras?: (controlPort: number) => Record<string, unknown>;
  /** Extra routes on the control server, e.g. a large fixture to stream. */
  extraRoutes?: Record<
    string,
    (req: http.IncomingMessage, res: http.ServerResponse) => void
  >;
  /** Expression evaluated in the app to configure it before a profiled run. */
  configureExpr?: (variant: string | undefined, controlPort: number) => string;
};

/** Metro's default 8081 is often taken (Docker), so default one above it. */
const DEFAULT_METRO_PORT = 8082;
const DEFAULT_CONTROL_PORT = 9099;
/** The Expo host app, checked in beside the harness it runs. */

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

type QueueItem = {variant: string | undefined; name: string; group: string};

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
  config: RnBenchConfig,
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
        const {variant, ...rest} = item;
        json(res, {
          done: false,
          ...rest,
          // The variant travels under the config's own flag name, so an app
          // reading `next.backend` keeps working.
          ...(config.variants && variant !== undefined
            ? {[config.variants.flag]: variant}
            : {}),
          ...(config.nextExtras?.(port) ?? {}),
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

      default: {
        const extra = config.extraRoutes?.[url.pathname];
        if (extra) {
          extra(req, res);
          return;
        }
        res.writeHead(404);
        res.end();
      }
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

function makeOptionDefinitions(config: RnBenchConfig) {
  const {variants} = config;
  return [
    {
      name: 'platform',
      type: platform,
      defaultValue: 'android',
      description: `Device platform: ${allPlatforms.join(', ')}, or all`,
    },
    ...(variants
      ? [
          {
            name: variants.flag,
            type: (arg: string) => {
              const a = arg.toLowerCase();
              if (!['all', ...variants.values].includes(a)) {
                throw new UnknownValueError(a, variants.flag);
              }
              return a;
            },
            multiple: true,
            defaultValue: [variants.values[0]],
            description: `${variants.describe}: ${variants.values.join(', ')}, or all`,
          },
        ]
      : []),
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
      defaultValue: path.join(config.rootDir, 'rn'),
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
      name: 'repeat',
      type: Number,
      defaultValue: 1,
      description:
        'Run the whole queue N times and report the median per benchmark plus the spread. A single run on an emulator is worth about +/-3%, so a smaller difference than that cannot be resolved without this.',
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
}

type Options = {
  'platform': Platform | 'all';
  /** Present only when the config declares a variant axis. */
  [variantFlag: string]: unknown;
  'device'?: string;
  'run'?: RegExp;
  'format': Format;
  'list'?: boolean;
  'app': string;
  'port': number;
  'metro-port': number;
  'idle-timeout': number;
  'repeat': number;
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

async function installBundle(appDir: string, rootDir: string): Promise<void> {
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
 * traced window. Requires the Expo app to publish the configured global (see
 * its App.tsx).
 */
async function profileOnDevice(
  config: RnBenchConfig,
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
    config.globalName,
  );
  try {
    await session.waitForHarness();
    const configureExpr = config.configureExpr?.(item.variant, options.port);
    if (configureExpr) {
      await session.evaluate(configureExpr);
    }

    await session.startTracing();
    const result = await runBenchmarkInApp(
      session,
      item.name,
      item.group,
      options['idle-timeout'] * 1000,
      config.globalName,
    );
    const events = await session.stopTracing();

    logLine(`${item.variant ? item.variant + ' ' : ''}${result}`, options);

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

/**
 * Collapses N runs of the same queue into one outcome per benchmark, taking the
 * median run so a single unlucky one cannot dominate, and reporting the spread
 * so it is obvious when a difference is smaller than the noise.
 *
 * An error in any repeat is kept: a benchmark that fails intermittently is a
 * real result, not something to average away.
 */
function summarizeRepeats(repeats: Outcome[][], options: Options): Outcome[] {
  const byKey = new Map<string, Outcome[]>();
  for (const run of repeats) {
    for (const o of run) {
      const key = `${o.item.variant ?? ''}\u0000${o.item.group}\u0000${o.item.name}`;
      (byKey.get(key) ?? byKey.set(key, []).get(key)!).push(o);
    }
  }

  const summarized: Outcome[] = [];
  for (const group of byKey.values()) {
    const failure = group.find(o => 'error' in o);
    if (failure) {
      summarized.push(failure);
      continue;
    }
    const ok = group as {item: QueueItem; result: BenchmarkResult}[];
    const sorted = ok.toSorted(
      (a, b) =>
        a.result.runTimesStatistics.medianMs -
        b.result.runTimesStatistics.medianMs,
    );
    const median = sorted[sorted.length >> 1];
    const lo = sorted[0].result.runTimesStatistics.medianMs;
    const hi = sorted.at(-1)!.result.runTimesStatistics.medianMs;
    const mid = median.result.runTimesStatistics.medianMs;
    logLine(
      `  ${median.item.name}: median ${mid.toFixed(2)} ms over ${
        ok.length
      } runs, spread ${lo.toFixed(2)}-${hi.toFixed(2)} ms (${(
        (100 * (hi - lo)) /
        (mid || 1)
      ).toFixed(1)}%)`,
      options,
    );
    summarized.push(median);
  }
  return summarized;
}

async function runPlatform(
  config: RnBenchConfig,
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
    // idle instead of running anything, while any extra routes (a large
    // fixture, say) stay served.
    const idle = await startControlServer(
      config,
      [],
      options.port,
      options['idle-timeout'] * 1000,
      () => {},
    );
    let expo: ChildProcess | undefined;
    try {
      expo = spawnExpo(driver, device, metroPort, options);
      await profileOnDevice(config, p, queue[0], device, metroPort, options);
      return [];
    } finally {
      killGroup(expo);
      await idle.close();
    }
  }

  const server = await startControlServer(
    config,
    queue,
    options.port,
    options['idle-timeout'] * 1000,
    (outcome, index, total) => {
      if ('error' in outcome) {
        return;
      }
      const label = `[${index + 1}/${total}]${
        outcome.item.variant ? ' ' + outcome.item.variant : ''
      }`;
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

/**
 * Runs a package's benchmarks on a device. The package supplies its benchmark
 * list, its Expo app and the global its harness publishes; everything else —
 * device drivers, the control server, Expo launching, Hermes profiling — is
 * shared.
 */
export async function runRnBench(config: RnBenchConfig): Promise<void> {
  const optionDefinitions = makeOptionDefinitions(config);
  const options = commandLineArgs(optionDefinitions) as Options;

  if (options.help) {
    // oxlint-disable-next-line no-console
    console.log(
      commandLineUsage([
        {content: `Usage: ${config.cliName} [options...]`},
        {optionList: optionDefinitions},
      ]),
    );
    process.exit();
  }

  let names = await config.listBenchmarks();
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

  const {variants} = config;
  const selected = variants
    ? ((options[variants.flag] as string[] | undefined) ?? [variants.values[0]])
    : [];
  const variantValues: (string | undefined)[] = variants
    ? selected.includes('all')
      ? [...variants.values]
      : selected
    : [undefined];
  const platforms: Platform[] =
    options.platform === 'all' ? [...allPlatforms] : [options.platform];

  // Always rebuild. Measuring a stale bundle silently reports the previous
  // revision's numbers, which looks exactly like "the change did nothing" --
  // and a null result is the one outcome nobody re-checks.
  logLine(
    options.profile
      ? // Hermes frames are just `Ys`/`ie`/`dr` against the shipped bundle, so
        // rebuild without minification before installing it.
        'Rebuilding the RN bundle unminified for readable frames...'
      : 'Rebuilding the RN bundle...',
    options,
  );
  await new Promise<void>((resolve, reject) => {
    const build = spawn(
      'node',
      [path.join(config.rootDir, config.buildScript ?? 'tool/build.ts')],
      {
        cwd: config.rootDir,
        env: options.profile
          ? {...process.env, PERF_RN_NO_MINIFY: '1'}
          : process.env,
        stdio: options.verbose ? 'inherit' : 'ignore',
      },
    );
    build.on('exit', code =>
      code === 0 ? resolve() : reject(new Error(`build exited ${code}`)),
    );
    build.on('error', reject);
  });

  await installBundle(options.app, config.rootDir);

  const queue: QueueItem[] = variantValues.flatMap(variant =>
    names.map(({name, group}) => ({variant, name, group})),
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

  if (options.profile && options.repeat > 1) {
    // oxlint-disable-next-line no-console
    console.error(
      '--profile captures one trace; --repeat has nothing to average.',
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
      `--profile needs exactly one benchmark on one platform` +
        (variants ? ` with one --${variants.flag}` : '') +
        `; got ${queue.length} benchmark(s) on ${platforms.length} platform(s).`,
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

    const repeats: Outcome[][] = [];
    for (let rep = 0; rep < options.repeat; rep++) {
      if (options.repeat > 1) {
        logLine(`\n-- repeat ${rep + 1}/${options.repeat} --`, options);
      }
      repeats.push(await runPlatform(config, p, [...queue], options));
    }
    const outcomes =
      options.repeat > 1 ? summarizeRepeats(repeats, options) : repeats[0];

    for (const outcome of outcomes) {
      if ('error' in outcome) {
        failed = true;
        process.stderr.write(
          `${p}${outcome.item.variant ? ' / ' + outcome.item.variant : ''} / ` +
            `${outcome.item.name}: ${outcome.error}\n`,
        );
        continue;
      }
      // Keep platform and variant in the machine-readable names so a matrix run
      // does not collapse into indistinguishable keys.
      const result = {
        ...outcome.result,
        name: `${p} ${
          outcome.item.variant ? outcome.item.variant + ' ' : ''
        }${outcome.result.name}`,
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
