import commandLineArgs from 'command-line-args';
import commandLineUsage from 'command-line-usage';
import getPort from 'get-port';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import process from 'node:process';
import {fileURLToPath} from 'node:url';
import * as playwright from 'playwright';
import {createServer} from 'vite';
import {assert} from '../../shared/src/asserts.ts';
import {makeDefine} from '../../shared/src/build.ts';
import {
  type BencherMetricsFormat,
  toBencherMetricFormat,
} from './bencher-metric-format.ts';
import {formatAsBenchmarkJS, formatAsReplicache} from './format.ts';
import {createGithubActionBenchmarkJSONEntries} from './github-action-benchmark.ts';

type Format = 'benchmarkJS' | 'json' | 'replicache' | 'bmf';

const allBrowsers = ['chromium', 'firefox', 'webkit'] as const;
type Browser = (typeof allBrowsers)[number];

type BenchmarkMeta = {
  name: string;
  group: string;
};

const rootDir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');

class UnknownValueError extends Error {
  name = 'UNKNOWN_VALUE';
  value: string;
  optionName: string;

  constructor(arg: string, optionName: string) {
    super(`Unknown format ${arg}`);
    this.value = arg;
    this.optionName = '--' + optionName;
  }
}

function browser(arg: string) {
  arg = arg.toLowerCase();
  if (!['all', ...allBrowsers].includes(arg)) {
    throw new UnknownValueError(arg, 'browsers');
  }
  return arg as Browser | 'all';
}

function browserName(browser: Browser) {
  let name = browser[0].toUpperCase() + browser.slice(1);
  if (name === 'Webkit') {
    name = 'WebKit';
  }
  return name;
}

function format(arg: string): Format {
  if (!['benchmarkJS', 'json', 'replicache', 'bmf'].includes(arg)) {
    throw new UnknownValueError(arg, 'format');
  }
  return arg as Format;
}

async function main() {
  const optionDefinitions = [
    {
      name: 'list',
      alias: 'l',
      type: Boolean,
      description: 'List available benchmarks',
    },
    {
      name: 'groups',
      multiple: true,
      description: 'Benchmark groups to run',
    },
    {
      name: 'run',
      type: RegExp,
      description: 'Run only those tests matching the regular expression.',
    },
    {
      name: 'browsers',
      type: browser,
      multiple: true,
      defaultValue: ['chromium'],
      description: `Browsers to run against, any of ${allBrowsers.join(
        ', ',
      )}, or all`,
    },
    {
      name: 'verbose',
      alias: 'v',
      type: Boolean,
      defaultValue: false,
      description: 'Display additional information while running',
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
      name: 'devtools',
      type: Boolean,
      description: 'Opens a browser to run benchmarks manually',
    },
    {
      name: 'help',
      alias: 'h',
      type: Boolean,
      description: 'Show this help message',
    },
  ];

  type Options = {
    format: Format;
    verbose: boolean;
    browsers: readonly (Browser | 'all')[];
    groups?: string[];
    help?: boolean;
    run?: RegExp;
    list?: boolean;
    devtools?: boolean;
  };
  const options = commandLineArgs(optionDefinitions) as Options;

  if (options.help) {
    // oxlint-disable-next-line no-console
    console.log(
      commandLineUsage([
        {content: 'Usage: perf [options...]'},
        {optionList: optionDefinitions},
      ]),
    );
    process.exit();
  }

  if (options.browsers.length === 1 && options.browsers[0] === 'all') {
    options.browsers = allBrowsers;
  }
  if (options.devtools && options.browsers.length !== 1) {
    // oxlint-disable-next-line no-console
    console.error('Exactly one browser may be specified with --devtools');
    process.exit(1);
  }
  if (options.format === 'json' && options.browsers.length !== 1) {
    // oxlint-disable-next-line no-console
    console.error('Exactly one browser may be specified with --format=json');
    process.exit(1);
  }
  if (options.format === 'bmf' && options.browsers.length !== 1) {
    // oxlint-disable-next-line no-console
    console.error('Exactly one browser may be specified with --format=bmf');
    process.exit(1);
  }
  if (
    options.groups === undefined &&
    options.run === undefined &&
    !options.list &&
    !options.devtools
  ) {
    options.groups = ['replicache'];
  }

  const port = await getPort({port: 8080});

  const define = makeDefine('release');

  const server = await createServer({
    define,
    esbuild: {
      define,
    },
    root: rootDir,
    mode: 'production',
  });
  await server.listen(port);

  const userDataDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'replicache-playwright-'),
  );
  let first = true;
  for (const browser of options.browsers) {
    if (!first) {
      logLine('', options);
    }
    first = false;
    assert(browser !== 'all');

    const context =
      await playwright[browser].launchPersistentContext(userDataDir);
    const page = await context.newPage();

    // The perf test should only import out/replicache.
    page.on('request', request => {
      const path = new URL(request.url()).pathname;
      if (path === '/src/replicache.js') {
        // oxlint-disable-next-line no-console
        console.error(
          `The perf test should not load:${path}. The perf tests should use the compiled output`,
        );
        process.exit(1);
      }
    });

    page.on('pageerror', e => {
      // oxlint-disable-next-line no-console
      console.error(e);
      process.exit(1);
    });

    const url = new URL(`http://localhost:${port}/index.html`);
    if (options.devtools) {
      for (const group of options.groups ?? ['replicache']) {
        url.searchParams.append('group', group);
      }
      if (options.run) {
        url.searchParams.append('run', options.run.source);
      }
    }
    await page.goto(url.toString());

    await runInBrowser(browser, page, options);

    if (options.devtools) {
      return;
    }

    if (!options.devtools) {
      // context.close does not terminate! Give it a second.
      await Promise.race([context.close(), wait(1000)]);
    } else {
      await new Promise(resolve => {
        setTimeout(() => resolve(undefined), 2 ** 31 - 1);
      }); // Don't let the dev server stop!
    }
  }

  if (!options.list) {
    logLine('Done!', options);
  }
  try {
    await fs.rm(userDataDir, {recursive: true, force: true});
  } catch {
    // Ignore.
  }
  await server.close();
}

async function runInBrowser(
  browser: Browser,
  page: playwright.Page,
  options: commandLineArgs.CommandLineOptions,
) {
  async function waitForBenchmarks() {
    await page.waitForFunction('typeof benchmarks !==  "undefined"', null, {
      // There is no need to wait for 30s. Things fail much faster.
      timeout: 1000,
    });
  }

  await waitForBenchmarks();

  let benchmarks: BenchmarkMeta[] = await page.evaluate('benchmarks');
  if (options.groups !== undefined) {
    benchmarks = benchmarks.filter(({group}) => options.groups.includes(group));
  }
  if (options.run !== undefined) {
    benchmarks = benchmarks.filter(({name}) => options.run.test(name));
  }

  if (options.devtools || options.list) {
    benchmarks.sort((a, b) => {
      if (a.group !== b.group) {
        return a.group < b.group ? -1 : 1;
      }
      return a.name < b.name ? -1 : 1;
    });
    // oxlint-disable-next-line no-console
    console.log(
      'Available benchmarks (group / name):\n' +
        benchmarks.map(({name, group}) => `${group} / ${name}`).join('\n'),
    );
    if (options.devtools) {
      // oxlint-disable-next-line no-console
      console.log(
        'Run a single benchmark with',
        '`await runBenchmarkByNameAndGroup(name, group)`',
      );
    }
    return;
  }

  const jsonEntries: unknown[] = [];

  let bmf: BencherMetricsFormat = {};

  logLine(
    `Running ${benchmarks.length} benchmarks on ${browserName(browser)}...`,
    options,
  );
  for (const benchmark of benchmarks) {
    const data = await page.evaluate(
      ({name, group}) =>
        // @ts-expect-error This function is run in a different global
        runBenchmarkByNameAndGroup(name, group),
      benchmark,
    );
    if (data[0] === 'error') {
      process.stderr.write(data[1] + '\n');
      process.exit(1);
    }

    if (data[0] === 'result') {
      const result = data[1];

      if (options.format === 'json') {
        jsonEntries.push(result);
      } else if (options.format === 'bmf') {
        bmf = {...bmf, ...toBencherMetricFormat(result)};
      }

      switch (options.format) {
        case 'json':
          jsonEntries.push(...createGithubActionBenchmarkJSONEntries(result));
          break;
        case 'bmf':
          bmf = {...bmf, ...toBencherMetricFormat(result)};
          break;
        case 'replicache':
          logLine(formatAsReplicache(result), options);
          break;
        case 'benchmarkJS':
          logLine(formatAsBenchmarkJS(result), options);
          break;
      }
    }
    await page.reload();
    await waitForBenchmarks();
  }
  if (options.format === 'json') {
    process.stdout.write(JSON.stringify(jsonEntries, undefined, 2) + '\n');
  } else if (options.format === 'bmf') {
    process.stdout.write(JSON.stringify(bmf, undefined, 2) + '\n');
  }
}

main().catch(err => {
  // oxlint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});

function logLine(s: string, options: commandLineArgs.CommandLineOptions) {
  if (options.format !== 'json' && options.format !== 'bmf') {
    process.stdout.write(s + '\n');
  }
}

function wait(n: number) {
  return new Promise(resolve => setTimeout(resolve, n));
}
