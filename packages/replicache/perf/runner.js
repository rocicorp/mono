/* eslint-env node, es2020 */
// @ts-check

import {startDevServer} from '@web/dev-server';
import {esbuildPlugin} from '@web/dev-server-esbuild';
import commandLineArgs from 'command-line-args';
import commandLineUsage from 'command-line-usage';
import * as fs from 'fs/promises';
import getPort from 'get-port';
import * as os from 'os';
import * as path from 'path';
import * as playwright from 'playwright';
import {fileURLToPath} from 'url';
import {makeDefine} from '../tool/make-define.js';

/** @typedef {'chromium' | 'firefox' | 'webkit'} Browser */

const allBrowsers = ['chromium', 'firefox', 'webkit'];

const rootDir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');

class UnknownValueError extends Error {
  name = 'UNKNOWN_VALUE';

  /**
   * @param {string} arg
   * @param {string} optionName
   */
  constructor(arg, optionName) {
    super(`Unknown format ${arg}`);
    this.value = arg;
    this.optionName = '--' + optionName;
  }
}

/**
 * @param {string} arg
 */
function browser(arg) {
  arg = arg.toLowerCase();
  if (!['all', ...allBrowsers].includes(arg)) {
    throw new UnknownValueError(arg, 'browsers');
  }
  return /** @type {Browser | 'all'} */ (arg);
}

/**
 * @param {Browser} browser
 */
function browserName(browser) {
  let name = browser[0].toUpperCase() + browser.slice(1);
  if (name === 'Webkit') {
    name = 'WebKit';
  }
  return name;
}

/**
 * @param {string} arg
 */
function format(arg) {
  if (!['benchmarkJS', 'json', 'replicache'].includes(arg)) {
    throw new UnknownValueError(arg, 'format');
  }
  return /** @type {'benchmarkJS' | 'json' | 'replicache'} */ (arg);
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
      description:
        'Format for text output, either benchmarkJS (default), json or replicache',
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
  const options = commandLineArgs(optionDefinitions);
  if (options.help) {
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
    console.error('Exactly one browser may be specified with --devtools');
    process.exit(1);
  }
  if (options.format === 'json' && options.browsers.length !== 1) {
    console.error('Exactly one browser may be specified with --format=json');
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

  const port = await getPort();
  const server = await startDevServer({
    config: {
      nodeResolve: true,
      rootDir,
      port,
      watch: false,
      plugins: [
        esbuildPlugin({
          ts: true,
          target: 'esnext',
          define: await makeDefine('release'),
        }),
      ],
    },
    readCliArgs: false,
    readFileConfig: false,
    logStartMessage: options.verbose,
  });

  const userDataDir = await fs.mkdtemp(
    path.join(os.tmpdir(), 'replicache-playwright-'),
  );
  let first = true;
  for (const browser of /** @type {Browser[]} */ (options.browsers)) {
    if (!first) {
      logLine('', options);
    }
    first = false;
    const context = await playwright[browser].launchPersistentContext(
      userDataDir,
      {devtools: options.devtools},
    );
    const page = await context.newPage();

    // The perf test should only import out/replicache.
    page.on('request', (/** @type {{ url: () => string }} */ request) => {
      const path = new URL(request.url()).pathname;
      if (path === '/src/replicache.js') {
        console.error(
          `The perf test should not load:${path}. The perf tests should use the compiled output`,
        );
        process.exit(1);
      }
    });

    page.on('pageerror', e => {
      console.error(e);
      process.exit(1);
    });

    await page.goto(`http://127.0.0.1:${port}/perf/index.html`);

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
  await server.stop();
}

/**
 * @param {Browser} browser
 * @param {playwright.Page} page
 * @param {commandLineArgs.CommandLineOptions} options
 */
async function runInBrowser(browser, page, options) {
  async function waitForBenchmarks() {
    await page.waitForFunction('typeof benchmarks !==  "undefined"', null, {
      // There is no need to wait for 30s. Things fail much faster.
      timeout: 1000,
    });
  }

  await waitForBenchmarks();

  /** @type {{name: string, group: string}[]} */
  let benchmarks = await page.evaluate('benchmarks');
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
    console.log(
      'Available benchmarks (group / name):\n' +
        benchmarks.map(({name, group}) => `${group} / ${name}`).join('\n'),
    );
    if (options.devtools) {
      console.log(
        'Run a single benchmark with',
        '`await runBenchmarkByNameAndGroup(name, group)`',
      );
    }
    return;
  }

  const jsonEntries = [];
  logLine(
    `Running ${benchmarks.length} benchmarks on ${browserName(browser)}...`,
    options,
  );
  for (const benchmark of benchmarks) {
    const result = await page.evaluate(
      ({name, group, format}) =>
        // @ts-expect-error This function is run in a different global
        // eslint-disable-next-line no-undef
        runBenchmarkByNameAndGroup(name, group, format),
      {format: options.format, ...benchmark},
    );
    if (result) {
      if (result.error) {
        process.stderr.write(result.error + '\n');
      } else {
        jsonEntries.push(...result.jsonEntries);
        logLine(result.text, options);
      }
    }
    await page.reload();
    await waitForBenchmarks();
  }
  if (options.format === 'json') {
    process.stdout.write(JSON.stringify(jsonEntries, undefined, 2) + '\n');
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});

/**
 * @param {string} s
 * @param {commandLineArgs.CommandLineOptions} options
 */
function logLine(s, options) {
  if (options.format !== 'json') {
    process.stdout.write(s + '\n');
  }
}

/** @param {number} n */
function wait(n) {
  return new Promise(resolve => setTimeout(resolve, n));
}
