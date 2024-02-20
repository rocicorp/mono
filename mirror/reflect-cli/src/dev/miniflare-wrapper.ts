import {resolver} from '@rocicorp/resolver';
import {Miniflare, MiniflareOptions} from 'miniflare';
import * as readline from 'node:readline';
import color from 'picocolors';
import {getLogger} from '../logger.js';

declare const TESTING: boolean;

let readlineInstanceForTesting: readline.Interface | undefined;

export function fakeCrashForTesting() {
  if (TESTING) {
    readlineInstanceForTesting?.emit('line', 'Segmentation fault');
  }
}

/**
 * This is a wrapper of Miniflare that looks for crashes and restarts the
 * server. It detects crashes by looking at stderr output.
 */
export class MiniflareWrapper {
  #mf: Miniflare;
  readonly #options: MiniflareOptions;
  #readyResolver = resolver<URL>();

  constructor(options: MiniflareOptions) {
    this.#options = {
      ...options,
      handleRuntimeStdio: (stdout, stderr) =>
        this.#handleRuntimeStdio(stdout, stderr),
    };
    this.#mf = this.#createMiniflare();
  }

  dispose(): Promise<void> {
    return this.#mf.dispose();
  }

  get ready(): Promise<URL> {
    return this.#readyResolver.promise;
  }

  async restart() {
    await this.#mf.dispose();
    this.#mf = this.#createMiniflare();
    await this.#mf.ready;
  }

  #createMiniflare() {
    const r = resolver<URL>();
    this.#readyResolver = r;
    const mf = new Miniflare(this.#options);
    mf.ready.then(
      url => r.resolve(url),
      e => r.reject(e),
    );
    return mf;
  }

  #handleRuntimeStdio(
    stdout: NodeJS.ReadableStream,
    stderr: NodeJS.ReadableStream,
  ) {
    readline.createInterface(stdout).on('line', data => getLogger().log(data));
    const rli = readline.createInterface(stderr);
    rli.on('line', data => {
      switch (classifyStdErrMessage(data)) {
        case StdErrClassification.Debug:
          getLogger().log(data);
          return;
        case StdErrClassification.Crash:
          getLogger().error(color.red(data));
          getLogger().error(color.red('Detected server crash...'));
          this.restart().catch(e => {
            getLogger().error('Failed to restart dev server', e);
          });
          return;
        case StdErrClassification.Unknown:
          getLogger().error(color.red(data));
          break;
        case StdErrClassification.Silence:
          break;
      }
    });

    if (TESTING) {
      readlineInstanceForTesting = rli;
    }
  }
}
const enum StdErrClassification {
  Unknown,
  Crash,
  Debug,
  Silence,
}

// Based on observed stderr as well as
// https://github.com/cloudflare/workers-sdk/blob/main/packages/wrangler/src/dev/miniflare.ts#L449
const classifications = new Map<string | RegExp, StdErrClassification>([
  ['Segmentation fault', StdErrClassification.Crash],

  [
    'Not symbolizing stack traces because $LLVM_SYMBOLIZER is not set',
    StdErrClassification.Debug,
  ],

  ['disconnected: worker_do_not_log;', StdErrClassification.Debug],

  // Matches stack traces from workerd
  //  - on unix: groups of 9 hex digits separated by spaces
  //  - on windows: groups of 12 hex digits, or a single digit 0, separated by spaces
  [/stack:( (0|[a-f\d]{4,})){3,}/, StdErrClassification.Silence],
]);

export function classifyStdErrMessage(message: string): StdErrClassification {
  for (const [pattern, classification] of classifications) {
    if (typeof pattern === 'string') {
      if (message.includes(pattern)) {
        return classification;
      }
    } else {
      if (pattern.test(message)) {
        return classification;
      }
    }
  }
  return StdErrClassification.Unknown;
}
