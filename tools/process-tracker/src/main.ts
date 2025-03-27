import {consoleLogSink, LogContext} from '@rocicorp/logger';
import find from 'find-process';
import {open} from 'node:fs/promises';
import pidusage from 'pidusage';
import {parseOptions} from '../../../packages/shared/src/options.ts';
import * as v from '../../../packages/shared/src/valita.ts';

const options = {
  pids: v.array(v.number()),
  display: {
    intervalSeconds: v.number().default(10),
  },
  output: {
    every: v.number().default(1),
    file: v.string().optional(),
  },
};

const MB = 1024 * 1024;

async function run() {
  const lc = new LogContext('debug', {}, consoleLogSink);
  const {pids, display, output} = parseOptions(options);

  let timer: NodeJS.Timeout | undefined;

  const names: string[] = [];
  for (const pid of pids) {
    const p = await find('pid', pid);
    if (p.length < 1) {
      lc.error?.(`Could not find process with pid`, pid);
      process.exit(-1);
    }
    names.push(getName(p[0]));
  }

  const file = output.file ? await open(output.file, 'w') : undefined;
  await file?.write(
    ['time', ...names.map(n => [`${n} CPU`, `${n} MEM`]).flat()].join('\t') +
      '\n',
  );
  if (file) {
    lc.info?.(
      `Writing metrics to ${output.file} every ${
        output.every * display.intervalSeconds
      } seconds`,
    );
  }

  lc.info?.();
  lc.info?.(
    names
      .map(n => `${n}${'\t'.repeat(Math.ceil((32 - n.length) / 8))}`)
      .join(''),
  );
  lc.info?.('CPU\t\tMEM\t\t'.repeat(names.length));
  const CLEAR_LINE = '\r' + ' '.repeat(32 * pids.length) + '\r';

  let i = 0;
  async function trackAndDisplay() {
    const stats = await pidusage(pids);
    // Note: The pid may have crashed so `stats[pid]` should
    //       be accessed with the `?.` operator.
    const line = pids
      .map(
        pid =>
          `${(stats[pid]?.cpu ?? 0).toFixed(1)}%\t\t` +
          `${Math.floor((stats[pid]?.memory ?? 0) / MB)}MB`,
      )
      .join('\t\t');
    process.stdout.write(`${CLEAR_LINE}${line}`);

    if (++i % output.every === 0) {
      void file?.write(
        [
          new Date().toISOString(),
          ...pids
            .map(pid => [stats[pid]?.cpu ?? 0, stats[pid]?.memory ?? 0])
            .flat(),
        ]
          .map(n => String(n))
          .join('\t') + '\n',
      );
    }
    timer = setTimeout(trackAndDisplay, display.intervalSeconds * 1000);
  }
  void trackAndDisplay();

  process.on('SIGINT', () => {
    lc.info?.();
    clearTimeout(timer);
  });
}

function getName({name, cmd}: {name: string; cmd: string}) {
  if (name === 'node') {
    const parts = cmd.split(/\s+/);
    for (let i = parts.length - 1; i >= 0; i--) {
      const part = parts[i];
      const lastSlash = part.lastIndexOf('/');
      if (lastSlash > 0) {
        return part.substring(lastSlash + 1);
      }
    }
  }
  return name;
}

await run();
