// oxlint-disable no-console
/**
 * `pnpm graph` — the workspace package dependency map.
 *
 * ONE model, three outputs:
 *
 *   docs/PACKAGE-GRAPH.md   committed — the diffable view, gated by `--check`
 *   docs/graph/model.json   gitignored — the extracted model itself; every other
 *                           renderer, script, or agent query should read this
 *                           instead of re-walking pnpm
 *   docs/graph/index.html   gitignored — the interactive view of that same model
 *
 * Ported from rindle's `scripts/graph/cli.mjs` + `scripts/js-package-graph.mjs`.
 */

import {spawn} from 'node:child_process';
import {mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import {dirname, relative, resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {REPO_ROOT, serializeModel} from './model.ts';
import {renderHtml} from './render-html.ts';
import {renderMarkdown} from './render-markdown.ts';
import {renderMetrics} from './render-metrics.ts';
import {META, workspaceModel} from './workspace.ts';

const DEFAULTS = {
  output: resolve(REPO_ROOT, META.docPath),
  metrics: resolve(REPO_ROOT, META.metricsPath),
  model: resolve(REPO_ROOT, META.modelPath),
  html: resolve(REPO_ROOT, META.htmlPath),
};

type Options = {
  output: string;
  metrics: string;
  model: string;
  html: string;
  check: boolean;
  stdout: boolean;
  open: boolean;
  help: boolean;
};

const USAGE = `Usage: node ${META.generator} [options]

Generate the workspace package model, the deterministic Markdown + Mermaid view,
the exported-metric catalog, and an interactive HTML view of the same model.

Options:
  --check            fail when either committed Markdown is stale (writes nothing)
  --output <path>    write the package graph Markdown somewhere else
  --metrics <path>   write the metric catalog Markdown somewhere else
  --model <path>     write the model JSON somewhere else
  --html <path>      write the interactive view somewhere else
  --open             open the interactive view once it is written
  --stdout           print the generated Markdown without writing anything
  -h, --help         show this help
`;

function parseArgs(argv: readonly string[]): Options {
  const options: Options = {
    ...DEFAULTS,
    check: false,
    stdout: false,
    open: false,
    help: false,
  };

  const pathOptions = new Map<string, 'output' | 'metrics' | 'model' | 'html'>([
    ['--output', 'output'],
    ['--metrics', 'metrics'],
    ['--model', 'model'],
    ['--html', 'html'],
  ]);

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    const key = pathOptions.get(arg);
    if (arg === '--check') options.check = true;
    else if (arg === '--stdout') options.stdout = true;
    else if (arg === '--open') options.open = true;
    else if (arg === '--help' || arg === '-h') options.help = true;
    else if (key) {
      const value = argv[i + 1];
      if (!value || value.startsWith('-')) {
        throw new Error(`${arg} needs a path`);
      }
      options[key] = resolve(process.cwd(), value);
      i++;
    } else {
      throw new Error(`unknown option: ${arg}`);
    }
  }

  if (options.check && options.stdout) {
    throw new Error('--check and --stdout cannot be combined');
  }
  if (options.check && options.open) {
    throw new Error('--check and --open cannot be combined');
  }
  return options;
}

function writeGenerated(path: string, contents: string): string {
  mkdirSync(dirname(path), {recursive: true});
  writeFileSync(path, contents);
  return relative(REPO_ROOT, path);
}

function openInBrowser(path: string): void {
  const [command, args]: [string, string[]] =
    process.platform === 'darwin'
      ? ['open', [path]]
      : process.platform === 'win32'
        ? ['cmd', ['/c', 'start', '', path]]
        : ['xdg-open', [path]];
  try {
    spawn(command, args, {stdio: 'ignore', detached: true}).unref();
  } catch {
    // Opening a browser is a convenience; the file is already on disk either
    // way.
  }
}

function main(): void {
  let options: Options;
  try {
    options = parseArgs(process.argv.slice(2));
  } catch (error) {
    console.error(`${(error as Error).message}\n\n${USAGE}`);
    process.exitCode = 2;
    return;
  }

  if (options.help) {
    process.stdout.write(USAGE);
    return;
  }

  try {
    const model = workspaceModel();
    const markdown = renderMarkdown(model, options.output);
    const metrics = renderMetrics(model, options.metrics);

    if (options.stdout) {
      process.stdout.write(markdown);
      return;
    }

    if (options.check) {
      const stale = [
        [options.output, markdown],
        [options.metrics, metrics],
      ].filter(([path, expected]) => {
        let current: string | null = null;
        try {
          current = readFileSync(path, 'utf8');
        } catch {
          // A missing output is stale, reported the same actionable way.
        }
        return current !== expected;
      });
      if (stale.length) {
        console.error(
          `${stale
            .map(([path]) => relative(REPO_ROOT, path))
            .join(
              ', ',
            )} stale; run \`pnpm ${META.command}\` and commit the result.`,
        );
        process.exitCode = 1;
        return;
      }
      console.log(
        `Package graph is current (${model.totals.packages} packages, ${model.totals.edges} direct edges, ${model.metricsCatalog?.totals.families ?? 0} metric series).`,
      );
      return;
    }

    const written = [
      writeGenerated(options.output, markdown),
      writeGenerated(options.metrics, metrics),
      writeGenerated(options.model, serializeModel(model)),
      writeGenerated(options.html, renderHtml(model)),
    ];
    console.log(
      `Wrote ${written.join(', ')} (${model.totals.packages} packages, ${model.totals.edges} direct edges, ${model.metricsCatalog?.totals.families ?? 0} metric series).`,
    );
    const unresolved = model.metricsCatalog?.unresolved ?? [];
    if (unresolved.length) {
      console.warn(
        `Unresolved metric declaration sites (catalog is incomplete): ${unresolved
          .map(u => `${u.file}:${u.line}`)
          .join(', ')}`,
      );
    }
    if (model.layerViolations.length) {
      const detail = model.layerViolations
        .map(({from, to}) => `${from} -> ${to}`)
        .join(', ');
      console.warn(
        `Layer inversions (dependency points up a layer): ${detail}`,
      );
    }
    if (options.open) openInBrowser(options.html);
  } catch (error) {
    console.error((error as Error).message);
    process.exitCode = 1;
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main();
}
