/**
 * Summarize ZeroLogLeak SARIF results and fail the run when a leak is found.
 *
 * Writes a markdown table to stdout and, when running under Actions, to the job
 * summary. The CodeQL step logs are extractor chatter and never show the alerts
 * themselves, so without this a run reads as silence either way.
 */
import {appendFileSync, readdirSync, readFileSync} from 'node:fs';
import {join} from 'node:path';

type Region = {startLine?: number};
type ArtifactLocation = {uri?: string};
type PhysicalLocation = {
  artifactLocation?: ArtifactLocation;
  region?: Region;
};
type Location = {physicalLocation?: PhysicalLocation; id?: number};
type Result = {
  message?: {text?: string};
  locations?: Location[];
  relatedLocations?: Location[];
  codeFlows?: unknown[];
};
type Sarif = {runs?: {results?: Result[]}[]};

type Alert = {
  uri: string;
  line: number | string;
  flows: number;
  message: string;
};

// SARIF messages embed `[label](n)`, where n indexes the result's
// relatedLocations. Left alone it renders as a link to nowhere.
const EMBEDDED_LINK = /\[([^\]]+)\]\((\d+)\)/g;

function place(location: Location | undefined): [string, number | string] {
  const physical = location?.physicalLocation;
  return [
    physical?.artifactLocation?.uri ?? '?',
    physical?.region?.startLine ?? '?',
  ];
}

/** Base URL for linking a path into the tree, when running under Actions. */
function blobUrl(): string | undefined {
  const {GITHUB_SERVER_URL, GITHUB_REPOSITORY, GITHUB_SHA} = process.env;
  if (GITHUB_SERVER_URL && GITHUB_REPOSITORY && GITHUB_SHA) {
    return `${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/blob/${GITHUB_SHA}`;
  }
  return undefined;
}

/**
 * Number of distinct source-to-sink paths behind one alert.
 *
 * Alerts sharing a sink are combined into a single SARIF result carrying one
 * message per path, so the alert count alone understates how much there is to
 * review.
 */
function flowCount(result: Result): number {
  const messages = (result.message?.text ?? '').split('\n').length;
  return Math.max(result.codeFlows?.length ?? 0, messages, 1);
}

/** Rewrites the embedded links to point at the source of the value. */
function resolveLinks(
  text: string,
  result: Result,
  base: string | undefined,
): string {
  const targets = new Map<string, Location>();
  for (const location of result.relatedLocations ?? []) {
    if (location.id !== undefined) {
      targets.set(String(location.id), location);
    }
  }
  return text.replace(EMBEDDED_LINK, (_match, label: string, id: string) => {
    const target = targets.get(id);
    if (!target) {
      return label;
    }
    const [uri, line] = place(target);
    return base
      ? `[${label} (${uri}:${line})](${base}/${uri}#L${line})`
      : `${label} (${uri}:${line})`;
  });
}

function main(directory: string): number {
  const base = blobUrl();
  const alerts: Alert[] = [];
  const files = readdirSync(directory)
    .filter(name => name.endsWith('.sarif'))
    .sort();
  for (const name of files) {
    const sarif: Sarif = JSON.parse(
      readFileSync(join(directory, name), 'utf8'),
    );
    for (const run of sarif.runs ?? []) {
      for (const result of run.results ?? []) {
        const [uri, line] = place(result.locations?.[0]);
        // A combined message holds one sentence per path; keep the table to one
        // line per alert and let the Flows column carry the rest.
        const [first = ''] = (result.message?.text ?? '').split('\n');
        alerts.push({
          uri,
          line,
          flows: flowCount(result),
          message: resolveLinks(first, result, base).replaceAll('|', '\\|'),
        });
      }
    }
  }

  const flows = alerts.reduce((total, alert) => total + alert.flows, 0);
  const lines = [
    '# Log leak analysis',
    '',
    'An **alert** is one log call the query flagged. A **flow** is one',
    'source-to-sink path reaching it, so a single call can carry several.',
    '',
    `## ${alerts.length} alert(s), ${flows} flow(s)`,
    '',
  ];
  if (alerts.length > 0) {
    lines.push('| Location | Flows | Message |', '| --- | --- | --- |');
    for (const {uri, line, flows: count, message} of alerts) {
      const cell = base
        ? `[${uri}:${line}](${base}/${uri}#L${line})`
        : `${uri}:${line}`;
      lines.push(`| ${cell} | ${count} | ${message} |`);
    }
    lines.push(
      '',
      `**FAILED**: ${alerts.length} leak(s) found across ${flows} flow(s).`,
    );
  } else {
    lines.push('No customer data reaching a log or error message.');
  }

  const report = lines.join('\n') + '\n';
  process.stdout.write(report);
  const summary = process.env.GITHUB_STEP_SUMMARY;
  if (summary) {
    appendFileSync(summary, report);
  }
  return alerts.length > 0 ? 1 : 0;
}

process.exitCode = main(process.argv[2] ?? 'sarif-results');
