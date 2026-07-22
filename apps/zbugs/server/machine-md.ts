/**
 * Pure renderers for the machine-readable markdown view of an issue, served at
 * `/p/:projectName/issue/:id.md` for crawlers and AI agents that cannot run
 * the synced SPA.
 *
 * Issue descriptions and comment bodies are already GitHub-flavored markdown,
 * so they are emitted verbatim; these functions only generate the framing
 * (heading, metadata list, comment headings). Output is deterministic: labels
 * are sorted by name and reactions are grouped by emoji value with sorted
 * logins.
 */

import {compareUTF8} from 'compare-utf8';
import {groupBy} from '../../../packages/shared/src/arrays.ts';

const BASE_URL = 'https://bugs.rocicorp.dev';

type MdUser = {readonly login: string};

type MdEmoji = {
  readonly value: string;
  readonly creator?: MdUser | undefined;
};

type MdLabel = {readonly name: string};

type MdProject = {
  readonly name: string;
  readonly lowerCaseName: string;
};

export type MdComment = {
  readonly created: number;
  readonly body: string;
  readonly creator?: MdUser | undefined;
  readonly emoji?: readonly MdEmoji[] | undefined;
};

export type MdIssue = {
  readonly id: string;
  readonly shortID?: number | null | undefined;
  readonly title: string;
  readonly open: boolean;
  readonly visibility: string;
  readonly created: number;
  readonly modified: number;
  readonly description: string;
  readonly project?: MdProject | undefined;
  readonly creator?: MdUser | undefined;
  readonly assignee?: MdUser | undefined;
  readonly labels?: readonly MdLabel[] | undefined;
  readonly emoji?: readonly MdEmoji[] | undefined;
};

const millisRegex = /\.\d+Z$/;

/** Formats an epoch-milliseconds timestamp as e.g. `2026-07-01T12:34:56Z`. */
function formatDate(epochMillis: number): string {
  return new Date(epochMillis).toISOString().replace(millisRegex, 'Z');
}

const whitespaceRegex = /\s+/g;

/** Collapses all whitespace runs (including newlines) to single spaces. */
function oneLine(s: string): string {
  return s.replace(whitespaceRegex, ' ').trim();
}

export function issueMdPath(
  projectLowerCaseName: string,
  ref: number | string,
): string {
  return `/p/${projectLowerCaseName}/issue/${ref}.md`;
}

function issueHumanURL(
  projectLowerCaseName: string,
  ref: number | string,
): string {
  return `${BASE_URL}/p/${projectLowerCaseName}/issue/${ref}`;
}

function formatReactions(
  emoji: readonly MdEmoji[] | undefined,
): string | undefined {
  if (!emoji || emoji.length === 0) {
    return undefined;
  }
  return [...groupBy(emoji, e => e.value)]
    .toSorted(([a], [b]) => compareUTF8(a, b))
    .map(([value, group]) => {
      const logins = group
        .map(e => e.creator?.login)
        .filter(login => login !== undefined)
        .sort(compareUTF8);
      const who = logins.length > 0 ? ` (${logins.join(', ')})` : '';
      return `${value} ×${group.length}${who}`;
    })
    .join(' · ');
}

export function renderIssueMd(
  issue: MdIssue,
  comments: readonly MdComment[],
  options: {readonly commentsCapped: boolean},
): string {
  const heading =
    // oxlint-disable-next-line eqeqeq
    issue.shortID != null ? `Bug ${issue.shortID}` : `Issue ${issue.id}`;
  const lines: string[] = [];
  lines.push(`# ${heading}: ${oneLine(issue.title)}`, '');

  lines.push(`- Status: ${issue.open ? 'open' : 'closed'}`);
  if (issue.project) {
    lines.push(`- Project: ${issue.project.name}`);
  }
  if (issue.creator) {
    lines.push(`- Creator: @${issue.creator.login}`);
  }
  if (issue.assignee) {
    lines.push(`- Assignee: @${issue.assignee.login}`);
  }
  const labels = (issue.labels ?? []).map(l => l.name).sort(compareUTF8);
  if (labels.length > 0) {
    lines.push(`- Labels: ${labels.join(', ')}`);
  }
  if (issue.visibility !== 'public') {
    lines.push(`- Visibility: ${issue.visibility}`);
  }
  lines.push(`- Created: ${formatDate(issue.created)}`);
  lines.push(`- Modified: ${formatDate(issue.modified)}`);
  const reactions = formatReactions(issue.emoji);
  if (reactions) {
    lines.push(`- Reactions: ${reactions}`);
  }
  if (issue.project) {
    lines.push(
      `- URL: ${issueHumanURL(issue.project.lowerCaseName, issue.shortID ?? issue.id)}`,
    );
  }

  lines.push('', '## Description', '');
  const description = issue.description.trim();
  lines.push(description === '' ? '_No description._' : description, '');

  lines.push(`## Comments (${comments.length})`, '');
  if (options.commentsCapped) {
    lines.push(
      `_Showing the ${comments.length} most recent comments; earlier comments omitted._`,
      '',
    );
  }
  for (const comment of comments) {
    lines.push(
      `### @${comment.creator?.login ?? 'unknown'} — ${formatDate(comment.created)}`,
      '',
    );
    const body = comment.body.trim();
    lines.push(body === '' ? '_No content._' : body, '');
    const commentReactions = formatReactions(comment.emoji);
    if (commentReactions) {
      lines.push(`Reactions: ${commentReactions}`, '');
    }
  }
  return lines.join('\n');
}

export function renderErrorMd(title: string, detail: string): string {
  return `# ${title}\n\n${detail}\n`;
}
