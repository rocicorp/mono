import type {Immutable} from 'shared/src/immutable';
import type {
  ReadonlyJSONValue,
  ReadTransaction,
  WriteTransaction,
} from 'zero-client';
import {z} from 'zod';

const ENTITIES_KEY_PREFIX = 'e/';

export const ISSUE_ENTITY_NAME = `issue`;
export const ISSUE_KEY_PREFIX = `${ISSUE_ENTITY_NAME}/`;
export const issueKey = (id: string) =>
  `${ENTITIES_KEY_PREFIX}${ISSUE_KEY_PREFIX}${id}`;
const labelKey = (id: string) => `${ENTITIES_KEY_PREFIX}label/${id}`;
const issueLabelKey = (id: string) => `${ENTITIES_KEY_PREFIX}issueLabel/${id}`;
export const issueID = (key: string) => {
  if (!key.startsWith(`${ENTITIES_KEY_PREFIX}${ISSUE_KEY_PREFIX}`)) {
    throw new Error(`Invalid issue key: ${key}`);
  }
  return key.substring(`${ENTITIES_KEY_PREFIX}${ISSUE_KEY_PREFIX}`.length);
};

export const enum Priority {
  None = 1,
  Low,
  Medium,
  High,
  Urgent,
}

export const prioritySchema = z.union([
  z.literal(Priority.None),
  z.literal(Priority.Low),
  z.literal(Priority.Medium),
  z.literal(Priority.High),
  z.literal(Priority.Urgent),
]);

export enum PriorityString {
  None = 'NONE',
  Low = 'LOW',
  Medium = 'MEDIUM',
  High = 'HIGH',
  Urgent = 'URGENT',
}

const labelColors = [
  '#483D8B', // Dark Slate Blue
  '#191970', // Midnight Blue
  '#36454F', // Charcoal
  '#556B2F', // Dark Olive Green
  '#4B0082', // Indigo
  '#333333', // Dark Charcoal
  '#2F4F4F', // Dark Slate Gray
  '#0C1021', // Onyx
];

export function getLabelColor(labelName: string) {
  const charCode = labelName.charCodeAt(2) || labelName.charCodeAt(0);
  return labelColors[charCode % labelColors.length];
}

export const priorityEnumSchema = z
  .nativeEnum(PriorityString)
  .transform(priorityFromString);

export function priorityFromString(priority: string): Priority {
  switch (priority) {
    case PriorityString.None:
      return Priority.None;
    case PriorityString.Low:
      return Priority.Low;
    case PriorityString.Medium:
      return Priority.Medium;
    case PriorityString.High:
      return Priority.High;
    case PriorityString.Urgent:
      return Priority.Urgent;
  }
  throw new Error('Invalid priority');
}

export function priorityToPriorityString(priority: Priority): PriorityString {
  switch (priority) {
    case Priority.None:
      return PriorityString.None;
    case Priority.Low:
      return PriorityString.Low;
    case Priority.Medium:
      return PriorityString.Medium;
    case Priority.High:
      return PriorityString.High;
    case Priority.Urgent:
      return PriorityString.Urgent;
  }
}

export enum StatusString {
  Backlog = 'BACKLOG',
  Todo = 'TODO',
  InProgress = 'IN_PROGRESS',
  Done = 'DONE',
  Canceled = 'CANCELED',
}

export const statusStringSchema = z
  .nativeEnum(StatusString)
  .transform(statusFromString);

export const enum Status {
  Backlog = 1,
  Todo,
  InProgress,
  Done,
  Canceled,
}

export const statusSchema = z.union([
  z.literal(Status.Backlog),
  z.literal(Status.Todo),
  z.literal(Status.InProgress),
  z.literal(Status.Done),
  z.literal(Status.Canceled),
]);

export function statusToStatusString(status: Status): StatusString {
  switch (status) {
    case Status.Backlog:
      return StatusString.Backlog;
    case Status.Todo:
      return StatusString.Todo;
    case Status.InProgress:
      return StatusString.InProgress;
    case Status.Done:
      return StatusString.Done;
    case Status.Canceled:
      return StatusString.Canceled;
  }
}

export function statusFromString(status: string): Status {
  switch (status) {
    case StatusString.Backlog:
      return Status.Backlog;
    case StatusString.Todo:
      return Status.Todo;
    case StatusString.InProgress:
      return Status.InProgress;
    case StatusString.Done:
      return Status.Done;
    case StatusString.Canceled:
      return Status.Canceled;
  }
  throw new Error('Invalid status');
}

export enum Order {
  Created = 'CREATED',
  Modified = 'MODIFIED',
  Status = 'STATUS',
  Priority = 'PRIORITY',
  Kanban = 'KANBAN',
}

export const orderEnumSchema = z.nativeEnum(Order);
export type OrderEnum = z.infer<typeof orderEnumSchema>;

export enum Filter {
  Priority,
  Status,
  Label,
}

const filterEnumSchema = z.nativeEnum(Filter);
export type FilterEnum = z.infer<typeof filterEnumSchema>;

export const issueSchema = z.object({
  id: z.string(),
  title: z.string(),
  priority: prioritySchema,
  status: statusSchema,
  modified: z.number(),
  created: z.number(),
  creatorID: z.string(),
  kanbanOrder: z.string(),
  description: z.string(),
});

export type Issue = Immutable<z.TypeOf<typeof issueSchema>>;
export type IssueUpdate = Omit<Partial<Issue>, 'modified'> & {id: string};
export type Label = {id: string; name: string};
export type IssueLabel = {id: string; issueID: string; labelID: string};
export type IssueWithLabels = {issue: Issue; labels: string[]};

export async function getIssue(
  tx: ReadTransaction,
  id: string,
): Promise<Issue | undefined> {
  const val = await tx.get(issueKey(id));
  if (val === undefined) {
    return undefined;
  }
  return issueSchema.parse(val);
}

export async function putIssue(
  tx: WriteTransaction,
  issue: Issue,
): Promise<void> {
  await tx.set(issueKey(issue.id), issue);
}

export function issueFromKeyAndValue(
  _key: string,
  value: ReadonlyJSONValue,
): Issue {
  return issueSchema.parse(value);
}

export const COMMENT_ENTITY_NAME = `comment`;
export const COMMENT_KEY_PREFIX = `${COMMENT_ENTITY_NAME}/`;
export const commentKey = (commentID: string) =>
  `${ENTITIES_KEY_PREFIX}${COMMENT_KEY_PREFIX}${commentID}`;
export const commentID = (key: string) => {
  if (!key.startsWith(`${ENTITIES_KEY_PREFIX}${COMMENT_KEY_PREFIX}`)) {
    throw new Error(`Invalid comment key: ${key}`);
  }
  return key.substring(`${ENTITIES_KEY_PREFIX}${COMMENT_KEY_PREFIX}`.length);
};

export const commentSchema = z.object({
  id: z.string(),
  issueID: z.string(),
  created: z.number(),
  body: z.string(),
  creatorID: z.string(),
});

export type Comment = Immutable<z.TypeOf<typeof commentSchema>>;

export async function putIssueComment(
  tx: WriteTransaction,
  comment: Comment,
): Promise<void> {
  await tx.set(commentKey(comment.id), comment);
}

export const MEMBER_KEY_PREFIX = `member/`;
export const memberKey = (memberId: string) =>
  `${ENTITIES_KEY_PREFIX}${MEMBER_KEY_PREFIX}${memberId}`;
export const memberID = (key: string) => {
  if (!key.startsWith(`${ENTITIES_KEY_PREFIX}${MEMBER_KEY_PREFIX}`)) {
    throw new Error(`Invalid member key: ${key}`);
  }
  return key.substring(`${ENTITIES_KEY_PREFIX}${MEMBER_KEY_PREFIX}`.length);
};

export async function getMember(
  tx: ReadTransaction,
  id: string,
): Promise<Member | undefined> {
  const val = await tx.get(memberKey(id));
  if (val === undefined) {
    return undefined;
  }
  return memberSchema.parse(val);
}

export async function putMember(
  tx: WriteTransaction,
  member: Member,
): Promise<void> {
  await tx.set(memberKey(member.id), member);
}

export async function putLabel(
  tx: WriteTransaction,
  label: Label,
): Promise<void> {
  await tx.set(labelKey(label.id), label);
}

export async function putIssueLabel(
  tx: WriteTransaction,
  issueLabel: IssueLabel,
): Promise<void> {
  await tx.set(issueLabelKey(issueLabel.id), issueLabel);
}

export const memberSchema = z.object({
  id: z.string(),
  name: z.string(),
});

export type Member = Immutable<z.TypeOf<typeof memberSchema>>;

const REVERSE_TIMESTAMP_LENGTH = Number.MAX_SAFE_INTEGER.toString().length;

export function reverseTimestampSortKey(timestamp: number, id: string): string {
  return (
    Math.floor(Number.MAX_SAFE_INTEGER - timestamp)
      .toString()
      .padStart(REVERSE_TIMESTAMP_LENGTH, '0') +
    '-' +
    id
  );
}
