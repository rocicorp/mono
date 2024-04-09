import type {
  ReadonlyJSONValue,
  ReadTransaction,
  WriteTransaction,
} from 'zero-client';
import {z} from 'zod';
import type {Immutable} from './immutable';

export const ISSUE_ENTITY_NAME = `issue`;
export const ISSUE_KEY_PREFIX = `${ISSUE_ENTITY_NAME}/`;
export const issueKey = (id: string) => `${ISSUE_KEY_PREFIX}${id}`;
export const issueID = (key: string) => {
  if (!key.startsWith(ISSUE_KEY_PREFIX)) {
    throw new Error(`Invalid issue key: ${key}`);
  }
  return key.substring(ISSUE_KEY_PREFIX.length);
};

export enum Priority {
  None = 'NONE',
  Low = 'LOW',
  Medium = 'MEDIUM',
  High = 'HIGH',
  Urgent = 'URGENT',
}

export const priorityEnumSchema = z.nativeEnum(Priority);
export type PriorityEnum = z.infer<typeof priorityEnumSchema>;

export const priorityOrderValues: Record<Priority, string> = {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  URGENT: '1',
  // eslint-disable-next-line @typescript-eslint/naming-convention
  HIGH: '2',
  // eslint-disable-next-line @typescript-eslint/naming-convention
  MEDIUM: '3',
  // eslint-disable-next-line @typescript-eslint/naming-convention
  LOW: '4',
  // eslint-disable-next-line @typescript-eslint/naming-convention
  NONE: '5',
};

export enum Status {
  Backlog = 'BACKLOG',
  Todo = 'TODO',
  InProgress = 'IN_PROGRESS',
  Done = 'DONE',
  Canceled = 'CANCELED',
}

export const statusOrderValues: Record<Status, string> = {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  BACKLOG: '1',
  // eslint-disable-next-line @typescript-eslint/naming-convention
  TODO: '2',
  // eslint-disable-next-line @typescript-eslint/naming-convention
  IN_PROGRESS: '3',
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DONE: '4',
  // eslint-disable-next-line @typescript-eslint/naming-convention
  CANCELED: '5',
};

export const statusEnumSchema = z.nativeEnum(Status);
export type StatusEnum = z.infer<typeof statusEnumSchema>;

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
}

const filterEnumSchema = z.nativeEnum(Filter);
export type FilterEnum = z.infer<typeof filterEnumSchema>;

export const issueSchema = z.object({
  id: z.string(),
  title: z.string(),
  priority: priorityEnumSchema,
  status: statusEnumSchema,
  modified: z.number(),
  created: z.number(),
  creatorID: z.string(),
  kanbanOrder: z.string(),
  description: z.string(),
});

export type Issue = Immutable<z.TypeOf<typeof issueSchema>>;
export type IssueUpdate = Omit<Partial<Issue>, 'modified'> & {id: string};

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
  `${COMMENT_KEY_PREFIX}${commentID}`;
export const commentID = (key: string) => {
  if (!key.startsWith(COMMENT_KEY_PREFIX)) {
    throw new Error(`Invalid comment key: ${key}`);
  }
  return key.substring(COMMENT_KEY_PREFIX.length);
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
  `${MEMBER_KEY_PREFIX}${memberId}`;
export const memberID = (key: string) => {
  if (!key.startsWith(MEMBER_KEY_PREFIX)) {
    throw new Error(`Invalid member key: ${key}`);
  }
  return key.substring(MEMBER_KEY_PREFIX.length);
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
