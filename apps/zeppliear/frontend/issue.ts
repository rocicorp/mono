import type {
  ReadonlyJSONValue,
  ReadTransaction,
  WriteTransaction,
} from 'replicache';
import {z} from 'zod';
import type {Immutable} from './immutable';

export const ISSUE_KEY_PREFIX = `issue/`;
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
  creator: z.string(),
  kanbanOrder: z.string(),
});

export type Issue = Immutable<z.TypeOf<typeof issueSchema>>;
export type IssueValue = Immutable<z.TypeOf<typeof issueValueSchema>>;
export const issueValueSchema = issueSchema.omit({
  id: true,
});

export type IssueUpdate = {
  issue: Issue;
  issueChanges: Partial<IssueValue>;
  descriptionUpdate?:
    | {
        description: Description;
        descriptionChange: Description;
      }
    | undefined;
};

export type IssueUpdateWithID = Immutable<{
  id: string;
  issueChanges: Partial<IssueValue>;
  descriptionChange?: Description | undefined;
}>;

export async function getIssue(
  tx: ReadTransaction,
  id: string,
): Promise<Issue | undefined> {
  const val = await tx.get(issueKey(id));
  if (val === undefined) {
    return undefined;
  }
  return {
    ...issueValueSchema.parse(val),
    id,
  };
}

export async function putIssue(
  tx: WriteTransaction,
  issue: Issue,
): Promise<void> {
  await tx.set(issueKey(issue.id), issue);
}

export function issueFromKeyAndValue(
  key: string,
  value: ReadonlyJSONValue,
): Issue {
  return {
    ...issueValueSchema.parse(value),
    id: issueID(key),
  };
}

export const DESCRIPTION_KEY_PREFIX = `description/`;
export const descriptionKey = (issueID: string) =>
  `${DESCRIPTION_KEY_PREFIX}${issueID}`;
export const descriptionSchema = z.string();
export const getDescriptionIssueId = (key: string) => {
  if (!key.startsWith(DESCRIPTION_KEY_PREFIX)) {
    throw new Error(`Invalid description key: ${key}`);
  }
  return key.substring(DESCRIPTION_KEY_PREFIX.length);
};

export type Description = Immutable<z.TypeOf<typeof descriptionSchema>>;
export async function getIssueDescription(
  tx: ReadTransaction,
  issueID: string,
): Promise<Description | undefined> {
  const entry = await tx.get(descriptionKey(issueID));
  if (entry === undefined) {
    return undefined;
  }
  return descriptionSchema.parse(entry);
}

export async function putIssueDescription(
  tx: WriteTransaction,
  issueID: string,
  description: Description,
): Promise<void> {
  await tx.set(descriptionKey(issueID), description);
}

export const COMMENT_KEY_PREFIX = `comment/`;
export const commentKey = (issueID: string, commentID: string) =>
  `${COMMENT_KEY_PREFIX}${issueID}/${commentID}`;
export const commentIDs = (key: string) => {
  if (!key.startsWith(COMMENT_KEY_PREFIX)) {
    throw new Error(`Invalid comment key: ${key}`);
  }
  const ids = key.substring(COMMENT_KEY_PREFIX.length).split('/');
  if (ids.length !== 2) {
    throw new Error(`Invalid comment key: ${key}`);
  }
  return {
    issueID: ids[0],
    commentID: ids[1],
  };
};

export const commentSchema = z.object({
  id: z.string(),
  issueID: z.string(),
  created: z.number(),
  body: z.string(),
  creator: z.string(),
});

export type Comment = Immutable<z.TypeOf<typeof commentSchema>>;
export type CommentValue = Immutable<z.TypeOf<typeof commentValueSchema>>;
export const commentValueSchema = commentSchema.omit({
  id: true,
  issueID: true,
});

export async function getIssueComments(
  tx: ReadTransaction,
  issueID: string,
): Promise<Comment[]> {
  const entries = await tx
    .scan({prefix: COMMENT_KEY_PREFIX + issueID})
    .entries()
    .toArray();
  return entries.map(([key, val]) => {
    const ids = commentIDs(key);
    return {
      ...commentValueSchema.parse(val),
      id: ids.commentID,
      issueID: ids.issueID,
    };
  });
}

export async function putIssueComment(
  tx: WriteTransaction,
  comment: Comment,
): Promise<void> {
  await tx.set(commentKey(comment.issueID, comment.id), comment);
}

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
