import type {WriteTransaction} from 'zero-client';
import {
  putIssue,
  getIssue,
  putIssueComment,
  Comment,
  Issue,
  commentKey,
  Member,
  putMember,
  IssueUpdate,
} from './issue';

export type M = typeof mutators;
export const mutators = {
  putIssue: async (
    tx: WriteTransaction,
    {
      issue,
    }: {
      issue: Issue;
    },
  ): Promise<void> => {
    await putIssue(tx, issue);
  },
  updateIssues: async (
    tx: WriteTransaction,
    {issueUpdates}: {issueUpdates: IssueUpdate[]},
  ): Promise<void> => {
    const modified = Date.now();
    for (const issueUpdate of issueUpdates) {
      const issue = await getIssue(tx, issueUpdate.id);
      if (issue === undefined) {
        console.info(`Issue ${issueUpdate.id} not found`);
        return;
      }
      const changed = {...issue, ...issueUpdate};
      changed.modified = modified;
      await putIssue(tx, changed);
    }
  },
  putIssueComment: async (
    tx: WriteTransaction,
    {
      comment,
      updateIssueModifed = true,
    }: {comment: Comment; updateIssueModifed?: boolean | undefined},
  ): Promise<void> => {
    if (updateIssueModifed) {
      const issue = await getIssue(tx, comment.issueID);
      if (issue === undefined) {
        console.info(`Issue ${comment.issueID} not found`);
        return;
      }
      const changed = {...issue, modified: Date.now()};
      await putIssue(tx, changed);
    }
    await putIssueComment(tx, comment);
  },
  deleteIssueComment: async (
    tx: WriteTransaction,
    {comment}: {comment: Comment},
  ): Promise<void> => {
    await tx.del(commentKey(comment.id));
  },
  putMember: async (
    tx: WriteTransaction,
    {
      member,
    }: {
      member: Member;
    },
  ): Promise<void> => {
    await putMember(tx, member);
  },
};
