import type {WriteTransaction} from 'zero-client';
import {
  putIssue,
  getIssue,
  Description,
  putIssueDescription,
  putIssueComment,
  Comment,
  Issue,
  commentKey,
  IssueUpdateWithID,
} from './issue';

export type M = typeof mutators;
export const mutators = {
  putIssue: async (
    tx: WriteTransaction,
    {
      issue,
      description,
    }: {
      issue: Issue;
      description: Description;
    },
  ): Promise<void> => {
    await putIssue(tx, issue);
    await putIssueDescription(tx, issue.id, description);
  },
  updateIssues: async (
    tx: WriteTransaction,
    issueUpdates: IssueUpdateWithID[],
  ): Promise<void> => {
    const modified = Date.now();
    for (const {id, issueChanges: changes, descriptionChange} of issueUpdates) {
      const issue = await getIssue(tx, id);
      if (issue === undefined) {
        console.info(`Issue ${id} not found`);
        return;
      }
      const changed = {...issue, ...changes};
      changed.modified = modified;
      await putIssue(tx, changed);
      if (descriptionChange) {
        await putIssueDescription(tx, id, descriptionChange);
      }
    }
  },
  putIssueComment: async (
    tx: WriteTransaction,
    comment: Comment,
    updateIssueModifed = true,
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
    comment: Comment,
  ): Promise<void> => {
    await tx.del(commentKey(comment.issueID, comment.id));
  },
};
