import type {Zero} from '@rocicorp/zero';
import type {IssueRow, Schema} from '../schema.js';

export function commentQuery(z: Zero<Schema>, displayed: IssueRow | undefined) {
  return z.query.comment
    .where('issueID', 'IS', displayed?.id ?? null)
    .related('creator')
    .related('emoji', emoji => emoji.related('creator'))
    .orderBy('created', 'asc')
    .orderBy('id', 'asc');
}
