import type {Zero} from '@rocicorp/zero';
import type {IssueRow, Schema} from '../schema.js';

export function commentQuery(z: Zero<Schema>, displayed: IssueRow | undefined) {
  return z.query.comment
    .where('issueID', 'IS', displayed?.id ?? null)
    .related('creator', creator => creator.one())
    .related('emoji', emoji =>
      emoji.related('creator', creator => creator.one()),
    )
    .orderBy('created', 'asc')
    .orderBy('id', 'asc');
}
