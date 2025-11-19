import type {IssueRow} from '../shared/schema.ts';
import type {ZeroBugs} from '../shared/zero-type.ts';

export function commentQuery(z: ZeroBugs, displayed: IssueRow | undefined) {
  return z.query.comment
    .where('issueID', 'IS', displayed?.id ?? null)
    .related('creator')
    .related('emoji', emoji => emoji.related('creator'))
    .orderBy('created', 'asc')
    .orderBy('id', 'asc');
}
