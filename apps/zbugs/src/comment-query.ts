import type {Zero} from '@rocicorp/zero';
import type {AuthData} from '../shared/auth.ts';
import type {Mutators} from '../shared/mutators.ts';
import type {IssueRow, Schema} from '../shared/schema.ts';

export function commentQuery(
  z: Zero<Schema, Mutators, AuthData | undefined>,
  displayed: IssueRow | undefined,
) {
  return z.query.comment
    .where('issueID', 'IS', displayed?.id ?? null)
    .related('creator')
    .related('emoji', emoji => emoji.related('creator'))
    .orderBy('created', 'asc')
    .orderBy('id', 'asc');
}
