import {builder} from '../shared/schema.ts';

export function commentQuery(id: string | null = null) {
  return builder.comment
    .where('issueID', 'IS', id)
    .related('creator')
    .related('emoji', emoji => emoji.related('creator'))
    .orderBy('created', 'asc')
    .orderBy('id', 'asc');
}
