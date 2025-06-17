import type {Query} from '@rocicorp/zero';
import type {Role} from '../shared/auth.ts';
import type {Schema} from '../shared/schema.ts';

export function applyIssuePermissions(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  q: Query<Schema, 'issue', any>,
  role: Role | undefined,
) {
  return q.where(({or, cmp, cmpLit}) =>
    or(cmp('visibility', '=', 'public'), cmpLit(role ?? null, '=', 'crew')),
  );
}
