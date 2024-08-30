import {useQuery} from './hooks/use-query.js';
import IssueItem from './issue-item.js';
import type {Issue, Priority} from './issue.js';
import type {IssuesProps} from './issues-props.js';

type Props = {
  onChangePriority?: ((issue: Issue, priority: Priority) => void) | undefined;
  onOpenDetail?: ((issue: Issue) => void) | undefined;
  issueID: string;
  issuesProps: IssuesProps;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type TODO = any;
export function IssueItemByID({
  issuesProps,
  issueID,
  onChangePriority,
  onOpenDetail,
}: Props) {
  const {query, queryDeps} = issuesProps;
  const rows = useQuery(
    query.where('id', '=', issueID),
    queryDeps.concat(issueID),
  );

  return rows.length === 0 ? null : (
    <IssueItem
      issue={rows[0] as TODO}
      onChangePriority={onChangePriority}
      onOpenDetail={onOpenDetail}
    />
  );
}
