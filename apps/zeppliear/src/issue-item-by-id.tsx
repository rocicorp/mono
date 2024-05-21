import {useQuery} from './hooks/use-zql.js';
import IssueItem from './issue-item.js';
import type {Issue, Priority} from './issue.js';
import type {IssuesProps} from './issues-props.js';

type Props = {
  onChangePriority?: ((issue: Issue, priority: Priority) => void) | undefined;
  onOpenDetail?: ((issue: Issue) => void) | undefined;
  issueID: string;
  issuesProps: IssuesProps;
};

export function IssueItemByID({
  issuesProps,
  issueID,
  onChangePriority,
  onOpenDetail,
}: Props) {
  const {query, queryDeps} = issuesProps;
  const rows = useQuery(
    query.where('issue.id', '=', issueID),
    queryDeps.concat(issueID),
  );

  return rows.length === 0 ? null : (
    <IssueItem
      issue={rows[0].issue}
      onChangePriority={onChangePriority}
      onOpenDetail={onOpenDetail}
    />
  );
}
