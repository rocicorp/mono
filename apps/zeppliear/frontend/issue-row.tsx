import React from 'react';
import {formatDate} from '../util/date';
import {getLabelColor, Issue, Priority, Status} from './issue';
import PriorityMenu from './priority-menu';
import StatusMenu from './status-menu';

interface Props {
  row: {issue: Issue; labels: string[]};
  onChangePriority: (issue: Issue, priority: Priority) => void;
  onChangeStatus: (issue: Issue, status: Status) => void;
  onOpenDetail: (issue: Issue) => void;
}

function IssueRow({
  row,
  onChangePriority,
  onChangeStatus,
  onOpenDetail,
}: Props) {
  const {issue} = row;
  const handleChangePriority = (p: Priority) => onChangePriority(issue, p);
  const handleChangeStatus = (status: Status) => onChangeStatus(issue, status);
  const handleIssueRowClick = () => onOpenDetail(issue);

  return (
    <div
      className="inline-flex items-center flex-grow flex-shrink w-full min-w-0 pl-2 pr-4 lg:pr-8 text-sm border-b border-gray-850 hover:bg-gray-850 hover:bg-opacity-40 h-11 cursor-pointer text-white border-y-1"
      id={issue.id}
      onClick={handleIssueRowClick}
    >
      <div className="flex-shrink-0 ml-2">
        <PriorityMenu
          labelVisible={false}
          onSelect={handleChangePriority}
          priority={issue.priority}
        />
      </div>
      <div className="flex-shrink-0 ml-1">
        <StatusMenu onSelect={handleChangeStatus} status={issue.status} />
      </div>
      <div className="flex-wrap flex-shrink-1 flex-grow ml-2 overflow-hidden font-medium line-clamp-1 overflow-ellipsis">
        {issue.title.slice(0, 3000) || ''}
      </div>
      <div className="flex-shrink-0 ml-2 font-normal sm:block">
        {row.labels.map(label => (
          <span
            key={label}
            className="rounded-full p-1 px-3 mx-1"
            style={{background: getLabelColor(label)}}
          >
            {label}
          </span>
        ))}
      </div>
      <div className="flex-shrink-0 ml-2 font-normal sm:block">
        {formatDate(new Date(issue.modified))}
      </div>
    </div>
  );
}

export default React.memo(IssueRow);
