import classNames from 'classnames';
import {memo} from 'react';
import type {Issue, Priority} from './issue.js';
import PriorityMenu from './priority-menu.jsx';

interface IssueItemBaseProps {
  issue: Issue;
  handleIssueItemClick?: () => void;
  handleChangePriority?: (priority: Priority) => void;
}

function IssueItemBase({
  issue,
  handleIssueItemClick = () => null,
  handleChangePriority = () => null,
}: IssueItemBaseProps) {
  return (
    <div
      className={classNames(
        'bg-gray-850 cursor-pointer flex flex-col px-4 py-2 text-white rounded focus:outline-none',
      )}
      onClick={handleIssueItemClick}
    >
      <div className="flex justify-between w-full overflow-hidden">
        <div className="flex flex-col">
          <span className="text-xs font-normal uppercase"></span>
          <span className="mt-1 text-sm font-medium text-gray-50 line-clamp-2 overflow-ellipsis h-10">
            {issue.title}
          </span>
        </div>
      </div>
      <div className="mt-1 flex items-center">
        <PriorityMenu
          onSelect={handleChangePriority}
          labelVisible={false}
          priority={issue.priority}
        />
      </div>
    </div>
  );
}

export default memo(IssueItemBase);
