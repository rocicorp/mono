import React from 'react';
import './issue-row-loading.css';
import {Priority, Status} from './issue.js';
import PriorityIcon from './priority-icon.js';
import StatusIcon from './status-icon.js';

function IssueRowLoading() {
  return (
    <div className="inline-flex items-center flex-grow flex-shrink w-full min-w-0 pl-2 pr-4 lg:pr-8 text-sm border-b border-gray-850 hover:bg-gray-850 hover:bg-opacity-40 h-11 cursor-pointer text-white border-y-1">
      <div className="flex flex-shrink-0 ml-2 justify-center w-[30px]">
        <PriorityIcon priority={Priority.None} />
      </div>
      <div className="flex flex-shrink-0 ml-1 justify-center w-[30px]">
        <StatusIcon status={Status.Todo} />
      </div>
      <div className="flex-shrink-0 ml-2 flex-grow font-normal sm:block">
        <Pill />
      </div>
      <div className="flex-shrink-0 ml-2 font-normal sm:block"></div>
      <div className="flex-shrink-0 ml-2 font-normal sm:block">
        <Pill width="min-w-[75px]" />
      </div>
    </div>
  );
}

function Pill({width = ''}: {width?: string}) {
  return (
    <span
      className={`rounded-full p-1 px-3 mx-1
      flex
      bg-gradient-to-r 
      from-gray-700
      to-gray-300
      background-animate
      ${width}`}
    >
      &nbsp;
    </span>
  );
}

export default React.memo(IssueRowLoading);
