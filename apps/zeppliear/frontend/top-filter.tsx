import {memo} from 'react';
import MenuIcon from './assets/icons/menu.svg';

import {noop} from 'lodash';
import {queryTypes, useQueryState} from 'next-usequerystate';
import FilterMenu from './filter-menu';
import {
  Order,
  Priority,
  PriorityString,
  Status,
  StatusString,
  priorityFromString,
  priorityToPriorityString,
  statusFromString,
  statusToStatusString,
} from './issue';
import SortOrderMenu from './sort-order-menu';

interface Props {
  title: string;
  onToggleMenu?: (() => void) | undefined;
  filteredIssuesCount?: number | undefined;
  issuesCount: number;
  showSortOrderMenu: boolean;
}

interface FilterStatusProps<Enum extends number | string> {
  filter: Enum[] | null;
  displayStrings: Record<Enum, string>;
  onDelete: () => void;
  label: string;
}

const priorityDisplayStrings = {
  [Priority.None]: 'None',
  [Priority.Low]: 'Low',
  [Priority.Medium]: 'Medium',
  [Priority.High]: 'High',
  [Priority.Urgent]: 'Urgent',
} as const;

const statusDisplayStrings = {
  [Status.Backlog]: 'Backlog',
  [Status.Todo]: 'Todo',
  [Status.InProgress]: 'In Progress',
  [Status.Done]: 'Done',
  [Status.Canceled]: 'Canceled',
} as const;

function FilterStatus<Enum extends number | string>({
  filter,
  onDelete,
  label,
  displayStrings,
}: FilterStatusProps<Enum>) {
  if (!filter || filter.length === 0) return null;
  return (
    <div className="flex items-center pr-4 space-x-[1px]">
      <span className="px-1 text-gray-50 bg-gray-850 rounded-l">
        {label} is
      </span>
      <span className="px-1 text-gray-50 bg-gray-850 ">
        {filter.map(f => displayStrings[f]).join(', ')}
      </span>
      <span
        className="px-1 text-gray-50 bg-gray-850 rounded-r cursor-pointer"
        onMouseDown={onDelete}
      >
        &times;
      </span>
    </div>
  );
}

function TopFilter({
  title,
  onToggleMenu = noop,
  filteredIssuesCount,
  issuesCount,
  showSortOrderMenu,
}: Props) {
  const [orderBy, setOrderByParam] = useQueryState(
    'orderBy',
    queryTypes
      .stringEnum<Order>(Object.values(Order))
      .withDefault(Order.Modified),
  );
  const [statusStringFilters, setStatusStringFilterByParam] = useQueryState(
    'statusFilter',
    queryTypes.array<StatusString>(
      queryTypes.stringEnum<StatusString>(Object.values(StatusString)),
    ),
  );
  const [priorityStringFilters, setPriorityStringFilterByParam] = useQueryState(
    'priorityFilter',
    queryTypes.array<PriorityString>(
      queryTypes.stringEnum<PriorityString>(Object.values(PriorityString)),
    ),
  );
  const [labelFilters, setLabelFilterByParam] = useQueryState(
    'labelFilter',
    queryTypes.array(queryTypes.string),
  );

  const statusFilters = statusStringFilters?.map(statusFromString) ?? null;
  const setStatusFilterByParam = (value: Status[] | null) =>
    setStatusStringFilterByParam(value && value.map(statusToStatusString));

  const priorityFilters =
    priorityStringFilters?.map(priorityFromString) ?? null;
  const setPriorityFilterByParam = (value: Priority[] | null) =>
    setPriorityStringFilterByParam(
      value && value.map(priorityToPriorityString),
    );

  return (
    <>
      <div className="flex justify-between flex-shrink-0 pl-2 lg:pl-9 pr-2 lg:pr-6 border-b border-gray-850 h-14 border-b-color-gray-50">
        {/* left section */}
        <div className="flex items-center">
          <button
            className="flex-shrink-0 h-full px-5 focus:outline-none lg:hidden"
            onClick={onToggleMenu}
          >
            <MenuIcon className="w-3.5 text-white hover:text-gray-50" />
          </button>
          <div className="p-1 font-semibold cursor-default">{title}</div>
          {filteredIssuesCount ? (
            <span>
              {filteredIssuesCount} / {issuesCount}
            </span>
          ) : (
            <span>{issuesCount}</span>
          )}
          <FilterMenu
            onSelectPriority={async priority => {
              const prioritySet = new Set(priorityFilters);
              if (prioritySet.has(priority)) {
                prioritySet.delete(priority);
              } else {
                prioritySet.add(priority);
              }
              await setPriorityFilterByParam(
                prioritySet.size === 0 ? null : [...prioritySet],
              );
            }}
            onSelectStatus={async status => {
              const statusSet = new Set(statusFilters);
              if (statusSet.has(status)) {
                statusSet.delete(status);
              } else {
                statusSet.add(status);
              }
              await setStatusFilterByParam([...statusSet]);
            }}
            onSelectLabel={async label => {
              const labelSet = new Set(labelFilters);
              if (labelSet.has(label)) {
                labelSet.delete(label);
              } else {
                labelSet.add(label);
              }
              await setLabelFilterByParam([...labelSet]);
            }}
          />
        </div>

        {/* right section */}
        <div className="flex items-center">
          {showSortOrderMenu && (
            <SortOrderMenu
              onSelect={orderBy => setOrderByParam(orderBy)}
              order={orderBy}
            />
          )}
        </div>
      </div>
      {(statusFilters && statusFilters.length) ||
      (priorityFilters && priorityFilters.length) ? (
        <div className="flex pl-2 lg:pl-9 pr-6 border-b border-gray-850 h-8">
          <FilterStatus
            filter={statusFilters}
            displayStrings={statusDisplayStrings}
            onDelete={() => setStatusFilterByParam(null)}
            label="Status"
          />
          <FilterStatus
            filter={priorityFilters}
            displayStrings={priorityDisplayStrings}
            onDelete={() => setPriorityStringFilterByParam(null)}
            label="Priority"
          />
        </div>
      ) : null}
    </>
  );
}

export default memo(TopFilter);
