import {memo} from 'react';
import MenuIcon from './assets/icons/menu.svg';

import {noop} from 'lodash';
//import {queryTypes, useQueryState} from 'next-usequerystate';
import FilterMenu from './filter-menu';
import {
  Order,
  Priority,
  // PriorityString,
  Status,
  // StatusString,
  // priorityFromString,
  // priorityToPriorityString,
  // statusFromString,
  // statusToStatusString,
} from './issue';
import SortOrderMenu from './sort-order-menu';
import {
  useLabelFilterState,
  useOrderByState,
  usePriorityFilterState,
  useStatusFilterState,
} from './hooks/query-state-hooks';

interface Props {
  title: string;
  onToggleMenu?: (() => void) | undefined;
  filteredIssuesCount?: number | undefined;
  issuesCount: number;
  showSortOrderMenu: boolean;
}

interface FilterStatusProps<Enum extends number | string> {
  filter: Enum[] | null;
  displayStrings?: Record<Enum, string> | undefined;
  operator?: string | undefined;
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
  operator,
}: FilterStatusProps<Enum>) {
  if (!filter || filter.length === 0) return null;
  return (
    <div className="flex items-center pr-4 space-x-[1px]">
      <span className="px-1 text-gray-50 bg-gray-850 rounded-l">
        {label} {operator ?? 'is'}
      </span>
      <span className="px-1 text-gray-50 bg-gray-850 ">
        {displayStrings !== undefined
          ? filter.map(f => displayStrings[f]).join(', ')
          : filter.join(', ')}
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
  const [orderBy, setOrderByParam] = useOrderByState();
  const [statusFilters, setStatusFilterByParam] = useStatusFilterState();
  const [priorityFilters, setPriorityFilterByParam] = usePriorityFilterState();
  const [labelFilters, setLabelFilterByParam] = useLabelFilterState();

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
            onSelectPriority={createEnumSetFilterHandler(
              priorityFilters,
              setPriorityFilterByParam,
            )}
            onSelectStatus={createEnumSetFilterHandler(
              statusFilters,
              setStatusFilterByParam,
            )}
            onSelectLabel={createEnumSetFilterHandler(
              labelFilters,
              setLabelFilterByParam,
            )}
          />
        </div>

        {/* right section */}
        <div className="flex items-center">
          {showSortOrderMenu && (
            <SortOrderMenu
              onSelect={orderBy => setOrderByParam(orderBy)}
              order={orderBy ?? Order.Created}
            />
          )}
        </div>
      </div>
      {(statusFilters && statusFilters.size) ||
      (priorityFilters && priorityFilters.size) ||
      (labelFilters && labelFilters.size) ? (
        <div className="flex pl-2 lg:pl-9 pr-6 border-b border-gray-850 h-8">
          <FilterStatus
            filter={statusFilters ? Array.from(statusFilters) : null}
            displayStrings={statusDisplayStrings}
            onDelete={() => setStatusFilterByParam(null)}
            label="Status"
          />
          <FilterStatus
            filter={priorityFilters ? Array.from(priorityFilters) : null}
            displayStrings={priorityDisplayStrings}
            onDelete={() => setPriorityFilterByParam(null)}
            label="Priority"
          />
          <FilterStatus
            filter={labelFilters ? Array.from(labelFilters) : null}
            onDelete={() => setLabelFilterByParam(null)}
            label="Label"
            operator="is any of"
          />
        </div>
      ) : null}
    </>
  );
}

function createEnumSetFilterHandler<T>(
  filters: Set<T> | null,
  setFilters: (f: Set<T> | null) => void,
) {
  return (e: T) => {
    const set = new Set(filters);
    if (set.has(e)) {
      set.delete(e);
    } else {
      set.add(e);
    }
    setFilters(set.size === 0 ? null : set);
  };
}

export default memo(TopFilter);
