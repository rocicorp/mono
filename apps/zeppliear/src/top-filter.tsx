import {noop} from 'lodash';
import {memo} from 'react';
import MenuIcon from './assets/icons/menu.svg?react';
import FilterMenu from './filter-menu.js';
import {createToggleFilterHandler} from './filters.js';
import {
  useLabelFilterState,
  useOrderByState,
  usePriorityFilterState,
  useStatusFilterState,
  useTextSearchState,
} from './hooks/query-state-hooks.js';
import {Order, Priority, Status} from './issue.js';
import SortOrderMenu from './sort-order-menu.jsx';

interface Props {
  onToggleMenu?: (() => void) | undefined;
}

interface FilterStatusProps<Enum extends number | string> {
  filter: Set<Enum> | null;
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
  if (!filter) return null;
  return (
    <div className="flex items-center pr-4 space-x-[1px]">
      <span className="px-1 text-gray-50 bg-gray-850 rounded-l">
        {label} {operator ?? 'is'}
      </span>
      <span className="px-1 text-gray-50 bg-gray-850 ">
        {displayStrings !== undefined
          ? Array.from(filter)
              .map(f => displayStrings[f])
              .join(', ')
          : Array.from(filter).join(', ')}
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

function TopFilter({onToggleMenu = noop}: Props) {
  const [orderBy, setOrderByParam] = useOrderByState();
  const [statusFilters, setStatusFilterByParam] = useStatusFilterState();
  const [priorityFilters, setPriorityFilterByParam] = usePriorityFilterState();
  const [labelFilters, setLabelFilterByParam] = useLabelFilterState();
  const [textSearch, setTextSearch] = useTextSearchState();

  return (
    <>
      <div
        className="flex justify-between flex-shrink-0 border-b border-gray-850 h-14 border-b-color-gray-50"
        style={{paddingLeft: '1.3rem', paddingRight: '1.3rem'}}
      >
        {/* left section */}
        <div className="flex items-center">
          <button
            className="flex-shrink-0 h-full px-5 focus:outline-none lg:hidden"
            onClick={onToggleMenu}
          >
            <MenuIcon className="w-3.5 text-white hover:text-gray-50" />
          </button>
          <FilterMenu
            onSelectPriority={createToggleFilterHandler(
              priorityFilters,
              setPriorityFilterByParam,
            )}
            onSelectStatus={createToggleFilterHandler(
              statusFilters,
              setStatusFilterByParam,
            )}
            onSelectLabel={createToggleFilterHandler(
              labelFilters,
              setLabelFilterByParam,
            )}
          />
        </div>

        <div className="flex grow items-center mx-3">
          <input
            type="text"
            className="grow border-gray-700 bg-gray-700 opacity-75 hover:opacity-100 focus:opacity-100"
            placeholder="Search"
            value={textSearch ?? ''}
            onChange={e => setTextSearch(e.target.value)}
          />
        </div>

        {/* right section */}
        <div className="flex items-center">
          <SortOrderMenu
            onSelect={orderBy => setOrderByParam(orderBy)}
            order={orderBy ?? Order.Created}
          />
        </div>
      </div>
      {statusFilters || priorityFilters || labelFilters ? (
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
            onDelete={() => setPriorityFilterByParam(null)}
            label="Priority"
          />
          <FilterStatus
            filter={labelFilters}
            onDelete={() => setLabelFilterByParam(null)}
            label="Label"
            operator="is any of"
          />
        </div>
      ) : null}
    </>
  );
}

export default memo(TopFilter);
