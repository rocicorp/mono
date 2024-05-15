import useQueryState, {
  identityProcessor,
  QueryStateProcessor,
} from './useQueryState';
import {
  Priority,
  priorityEnumSchema,
  priorityToPriorityString,
  Status,
  statusStringSchema,
  statusToStatusString,
  type Issue,
  type Order,
} from '../issue';
import {
  getPriorities,
  getPriorityFilter,
  getStatuses,
  getStatusFilter,
  getViewStatuses,
  hasNonViewFilters as doesHaveNonViewFilters,
} from '../filters';
import {useState} from 'react';
import type {SafeParseReturnType} from 'zod/lib/types';

const processOrderBy: QueryStateProcessor<Order> = {
  toString: (value: Order) => value,
  fromString: (value: string | null) => (value ?? 'MODIFIED') as Order,
};

function makeStringSetProcessor(): QueryStateProcessor<Set<string>> {
  return {
    toString: (value: Set<string>) => [...value.values()].join(','),
    fromString: (value: string | null) =>
      value === null ? null : new Set(value.split(',')),
  };
}

export function makeEnumSetProcessor<T>(
  toString: (value: T) => string,
  safeParse: (data: unknown) => SafeParseReturnType<string, T>,
): QueryStateProcessor<Set<T>> {
  return {
    toString: (value: Set<T>) => [...value.values()].map(toString).join(','),
    fromString: (value: string | null): Set<T> | null => {
      if (!value) {
        return null;
      }
      const enumSet = new Set<T>();
      for (const p of value.split(',')) {
        const parseResult = safeParse(p.trim());
        if (parseResult.success) {
          enumSet.add(parseResult.data);
        }
      }
      return enumSet;
    },
  };
}

export function useOrderByState() {
  return useQueryState('orderBy', processOrderBy);
}

export function useStatusFilterState() {
  return useQueryState(
    'statusFilter',
    makeEnumSetProcessor<Status>(statusToStatusString, data =>
      statusStringSchema.safeParse(data),
    ),
  );
}

export function usePriorityFilterState() {
  return useQueryState(
    'priorityFilter',
    makeEnumSetProcessor<Priority>(priorityToPriorityString, data =>
      priorityEnumSchema.safeParse(data),
    ),
  );
}

export function useLabelFilterState() {
  return useQueryState('labelFilter', makeStringSetProcessor());
}

export function useViewState() {
  return useQueryState('view', identityProcessor);
}

export function useIssueDetailState() {
  return useQueryState('iss', identityProcessor);
}

export function useFilterStates() {
  const [statusFilter] = useStatusFilterState();
  const [priorityFilter] = usePriorityFilterState();
  const [labelFilter] = useLabelFilterState();

  const statusFilterArray = statusFilter ? Array.from(statusFilter) : [];
  const priorityFilterArray = priorityFilter ? Array.from(priorityFilter) : [];
  const labelFilterArray = labelFilter ? Array.from(labelFilter) : [];

  return {
    statusFilter: statusFilterArray,
    priorityFilter: priorityFilterArray,
    labelFilter: labelFilterArray,
    filtersIdentity: `${statusFilterArray.join('')}-${priorityFilterArray.join(
      '',
    )}-${labelFilterArray.join('')}`,
  };
}

export function useFilters() {
  const baseStates = useFilterStates();
  const [prevIdentity, setPrevIdentity] = useState<string | null>(null);
  const [view] = useViewState();
  const [prevView, setPrevView] = useState<string | null>(null);

  const [state, setState] = useState<{
    filters: ((issue: Issue) => boolean)[];
    hasNonViewFilters: boolean;
  }>({
    filters: [],
    hasNonViewFilters: false,
  });

  if (prevIdentity !== baseStates.filtersIdentity || prevView !== view) {
    setPrevIdentity(baseStates.filtersIdentity);
    setPrevView(view);

    const viewStatuses = getViewStatuses(view);
    const statuses = getStatuses(baseStates.statusFilter);
    const statusFilterFn = getStatusFilter(viewStatuses, statuses);
    const filterFns = [
      statusFilterFn,
      getPriorityFilter(getPriorities(baseStates.priorityFilter)),
    ].filter(f => f !== null) as ((issue: Issue) => boolean)[];

    const hasNonViewFilters = !!(
      doesHaveNonViewFilters(viewStatuses, statuses) ||
      filterFns.filter(f => f !== statusFilterFn).length > 0
    );
    setState({filters: filterFns, hasNonViewFilters});
  }

  return state;
}
