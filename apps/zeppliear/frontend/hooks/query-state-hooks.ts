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
  type Order,
} from '../issue';
import {
  getViewStatuses,
  hasNonViewFilters as doesHaveNonViewFilters,
} from '../filters';
import {useState} from 'react';
import type {SafeParseReturnType} from 'zod/lib/types';

const processOrderBy: QueryStateProcessor<Order> = {
  toString: (value: Order) => value,
  fromString: (value: string | null) => (value ?? 'MODIFIED') as Order,
};

const stringSetProcessor = {
  toString: (value: Set<string>) => [...value.values()].join(','),
  fromString: (value: string | null) =>
    value === null ? null : new Set(value.split(',')),
};

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

const statusProcessor = makeEnumSetProcessor<Status>(
  statusToStatusString,
  data => statusStringSchema.safeParse(data),
);
export function useStatusFilterState() {
  return useQueryState('statusFilter', statusProcessor);
}

const priorityProcessor = makeEnumSetProcessor<Priority>(
  priorityToPriorityString,
  data => priorityEnumSchema.safeParse(data),
);

export function usePriorityFilterState() {
  return useQueryState('priorityFilter', priorityProcessor);
}

export function useLabelFilterState() {
  return useQueryState('labelFilter', stringSetProcessor);
}

export function useViewState() {
  return useQueryState('view', identityProcessor);
}

export function useIssueDetailState() {
  return useQueryState('iss', identityProcessor);
}

export type FiltersState = {
  statusFilter: Set<Status> | null;
  priorityFilter: Set<Priority> | null;
  labelFilter: Set<string> | null;
  filtersIdentity: string;
  hasNonViewFilters: boolean;
};

function setIdentity(filters: Set<unknown> | null) {
  return filters ? Array.from(filters).join('') : '';
}
export function useFilters(): FiltersState {
  const [statusFilter] = useStatusFilterState();
  const [priorityFilter] = usePriorityFilterState();
  const [labelFilter] = useLabelFilterState();

  const filtersIdentity = [statusFilter, priorityFilter, labelFilter]
    .map(setIdentity)
    .join('-');

  const [prevIdentity, setPrevIdentity] = useState<string | null>(null);
  const [view] = useViewState();
  const [prevView, setPrevView] = useState<string | null>(null);
  const [state, setState] = useState<FiltersState>({
    statusFilter,
    priorityFilter,
    labelFilter,
    filtersIdentity,
    hasNonViewFilters: false,
  });

  if (prevIdentity !== filtersIdentity || prevView !== view) {
    setPrevIdentity(filtersIdentity);
    setPrevView(view);
    const viewStatuses = getViewStatuses(view);
    const hasNonViewFilters = !!doesHaveNonViewFilters(
      viewStatuses,
      statusFilter,
    );
    setState({
      statusFilter,
      priorityFilter,
      labelFilter,
      filtersIdentity,
      hasNonViewFilters,
    });
  }

  return state;
}
