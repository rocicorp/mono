import {Order, Status, orderEnumSchema} from './issue.js';

export function hasNonViewFilters(
  viewStatuses: Set<Status>,
  statuses: Set<Status> | null,
) {
  if (statuses) {
    for (const s of statuses) {
      if (viewStatuses.has(s)) {
        return true;
      }
    }
  }
  return false;
}

export function getViewStatuses(view: string | null): Set<Status> {
  switch (view?.toLowerCase()) {
    case 'active':
      return new Set([Status.InProgress, Status.Todo]);
    case 'backlog':
      return new Set([Status.Backlog]);
    default:
      return new Set([
        Status.Backlog,
        Status.Todo,
        Status.InProgress,
        Status.Done,
        Status.Canceled,
      ]);
  }
}

export function getIssueOrder(
  view: string | null,
  orderBy: string | null,
): Order {
  if (view === 'board') {
    return Order.Kanban;
  }
  const parseResult = orderEnumSchema.safeParse(orderBy);
  return parseResult.success ? parseResult.data : Order.Modified;
}

export function createToggleFilterHandler<T>(
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
