import {Order, orderEnumSchema} from './issue.js';

export function getIssueOrder(orderBy: string | null): Order {
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
