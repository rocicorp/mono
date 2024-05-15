import {Issue, Order, Priority, Status} from './issue';

export type Op = '<=' | '>=';
export type DateQueryArg = `${number}|${Op}`;

export function hasNonViewFilters(
  viewStatuses: Set<Status>,
  statuses: Set<Status>,
) {
  for (const s of statuses) {
    if (viewStatuses.has(s)) {
      return true;
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
      return new Set();
  }
}

export function getStatuses(statusFilter: Status[] | null): Set<Status> {
  return new Set(statusFilter ? statusFilter : []);
}

export function getPriorities(
  priorityFilter: Priority[] | null,
): Set<Priority> {
  return new Set(priorityFilter ? priorityFilter : []);
}

export function getPriorityFilter(
  priorities: Set<Priority>,
): null | ((issue: Issue) => boolean) {
  if (priorities.size === 0) {
    return null;
  }
  return issue => priorities.has(issue.priority);
}

export function getStatusFilter(
  viewStatuses: Set<Status>,
  statuses: Set<Status>,
): null | ((issue: Issue) => boolean) {
  const filterStatuses = statuses.size === 0 ? viewStatuses : statuses;
  if (filterStatuses.size === 0) {
    return null;
  }
  return issue => filterStatuses.has(issue.status);
}

export function getCreatorFilter(
  creators: Set<string>,
): null | ((issue: Issue) => boolean) {
  if (creators.size === 0) {
    return null;
  }
  return issue => creators.has(issue.creatorID.toLowerCase());
}

export function getViewFilter(
  viewStatuses: Set<Status>,
): (issue: Issue) => boolean {
  return issue =>
    viewStatuses.size === 0 ? true : viewStatuses.has(issue.status);
}

export function getModifiedFilter(
  args: DateQueryArg[] | null,
): ((issue: Issue) => boolean) | null {
  if (args === null) {
    return null;
  }
  return createTimeFilter('modified', args);
}

export function getCreatedFilter(
  args: DateQueryArg[] | null,
): null | ((issue: Issue) => boolean) {
  if (args === null) {
    return null;
  }
  return createTimeFilter('created', args);
}

function createTimeFilter(
  property: 'created' | 'modified',
  args: DateQueryArg[] | null,
): (issue: Issue) => boolean {
  let before: number | null = null;
  let after: number | null = null;
  for (const arg of args || []) {
    const [timePart, op] = arg.split('|') as [string, Op];
    const time = parseInt(timePart);
    switch (op) {
      case '<=':
        before = before ? Math.min(before, time) : time;
        break;
      case '>=':
        after = after ? Math.max(after, time) : time;
        break;
    }
  }
  return issue => {
    if (before && issue[property] > before) {
      return false;
    }
    if (after && issue[property] < after) {
      return false;
    }
    return true;
  };
}

export function getCreators(creatorFilter: string[] | null): Set<string> {
  return new Set(creatorFilter ? creatorFilter : []);
}

export function getTitleFilter(title: string): (issue: Issue) => boolean {
  return issue => issue.title.toLowerCase().includes(title.toLowerCase());
}

export function getIssueOrder(
  view: string | null,
  orderBy: Order | null,
): Order {
  if (view === 'board') {
    return Order.Kanban;
  }
  return orderBy ?? Order.Modified;
}
