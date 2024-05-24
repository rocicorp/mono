export function formatDate(
  date?: Date | undefined,
  includeYear = false,
): string {
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    year: includeYear ? 'numeric' : undefined,
  }).format(date);
}

export function timeAgo(
  timestamp?: string | number | Date,
): string | undefined {
  const second = 1000;
  const minute = 60 * 1000;
  const hour = 60 * minute;
  const day = 24 * hour;
  const month = 30 * day;

  if (!timestamp) return undefined;

  if (isNaN(new Date(timestamp).getTime())) {
    throw new Error('Invalid timestamp');
  }

  const difference =
    getCurrentDateWithFutureShift().getTime() - new Date(timestamp).getTime();

  if (Math.floor(difference / month) > 1)
    return formatDate(new Date(timestamp), true);
  if (Math.floor(difference / day) > 1)
    return `${Math.floor(difference / day)} days ago`;
  if (Math.floor(difference / hour) > 1)
    return `${Math.floor(difference / hour)} hours ago`;
  if (Math.floor(difference / minute) > 1)
    return `${Math.floor(difference / minute)} minutes ago`;
  if (Math.floor(difference / second) > 1)
    return `${Math.floor(difference / second)} seconds ago`;
  return `just now`;
}

export function getCurrentDateWithFutureShift() {
  const d = new Date();
  // Shift to 105 years in the future.
  d.setFullYear(d.getFullYear() + 105);
  return d;
}
