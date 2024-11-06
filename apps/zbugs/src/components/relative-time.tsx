import {useEffect, useState} from 'react';

interface Props {
  timestamp: number;
  absolute?: boolean;
  format?: {
    year?: 'numeric' | '2-digit';
    month?: 'numeric' | '2-digit' | 'long' | 'short' | 'narrow';
    day?: 'numeric' | '2-digit';
    hour?: 'numeric' | '2-digit';
    minute?: 'numeric' | '2-digit';
  };
}

function RelativeTime({timestamp, absolute = false, format}: Props) {
  const now = useNow();
  const fullTimestamp = fullTimestampFormat.format(timestamp);
  return (
    <span title={fullTimestamp}>
      {getRelativeTime(now, timestamp, absolute, format)}
    </span>
  );
}

export default RelativeTime;

const fullTimestampFormat = Intl.DateTimeFormat('en-US', {
  year: 'numeric',
  month: 'short',
  day: 'numeric',
  hour: 'numeric',
  minute: 'numeric',
});

const ONE_MINUTE = 60 * 1000;
const TWO_MINUTES = 2 * ONE_MINUTE;
const ONE_HOUR = 60 * ONE_MINUTE;

function getRelativeTime(
  now: number,
  timestamp: number,
  absolute: boolean,
  format: Props['format'],
) {
  const delta = now - timestamp;

  const timestampDate = new Date(timestamp);
  const timestampYear = timestampDate.getFullYear();
  const currentYear = new Date(now).getFullYear();

  if (timestampYear < currentYear) {
    return formatLongAgo(timestamp);
  }
  // If 'absolute' is true or timestamp is older than 2 days, show the full date and time
  if (absolute || delta > 48 * ONE_HOUR) {
    return timestampDate.toLocaleString('en-US', {
      year:
        format?.year ?? (timestampYear < currentYear ? 'numeric' : undefined),
      month: format?.month ?? 'short',
      day: format?.day ?? 'numeric',
      hour: format?.hour ?? 'numeric',
      minute: format?.minute ?? 'numeric',
      hour12: true,
    });
  }

  if (delta < ONE_MINUTE) {
    return 'just now';
  }
  if (delta < TWO_MINUTES) {
    return '1 minute ago';
  }
  if (delta < ONE_HOUR) {
    return Math.floor(delta / ONE_MINUTE) + ' minutes ago';
  }
  if (delta < 2 * ONE_HOUR) {
    return '1 hour ago';
  }
  if (delta < 24 * ONE_HOUR) {
    return Math.floor(delta / ONE_HOUR) + ' hours ago';
  }
  return '1 day ago';
}

const longAgoFormatter = new Intl.DateTimeFormat('en-US', {
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: 'numeric',
  minute: 'numeric',
});

type LongAgoParts = {
  year: string;
  month: string;
  day: string;
  hour: string;
  minute: string;
  dayPeriod: string;
};

function formatToNamedParts(
  formatter: Intl.DateTimeFormat,
  timestamp: number,
): LongAgoParts {
  const rv: Record<string, string> = {};
  for (const {type, value} of formatter.formatToParts(timestamp)) {
    rv[type] = value;
  }
  return rv as LongAgoParts;
}

function formatLongAgo(timestamp: number) {
  const {year, month, day, hour, minute, dayPeriod} = formatToNamedParts(
    longAgoFormatter,
    timestamp,
  );
  return `${year}/${month}/${day}, ${hour}:${minute} ${dayPeriod}`;
}

const timers: Set<() => void> = new Set();
let intervalID: ReturnType<typeof setInterval> | undefined;

function useSharedInterval(fn: () => void): void {
  useEffect(() => {
    timers.add(fn);
    if (timers.size === 1) {
      intervalID = setInterval(() => {
        for (const fn of timers) {
          fn();
        }
      }, 1_000);
    }
    return () => {
      timers.delete(fn);
      if (timers.size === 0) {
        clearInterval(intervalID);
      }
    };
  });
}

/**
 * The current time in milliseconds. Updates every second.
 */
function useNow(): number {
  const [now, setNow] = useState(Date.now());
  useSharedInterval(() => setNow(Date.now()));
  return now;
}
