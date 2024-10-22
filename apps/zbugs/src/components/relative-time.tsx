import React, {useState, useEffect} from 'react';

interface RelativeTimeProps {
  timestamp: string | number | Date;
  absolute?: boolean;
  format?: {
    year?: 'numeric' | '2-digit';
    month?: 'numeric' | '2-digit' | 'long' | 'short' | 'narrow';
    day?: 'numeric' | '2-digit';
    hour?: 'numeric' | '2-digit';
    minute?: 'numeric' | '2-digit';
  };
}

const RelativeTime: React.FC<RelativeTimeProps> = ({
  timestamp,
  absolute = false,
  format,
}) => {
  const [displayTime, setDisplayTime] = useState<string>('');
  const [fullTimestamp, setFullTimestamp] = useState<string>('');

  useEffect(() => {
    const getRelativeTime = (timestampDate: string | number | Date) => {
      const now = new Date();
      const timestamp = new Date(timestampDate);
      const diffInSeconds = Math.floor(
        (now.getTime() - timestamp.getTime()) / 1000,
      );
      const diffInMinutes = Math.floor(diffInSeconds / 60);
      const diffInHours = Math.floor(diffInMinutes / 60);
      const diffInDays = Math.floor(diffInHours / 24);

      const timestampYear = timestamp.getFullYear();
      const currentYear = now.getFullYear();

      // Full absolute timestamp for the title tag
      setFullTimestamp(
        timestamp.toLocaleString('en-US', {
          year: 'numeric',
          month: 'short',
          day: 'numeric',
          hour: 'numeric',
          minute: 'numeric',
        }),
      );

      if (timestampYear < currentYear) {
        return `${timestampYear}/${String(timestamp.getMonth() + 1).padStart(
          2,
          '0',
        )}/${String(timestamp.getDate()).padStart(
          2,
          '0',
        )}, ${timestamp.toLocaleString('en-US', {
          hour: 'numeric',
          minute: 'numeric',
          hour12: true,
        })}`;
      }

      // If 'absolute' is true or timestamp is older than 2 days, show the full date and time
      if (absolute || diffInDays > 2) {
        return timestamp.toLocaleString('en-US', {
          year:
            format?.year ??
            (timestampYear < currentYear ? 'numeric' : undefined),
          month: format?.month ?? 'short',
          day: format?.day ?? 'numeric',
          hour: format?.hour ?? 'numeric',
          minute: format?.minute ?? 'numeric',
          hour12: true,
        });
      }

      // Short relative format
      if (diffInSeconds < 60) {
        return `${diffInSeconds}s ago`;
      } else if (diffInMinutes < 60) {
        return `${diffInMinutes}m ago`;
      } else if (diffInHours < 24) {
        return `${diffInHours}h ago`;
      } else {
        return `${diffInDays}d ago`;
      }
    };

    const interval = setInterval(() => {
      setDisplayTime(getRelativeTime(timestamp));
    }, 1000);

    setDisplayTime(getRelativeTime(timestamp));

    return () => clearInterval(interval);
  }, [timestamp, absolute, format]);

  return <span title={fullTimestamp}>{displayTime}</span>;
};

export default RelativeTime;
