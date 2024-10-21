import React, {useState, useEffect} from 'react';

interface RelativeTimeProps {
  created: string | number | Date;
}

const rtf = new Intl.RelativeTimeFormat('en', {numeric: 'auto'});

const RelativeTime: React.FC<RelativeTimeProps> = ({created}) => {
  const [displayTime, setDisplayTime] = useState<string>('');

  useEffect(() => {
    const getRelativeTime = (createdDate: string | number | Date) => {
      const now = new Date();
      const created = new Date(createdDate);
      const diffInSeconds = Math.floor(
        (now.getTime() - created.getTime()) / 1000,
      );
      const diffInDays = diffInSeconds / 86400;

      const createdYear = created.getFullYear();
      const currentYear = now.getFullYear();

      // If the timestamp is older than 2 days, return a date and time string
      if (diffInDays > 2) {
        return created.toLocaleString('en-US', {
          year: createdYear < currentYear ? 'numeric' : undefined,
          month: 'short',
          day: 'numeric',
          hour: 'numeric',
          minute: 'numeric',
        });
      }

      // Otherwise, calculate relative time
      let unit: Intl.RelativeTimeFormatUnit;
      let value: number;

      if (diffInSeconds < 60) {
        unit = 'second';
        value = diffInSeconds;
      } else if (diffInSeconds < 3600) {
        unit = 'minute';
        value = Math.floor(diffInSeconds / 60);
      } else if (diffInSeconds < 86400) {
        unit = 'hour';
        value = Math.floor(diffInSeconds / 3600);
      } else {
        unit = 'day';
        value = Math.floor(diffInSeconds / 86400);
      }

      return rtf.format(-value, unit);
    };

    const interval = setInterval(() => {
      setDisplayTime(getRelativeTime(created));
    }, 1000);

    setDisplayTime(getRelativeTime(created));

    return () => clearInterval(interval);
  }, [created]);

  return <span>{displayTime}</span>;
};

export default RelativeTime;
