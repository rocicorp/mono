import Chartscii from 'chartscii';
import {doc, getDoc, getFirestore} from 'firebase/firestore';
import {appPath, appViewDataConverter} from 'mirror-schema/src/external/app.js';
import {
  monthMetricsPath,
  monthMetricsViewDataConverter,
  splitDate,
  totalMetricsPath,
  totalMetricsViewDataConverter,
  type DayOfMonth,
  type Hour,
  type MetricsNode,
  type Month,
} from 'mirror-schema/src/external/metrics.js';
import {must} from 'shared/src/must.js';
import {readAppConfig} from './app-config.js';
import {authenticate} from './auth-config.js';
import color from './colors.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function usageOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('year', {
      desc: 'Show summary of a given year, with monthly totals. Defaults to the current year.',
      type: 'number',
    })
    .option('month', {
      desc: 'Show summary of a given month, with daily totals.',
      type: 'number',
      choices: Array.from({length: 12}, (_, i) => i + 1),
    })
    .option('day', {
      describe: 'Show summary of a given day of the month, with hourly totals.',
      choices: Array.from({length: 31}, (_, i) => i + 1),
      type: 'number',
    })
    .option('today', {
      describe: 'Equivalent to --month=<current-month> --day=<current-day>.',
      type: 'boolean',
      conflicts: ['year', 'month', 'day'],
    });
}

type UsageHandlerArgs = YargvToInterface<ReturnType<typeof usageOptions>>;

export async function usageHandler(yargs: UsageHandlerArgs): Promise<void> {
  await authenticate(yargs);
  const firestore = getFirestore();
  const config = readAppConfig();
  const appID = config?.apps?.default?.appID;
  if (!appID) {
    console.info('Publish your app with `npx reflect publish` to view usage');
    return;
  }

  const appDoc = await getDoc(
    doc(firestore, appPath(appID)).withConverter(appViewDataConverter),
  );
  const {teamID} = must(appDoc.data());

  const {year, month, day, today} = yargs;
  const dayView = today || day !== undefined;
  const monthView = !dayView && month !== undefined;
  const yearView = !(dayView || monthView);

  const now = new Date();
  const date = new Date(
    year ?? now.getUTCFullYear(),
    (month ?? now.getUTCMonth() + 1) - 1,
    day ?? now.getUTCDate(),
  );
  const [yyyy, mm, dayOfMonth] = splitDate(date);

  let table: TableData;
  if (yearView) {
    const totalMetrics = (
      await getDoc(
        doc(firestore, totalMetricsPath(teamID, appID)).withConverter(
          totalMetricsViewDataConverter,
        ),
      )
    ).data();
    const yearMetrics = totalMetrics?.year?.[yyyy];
    table = tableData(yearMetrics, yearMetrics?.month, monthRows(date));
  } else {
    const monthMetrics = (
      await getDoc(
        doc(firestore, monthMetricsPath(yyyy, mm, teamID, appID)).withConverter(
          monthMetricsViewDataConverter,
        ),
      )
    ).data();
    if (monthView) {
      table = tableData(monthMetrics, monthMetrics?.day, dayRows(date));
    } else {
      const dayMetrics = monthMetrics?.day?.[dayOfMonth];
      table = tableData(dayMetrics, dayMetrics?.hour, hourRows());
    }
  }

  const chart = new Chartscii(
    table.rows,
    {
      label: chartTitle(date, {yearView, monthView, dayView}, table.total),
      width: 90,
      theme: 'lush',
      colorLabels: true,
      reverse: true,
    } as Chartscii.Options, // chartscii's index.d.ts is not up to date with index.js
  );

  // Add breakdown durations to the end of the bars.
  const lines = chart.create().split('\n');
  let maxDurLen = 0;
  table.rows.forEach((row, i) => {
    if (row.value > 0) {
      const dur = durationString(row.value);
      lines[lines.length - i - 2] += ` ${dur}`;
      maxDurLen = Math.max(maxDurLen, dur.length + 1);
    }
  });
  // Extend the x-axis to cover the added duration strings.
  lines[lines.length - 1] += '═'.repeat(maxDurLen);

  // Output!
  console.log();
  console.log(lines.join('\n'));
}

function chartTitle(
  date: Date,
  view: {yearView: boolean; monthView: boolean; dayView: boolean},
  total: MetricsNode | undefined,
): string {
  const {yearView, dayView} = view;
  const summaryPeriod = new Intl.DateTimeFormat(undefined, {
    year: 'numeric',
    month: yearView ? undefined : 'long',
    day: dayView ? 'numeric' : undefined,
  }).format(date);
  const duration = durationString(total?.total?.cs ?? 0);
  return `Connection time ${
    dayView ? 'on' : 'in'
  } ${summaryPeriod} (UTC): ${color.pink(color.bold(duration))}`;
}

type TableData = {
  total: MetricsNode | undefined;
  rows: {label: string; value: number; color: string}[];
};

function tableData<RowKey extends string>(
  total: MetricsNode | undefined,
  breakdown: {[key in RowKey]?: MetricsNode | undefined} | undefined,
  rowsKeys: RowKeys<RowKey>,
): TableData {
  return {
    total,
    rows: rowsKeys.all.map(key => ({
      label: rowsKeys.label(key),
      value: breakdown?.[key]?.total?.cs ?? 0,
      color: 'pink',
    })),
  };
}

type RowKeys<T extends string> = {
  all: T[];
  label: (key: T) => string;
};

function monthRows(date: Date): RowKeys<Month> {
  const shortMonth = new Intl.DateTimeFormat(undefined, {month: 'short'});
  const year = date.getUTCFullYear();
  return {
    all: Array.from({length: 12}, (_, i) => (i + 1).toString()) as Month[],
    label: month => shortMonth.format(new Date(year, parseInt(month) - 1)),
  };
}

function hourRows(): RowKeys<Hour> {
  return {
    all: Array.from({length: 24}, (_, i) => i.toString()) as Hour[],
    label: hour => `${hour.length < 2 ? '0' : ''}${hour}:00`,
  };
}

function dayRows(date: Date): RowKeys<DayOfMonth> {
  return {
    all: Array.from({length: daysInMonth(date)}, (_, i) =>
      (i + 1).toString(),
    ) as DayOfMonth[],
    label: day => day,
  };
}

export function daysInMonth(date: Date) {
  // Get day 0 of the next month, which is the number of days in this month.
  return new Date(date.getUTCFullYear(), date.getUTCMonth() + 1, 0).getDate();
}

export function durationString(dur: number): string {
  const seconds = Math.round(dur % 60);
  const minutes = Math.floor((dur / 60) % 60);
  const hours = Math.floor(dur / 3600);
  const hh = hours > 0 ? `${hours}:` : '';
  const mm = hours === 0 || minutes >= 10 ? `${minutes}:` : `0${minutes}:`;
  const ss = seconds >= 10 ? `${seconds}` : `0${seconds}`;
  return `${hh}${mm}${ss}`;
}
