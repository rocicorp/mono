import type {Firestore} from '@google-cloud/firestore';
import {FieldValue} from '@google-cloud/firestore';
import {
  Hour,
  Metrics,
  monthMetricsDataConverter,
  monthMetricsPath,
  totalMetricsDataConverter,
  totalMetricsPath,
  yearMonth,
  type DayOfMonth,
  type Metric,
  type Month,
} from 'mirror-schema/src/metrics.js';

/**
 * The Ledger contains the logic for atomically updating all aggregations of
 * an hourly window of a metric's value. This includes:
 * - daily and monthly totals for the app
 * - yearly and all-time totals for the app
 * - daily and monthly totals for the team
 * - yearly and all-time totals for the team
 */
export class Ledger {
  readonly #firestore: Firestore;

  constructor(firestore: Firestore) {
    this.#firestore = firestore;
  }

  /**
   * Sets the value of the given `metric` for the given `hourWindow`. This
   * replaces any existing value for that window (which means the method is
   * idempotent), and updates aggregations accordingly.
   */
  set(
    teamID: string,
    appID: string,
    hourWindow: Date,
    metric: Metric,
    newValue: number,
  ): Promise<void> {
    return this.#firestore.runTransaction(async tx => {
      const year = hourWindow.getFullYear().toString();
      const month = hourWindow.getMonth().toString() as Month;
      const day = hourWindow.getDate().toString() as DayOfMonth;
      const hour = hourWindow.getHours().toString() as Hour;

      const appMonthDoc = this.#firestore
        .doc(monthMetricsPath(year, month, teamID, appID))
        .withConverter(monthMetricsDataConverter);
      const teamMonthDoc = this.#firestore
        .doc(monthMetricsPath(year, month, teamID))
        .withConverter(monthMetricsDataConverter);
      const appTotalDoc = this.#firestore
        .doc(totalMetricsPath(teamID, appID))
        .withConverter(totalMetricsDataConverter);
      const teamTotalDoc = this.#firestore
        .doc(totalMetricsPath(teamID))
        .withConverter(totalMetricsDataConverter);

      const appMonth = (await tx.get(appMonthDoc)).data();
      const currValue = appMonth?.day?.[day]?.hour?.[hour]?.[metric];
      const delta = newValue - (currValue ?? 0);
      const update: Metrics = {[metric]: FieldValue.increment(delta)};

      const monthUpdate = {
        teamID,
        appID,
        yearMonth: yearMonth(hourWindow),
        total: update,
        day: {
          [day]: {
            total: update,
            hour: {[hour]: update},
          },
        },
      };
      const monthFields = [
        'teamID',
        'appID',
        'yearMonth',
        `total.${metric}`,
        `day.${day}.total.${metric}`,
        `day.${day}.hour.${hour}.${metric}`,
      ];

      tx.set(appMonthDoc, monthUpdate, {mergeFields: monthFields});
      tx.set(
        teamMonthDoc,
        {...monthUpdate, appID: null},
        {mergeFields: monthFields},
      );

      const totalUpdate = {
        teamID,
        appID,
        total: update,
        year: {[year]: update},
      };
      const totalFields = [
        'teamID',
        'appID',
        `total.${metric}`,
        `year.${year}.${metric}`,
      ];

      tx.set(appTotalDoc, totalUpdate, {mergeFields: totalFields});
      tx.set(
        teamTotalDoc,
        {...totalUpdate, appID: null},
        {mergeFields: totalFields},
      );
    });
  }
}
