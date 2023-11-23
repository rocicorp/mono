import {FieldValue, type Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {
  Metrics,
  monthMetricsDataConverter,
  monthMetricsPath,
  splitDate,
  totalMetricsDataConverter,
  totalMetricsPath,
  yearMonth,
  type Metric,
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
   * Sets the values of the `newMetrics` for the given `hourWindow`. This
   * replaces any existing values for that window (which means the method is
   * idempotent), and updates aggregations accordingly.
   *
   * If the values for the `newMetrics` are equivalent to existing values, no writes are
   * performed. This makes it reasonable to query and update aggregated values
   * redundantly; only changed values (e.g. new data) will incur writes to
   * Firestore.
   *
   * @return `true` if an update was written, `false` if the operation was a no-op.
   */
  set(
    teamID: string,
    appID: string,
    hourWindow: Date,
    newMetrics: Map<Metric, number>,
  ): Promise<boolean> {
    const timeDesc = hourWindow.toISOString().split(':')[0];
    const window = `${teamID}/${appID}/${timeDesc}[${[...newMetrics.keys()]}]`;

    return this.#firestore.runTransaction(async tx => {
      const [year, month, day, hour] = splitDate(hourWindow);

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
      const currMetrics = appMonth?.day?.[day]?.hour?.[hour]?.total;
      const update: Metrics = Object.fromEntries(
        [...newMetrics]
          .map(([metric, newValue]) => {
            const currValue = currMetrics?.[metric] ?? 0;
            const delta = newValue - currValue;
            return [metric, delta] as [Metric, number];
          })
          .filter(([_, delta]) => Math.abs(delta) > 1e-11) // Ignore insignificant floating point deltas.
          .map(
            ([metric, delta]) =>
              [metric, FieldValue.increment(delta)] as [Metric, FieldValue],
          ),
      );
      if (Object.keys(update).length === 0) {
        logger.info(`No metrics update for ${window}`);
        return false;
      }
      logger.info(`Updating metrics ${window}`, currMetrics, newMetrics);

      const monthUpdate = {
        teamID,
        appID,
        yearMonth: yearMonth(hourWindow),
        total: update,
        day: {
          [day]: {
            total: update,
            hour: {[hour]: {total: update}},
          },
        },
      };
      const monthFields = ['teamID', 'appID', 'yearMonth'].concat(
        Object.keys(update).flatMap(metric => [
          `total.${metric}`,
          `day.${day}.total.${metric}`,
          `day.${day}.hour.${hour}.total.${metric}`,
        ]),
      );

      tx.set(appMonthDoc, monthUpdate, {mergeFields: monthFields});
      tx.set(
        teamMonthDoc,
        {...monthUpdate, appID: null},
        {mergeFields: monthFields},
      );

      const totalUpdate = {
        teamID,
        appID,
        yearMonth: null,
        total: update,
        year: {
          [year]: {
            total: update,
            month: {[month]: {total: update}},
          },
        },
      };
      const totalFields = ['teamID', 'appID', 'yearMonth'].concat(
        Object.keys(update).flatMap(metric => [
          `total.${metric}`,
          `year.${year}.total.${metric}`,
          `year.${year}.month.${month}.total.${metric}`,
        ]),
      );

      tx.set(appTotalDoc, totalUpdate, {mergeFields: totalFields});
      tx.set(
        teamTotalDoc,
        {...totalUpdate, appID: null},
        {mergeFields: totalFields},
      );
      return true;
    });
  }
}
