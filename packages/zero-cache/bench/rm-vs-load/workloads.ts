import {assert} from '../../../shared/src/asserts.ts';
import {
  emptyOperationCounts,
  makeDelete,
  makeInsert,
  makeTransaction,
  makeUpdate,
  watermarkFor,
  type GeneratedTransaction,
  type OperationCounts,
} from './fixtures.ts';
import type {Scenario, ScenarioWorkload} from './types.ts';

export type TransactionGenerator = {
  next(tx: number): GeneratedTransaction;
};

type MixedRowChurnWorkload = Extract<
  ScenarioWorkload,
  {readonly kind: 'mixed-row-churn'}
>;

export function createTransactionGenerator(
  scenario: Scenario,
): TransactionGenerator {
  switch (scenario.workload.kind) {
    case 'insert-only':
      return {
        next: tx => makeTransaction(tx, scenario.rowsPerTx, scenario.payload),
      };
    case 'mixed-row-churn':
      return new MixedRowChurnGenerator(scenario);
  }
}

export function workloadName(workload: ScenarioWorkload): string {
  switch (workload.kind) {
    case 'insert-only':
      return 'insert-only';
    case 'mixed-row-churn':
      return (
        `mixed-row-churn ` +
        `${workload.insertWeight}i/` +
        `${workload.updateWeight}u/` +
        `${workload.deleteWeight}d`
      );
  }
}

export function addOperationCounts(
  total: OperationCounts,
  next: OperationCounts,
) {
  total.insert += next.insert;
  total.update += next.update;
  total.delete += next.delete;
}

class MixedRowChurnGenerator implements TransactionGenerator {
  readonly #scenario: Scenario;
  readonly #workload: MixedRowChurnWorkload;
  readonly #period: number;
  readonly #activeIDs: string[] = [];
  #nextID = 0;
  #opIndex = 0;

  constructor(scenario: Scenario) {
    assert(
      scenario.workload.kind === 'mixed-row-churn',
      'expected mixed-row-churn workload',
    );
    this.#scenario = scenario;
    this.#workload = scenario.workload;
    this.#period =
      this.#workload.insertWeight +
      this.#workload.updateWeight +
      this.#workload.deleteWeight;
    assert(this.#period > 0, 'mixed workload weights must be positive');
  }

  next(tx: number): GeneratedTransaction {
    const watermark = watermarkFor(tx);
    const changes: GeneratedTransaction['changes'] = [
      ['begin', {tag: 'begin'}, {commitWatermark: watermark}],
    ];
    const operationCounts = emptyOperationCounts();

    for (let seq = 0; seq < this.#scenario.rowsPerTx; seq++) {
      const requestedOp = this.#nextRequestedOperation();
      const op = this.#activeIDs.length === 0 ? 'insert' : requestedOp;

      switch (op) {
        case 'insert': {
          const id = `m-${(this.#nextID++).toString(36)}`;
          this.#activeIDs.push(id);
          operationCounts.insert++;
          changes.push([
            'data',
            makeInsert(tx, seq, this.#scenario.payload.bytes, id),
          ]);
          break;
        }
        case 'update': {
          const id = this.#activeIDs[this.#pickIndex(tx, seq)];
          assert(id !== undefined, 'expected active row for update');
          operationCounts.update++;
          changes.push([
            'data',
            makeUpdate(id, tx, seq, this.#scenario.payload.bytes),
          ]);
          break;
        }
        case 'delete': {
          const index = this.#pickIndex(tx, seq);
          const id = this.#activeIDs[index];
          assert(id !== undefined, 'expected active row for delete');
          const last = this.#activeIDs.pop();
          if (last !== undefined && index < this.#activeIDs.length) {
            this.#activeIDs[index] = last;
          }
          operationCounts.delete++;
          changes.push(['data', makeDelete(id)]);
          break;
        }
      }
    }

    changes.push(['commit', {tag: 'commit'}, {watermark}]);
    return {
      watermark,
      changes,
      rows: this.#scenario.rowsPerTx,
      operationCounts,
    };
  }

  #nextRequestedOperation(): keyof OperationCounts {
    const {insertWeight, updateWeight} = this.#workload;
    const slot = this.#opIndex++ % this.#period;
    if (slot < insertWeight) {
      return 'insert';
    }
    if (slot < insertWeight + updateWeight) {
      return 'update';
    }
    return 'delete';
  }

  #pickIndex(tx: number, seq: number): number {
    return (tx * this.#scenario.rowsPerTx + seq) % this.#activeIDs.length;
  }
}
