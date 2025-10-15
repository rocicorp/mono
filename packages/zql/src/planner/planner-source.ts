import type {Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import {
  PlannerConnection,
  type ConnectionCostModel,
} from './planner-connection.ts';
import type {PlannerConstraint} from './planner-constraint.ts';

export type {ConnectionCostModel};

export class PlannerSource {
  readonly name: string;
  readonly #model: ConnectionCostModel;

  constructor(name: string, model: ConnectionCostModel) {
    this.name = name;
    this.#model = model;
  }

  connect(
    sort: Ordering,
    filters: Condition | undefined,
    baseConstraints?: PlannerConstraint,
  ): PlannerConnection {
    return new PlannerConnection(
      this.name,
      this.#model,
      sort,
      filters,
      baseConstraints,
    );
  }
}
