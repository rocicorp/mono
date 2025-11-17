export class NotImplementedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NotImplementedError';
  }
}

export type PlannerExceptionKind = 'max_flippable_joins';

export class PlannerException extends Error {
  readonly kind: PlannerExceptionKind;

  constructor(kind: PlannerExceptionKind, message: string) {
    super(message);
    this.name = 'PlannerException';
    this.kind = kind;
  }
}
