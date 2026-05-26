import {assert} from '../../../../shared/src/asserts.ts';

export class MutationAlreadyProcessedError extends Error {
  constructor(clientID: string, received: number, actual: number | bigint) {
    super(
      `Ignoring mutation from ${clientID} with ID ${received} as it was already processed. Expected: ${actual}`,
    );
    assert(
      received < actual,
      () =>
        `MutationAlreadyProcessedError: received (${received}) must be < actual (${actual})`,
    );
  }
}
