import {assert} from '../../../../shared/src/asserts.ts';

export class MutationAlreadyProcessedError extends Error {
  /**
   * The client's current last mutation ID. Kept out of the message, which is
   * returned to the client; the server logs it separately.
   */
  readonly lastMutationID: number | bigint;

  constructor(clientID: string, received: number, actual: number | bigint) {
    super(
      `Ignoring mutation from ${clientID} with ID ${received} as it was already processed`,
    );
    this.lastMutationID = actual;
    assert(
      received < actual,
      () =>
        `MutationAlreadyProcessedError: received (${received}) must be < actual (${actual})`,
    );
  }
}
