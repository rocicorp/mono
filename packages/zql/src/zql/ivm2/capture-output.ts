import type {Input, Output} from './operator.js';
import type {Change} from './change.js';

/**
 * A simple output that consumes and stores all pushed changes.
 */
export class CaptureOutput implements Output {
  readonly changes: Change[] = [];

  push(change: Change, _source: Input) {
    this.changes.push(change);
  }

  reset() {
    this.changes.length = 0;
  }
}
