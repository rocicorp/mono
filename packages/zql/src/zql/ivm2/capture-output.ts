import type {Input, Output} from './operator.js';
import type {Change, TreeDiff} from './tree-diff.js';

/**
 * A simple output that consumes and stores all pushed changes.
 * TODO(aa): Extend to support storing subdiffs too.
 */
export class CaptureOutput implements Output {
  readonly changes: Change[] = [];

  push(_source: Input, diff: TreeDiff) {
    this.changes.push(...diff);
  }

  reset() {
    this.changes.length = 0;
  }
}
