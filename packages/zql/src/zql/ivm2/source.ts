import {Row} from './data.js';
import {Input, Output} from './operator.js';

export type SourceChange = {
  type: 'add' | 'remove';
  row: Row;
};

/**
 * A source is an input that serves as the root data source of the pipeline.
 * Sources can have multiple outputs.
 */
export interface Source extends Input {
  addOutput(output: Output): void;
  push(change: SourceChange): void;
}
