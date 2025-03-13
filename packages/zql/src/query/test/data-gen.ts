import type {Row} from '../../../../zero-protocol/src/data.ts';

export type Dataset = {
  [table: string]: Row[];
};
