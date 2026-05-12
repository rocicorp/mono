import type {Downstream} from '../../../zero-protocol/src/down.ts';

const preencodedDownstreams = new WeakMap<Downstream, string>();

export function preencodeDownstream(data: Downstream): Downstream {
  preencodedDownstreams.set(data, JSON.stringify(data));
  return data;
}

export function getPreencodedDownstream(data: Downstream): string | undefined {
  return preencodedDownstreams.get(data);
}
