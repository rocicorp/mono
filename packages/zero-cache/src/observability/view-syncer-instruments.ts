import {metrics, type Meter} from '@opentelemetry/api';

// intentional lazy initialization so it is not started before the SDK is started.

let meter: Meter | undefined;

export function getMeter() {
  if (!meter) {
    meter = metrics.getMeter('view-syncer');
  }
  return meter;
}

export function cache<TRet>(): (
  name: string,
  creator: (name: string) => TRet,
) => TRet {
  const instruments = new Map<string, unknown>();
  return (name: string, creator: (name: string) => TRet) => {
    const existing = instruments.get(name);
    if (existing) {
      return existing as TRet;
    }

    const ret = creator(name);
    instruments.set(name, ret);
    return ret;
  };
}
