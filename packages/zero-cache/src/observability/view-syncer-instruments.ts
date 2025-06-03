import {metrics, type Meter} from '@opentelemetry/api';

// intentional lazy initialization so it is not started before the SDK is started.

let meter: Meter | undefined;

export function getMeter() {
  if (!meter) {
    meter = metrics.getMeter('view-syncer');
  }
  return meter;
}
