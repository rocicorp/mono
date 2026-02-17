import {trace} from '@opentelemetry/api';
import {version} from '../../../../otel/src/version.ts';

export const tracer = trace.getTracer('view-syncer', version);
