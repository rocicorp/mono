import type {Subscriber} from '../../src/services/change-streamer/subscriber.ts';
import type {PayloadProfile} from './fixtures.ts';

export type Scenario = {
  readonly name: string;
  readonly rowsPerTx: number;
  readonly payload: PayloadProfile;
  readonly targetTxPerSec: number;
};

export type ConsumerConfig = {
  readonly count: number;
  readonly ackDelayMs: number;
  readonly applyMessages: boolean;
  readonly clientCpuMicros: number;
  readonly slowAckDelayMs: number;
  readonly slowEvery: number;
};

export type ScenarioSummary = {
  readonly name: string;
  readonly rowsPerTx: number;
  readonly payload: string;
  readonly payloadBytes: number;
  readonly targetTxPerSec: number;
  readonly durationMs: number;
  readonly tx: number;
  readonly rows: number;
  readonly storerBytes: number;
  readonly elapsedMs: number;
  readonly storerDrainMs: number;
  readonly ingestTxPerSec: number;
  readonly ingestRowsPerSec: number;
  readonly fanoutMessages: number;
  readonly fanoutMessagesPerSec: number;
  readonly p50TxLatencyMs: number;
  readonly p95TxLatencyMs: number;
  readonly p99TxLatencyMs: number;
  readonly subscriberCount: number;
  readonly reconnectCatchup: boolean;
  readonly reconnectCatchupFrom: string | null;
  readonly reconnectMessages: number;
  readonly subscriberAckDelayMs: number;
  readonly subscriberApplyMessages: boolean;
  readonly subscriberClientCpuMicros: number;
  readonly slowSubscriberAckDelayMs: number;
  readonly slowSubscriberEvery: number;
  readonly maxAckLagMessages: number;
  readonly avgAckLagMessages: number;
};

export type Summary = {
  readonly name: 'zero-cache-rm-vs-load';
  readonly mode: 'smoke' | 'full';
  readonly generatedAt: string;
  readonly rmCount: 1;
  readonly viewSyncerCount: number;
  readonly scenarios: readonly ScenarioSummary[];
};

export type LoadConsumer = {
  readonly sub: Subscriber;
  readonly stop: () => void;
  readonly done: Promise<void>;
  readonly stats: () => {
    readonly processed: number;
    readonly maxAckLagMessages: number;
    readonly totalAckLagMessages: number;
    readonly samples: number;
  };
};
