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
  readonly runtime: ConsumerRuntime;
  readonly applyMode: ConsumerApplyMode;
  readonly applyMessages: boolean;
  readonly transportMode: ConsumerTransportMode;
  readonly transportAckMode: ConsumerTransportAckMode;
  readonly transportBatchMessages: number;
  readonly protocolMode: ConsumerProtocolMode;
  readonly clientCpuMicros: number;
  readonly slowAckDelayMs: number;
  readonly slowEvery: number;
};

export type ConsumerRuntime = 'inline' | 'worker';

export type ConsumerApplyMode =
  | 'none'
  | 'direct'
  | 'worker-message'
  | 'worker-batch';

export type ConsumerTransportMode = 'in-process' | 'websocket';
export type ConsumerTransportAckMode = 'per-message' | 'cumulative';
export type ConsumerProtocolMode =
  | 'v6'
  | 'v7'
  | 'message-json'
  | 'batch-json'
  | 'batch-compact';

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
  readonly loadPhaseMs: number;
  readonly elapsedMs: number;
  readonly storerDrainMs: number;
  readonly writeLoopTxPerSec: number;
  readonly writeLoopRowsPerSec: number;
  readonly ingestTxPerSec: number;
  readonly ingestRowsPerSec: number;
  readonly fanoutMessages: number;
  readonly fanoutMessagesPerSec: number;
  readonly websocketMessages: number;
  readonly websocketMessagesPerSec: number;
  readonly websocketBytes: number;
  readonly websocketBytesPerSec: number;
  readonly websocketAcks: number;
  readonly websocketAckBytes: number;
  readonly processCpuUserMs: number;
  readonly processCpuSystemMs: number;
  readonly processCpuTotalMs: number;
  readonly processCpuUtilization: number;
  readonly startHeapUsedBytes: number;
  readonly maxHeapUsedBytes: number;
  readonly endHeapUsedBytes: number;
  readonly startRssBytes: number;
  readonly maxRssBytes: number;
  readonly endRssBytes: number;
  readonly p50TxLatencyMs: number;
  readonly p95TxLatencyMs: number;
  readonly p99TxLatencyMs: number;
  readonly subscriberCount: number;
  readonly reconnectCatchup: boolean;
  readonly reconnectCatchupFrom: string | null;
  readonly reconnectMessages: number;
  readonly reconnectLagTx: number;
  readonly reconnectStartedAtTx: number | null;
  readonly reconnectStartedAtMs: number | null;
  readonly reconnectJoinWatermark: string | null;
  readonly reconnectCaughtUpToJoinMs: number | null;
  readonly reconnectCaughtUpToJoinDuringLoad: boolean;
  readonly reconnectFinalWatermark: string | null;
  readonly reconnectCaughtUpToFinalMs: number | null;
  readonly reconnectFinalCatchupWaitMs: number | null;
  readonly reconnectFinalAckedWatermark: string | null;
  readonly reconnectEndLagTx: number | null;
  readonly reconnectMaxAckLagMessages: number | null;
  readonly subscriberAckDelayMs: number;
  readonly subscriberApplyMode: ConsumerApplyMode;
  readonly subscriberApplyMessages: boolean;
  readonly subscriberTransportMode: ConsumerTransportMode;
  readonly subscriberTransportAckMode: ConsumerTransportAckMode;
  readonly subscriberTransportBatchMessages: number;
  readonly subscriberProtocolMode: ConsumerProtocolMode;
  readonly subscriberClientCpuMicros: number;
  readonly avgSubscriberParseMs: number;
  readonly avgSubscriberApplyMs: number;
  readonly avgSubscriberTxApplyMs: number;
  readonly maxSubscriberTxApplyMs: number;
  readonly avgSubscriberClientCpuMs: number;
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

export type LoadConsumerStats = {
  readonly processed: number;
  readonly ackedWatermark: string;
  readonly watermark: string;
  readonly pending: number;
  readonly transportMessages: number;
  readonly transportBytes: number;
  readonly transportAcks: number;
  readonly transportAckBytes: number;
  readonly maxAckLagMessages: number;
  readonly totalAckLagMessages: number;
  readonly totalParseMs: number;
  readonly totalApplyMs: number;
  readonly totalTxApplyMs: number;
  readonly maxTxApplyMs: number;
  readonly txApplySamples: number;
  readonly totalClientCpuMs: number;
  readonly samples: number;
};

export type ConsumerWorkerData = {
  readonly id: string;
  readonly url: string;
  readonly sqlitePath: string | undefined;
  readonly replicaVersion: string;
  readonly protocolMode: ConsumerProtocolMode;
  readonly transportAckMode: ConsumerTransportAckMode;
  readonly applyMode: ConsumerApplyMode;
  readonly workerBatchMessages: number;
  readonly clientCpuMicros: number;
  readonly ackDelayMs: number;
};

export type LoadConsumer = {
  readonly sub: Subscriber;
  readonly stop: () => void;
  readonly done: Promise<void>;
  readonly stats: () => LoadConsumerStats;
};
