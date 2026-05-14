export type HandoffScenario = {
  name: string;
  subscribers: number;
  messagesPerSubscriber: number;
  payloadBytes: number;
  yieldEvery: number;
  delayEvery?: number;
  delayMs?: number;
};

export const scenarios = [
  {
    name: 'small-reconnect',
    subscribers: 1,
    messagesPerSubscriber: 10_000,
    payloadBytes: 192,
    yieldEvery: 512,
  },
  {
    name: 'baseline-100k',
    subscribers: 1,
    messagesPerSubscriber: 100_000,
    payloadBytes: 192,
    yieldEvery: 512,
  },
  {
    name: 'large-row-burst',
    subscribers: 1,
    messagesPerSubscriber: 25_000,
    payloadBytes: 4096,
    yieldEvery: 256,
  },
  {
    name: 'slow-downstream',
    subscribers: 1,
    messagesPerSubscriber: 20_000,
    payloadBytes: 192,
    yieldEvery: 128,
    delayEvery: 64,
    delayMs: 1,
  },
  {
    name: '16-vs-reconnect',
    subscribers: 16,
    messagesPerSubscriber: 10_000,
    payloadBytes: 192,
    yieldEvery: 512,
  },
] as const satisfies readonly HandoffScenario[];
