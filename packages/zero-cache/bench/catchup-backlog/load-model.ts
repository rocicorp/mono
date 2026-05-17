import type {HandoffScenario} from './scenarios.ts';

export function messagesPerSubscriber(scenario: HandoffScenario) {
  if (scenario.messagesPerSubscriber !== undefined) {
    return scenario.messagesPerSubscriber;
  }
  if (!scenario.liveLoad) {
    throw new Error(`scenario ${scenario.name} is missing a message count`);
  }

  return Math.round(
    scenario.liveLoad.transactionsPerSecond *
      scenario.liveLoad.catchupSeconds *
      scenario.liveLoad.messagesPerTransaction,
  );
}

export function assumedCatchupMs(scenario: HandoffScenario) {
  return scenario.liveLoad ? scenario.liveLoad.catchupSeconds * 1000 : 0;
}

export function transactionsPerSecond(scenario: HandoffScenario) {
  return scenario.liveLoad?.transactionsPerSecond;
}

export function formatSeconds(ms: number) {
  return `${(ms / 1000).toFixed(1)}s`;
}
