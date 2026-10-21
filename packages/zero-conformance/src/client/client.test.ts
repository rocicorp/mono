import {test} from 'vitest';
import {emitVectors} from '../vectors.ts';
import {
  runScenario,
  START_TIME,
  type Scenario,
  type StepResult,
} from './harness.ts';
import {scenarios} from './scenarios.ts';

type Case = Scenario & {expect: StepResult[]};

test('client scenarios', {timeout: 600_000}, async () => {
  const cases: Case[] = [];
  for (const scenario of scenarios) {
    cases.push({...scenario, expect: await runScenario(scenario)});
  }
  emitVectors(
    'client',
    'client-scenarios',
    'Client scenarios run against the JS zero-client (TestZero over a MockSocket, fake timers, memory storage): what the app and the server observe after every step. See src/client/harness.ts.',
    cases,
    {startTime: START_TIME},
    {compact: true},
  );
});
