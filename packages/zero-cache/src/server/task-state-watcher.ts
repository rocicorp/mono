import type {LogContext} from '@rocicorp/logger';
import {promiseVoid} from '../../../shared/src/resolved-promises.js';
import {sleep} from '../../../shared/src/sleep.js';
import {RunningState} from '../services/running-state.js';
import type {Service} from '../services/service.js';
import type {Terminator} from './life-cycle.js';

const DEFAULT_POLL_INTERVAL_MS = 5000;

// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-lifecycle-explanation.html
const PRE_DRAIN_STATES = ['PROVISIONING', 'PENDING', 'ACTIVATING', 'RUNNING'];

const POST_DRAIN_STATES = [
  'DEACTIVATING',
  'STOPPING',
  'DEPROVISIONING',
  'STOPPED',
];

const ALL_STATES = new Set([...PRE_DRAIN_STATES, ...POST_DRAIN_STATES]);

export class TaskStateWatcher implements Service {
  readonly id = 'task-watcher';
  readonly #lc: LogContext;
  readonly #terminator: Terminator;
  readonly #pollIntervalMs: number;
  readonly #runningState = new RunningState(this.id);

  #lastStatus = '';

  constructor(
    lc: LogContext,
    terminator: Terminator,
    pollIntervalMs = DEFAULT_POLL_INTERVAL_MS,
  ) {
    this.#lc = lc;
    this.#terminator = terminator;
    this.#pollIntervalMs = pollIntervalMs;
  }

  // https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v4-response.html
  // eslint-disable-next-line @typescript-eslint/naming-convention
  updateDesiredStatus(metadata: {DesiredStatus: string}) {
    const {DesiredStatus: desiredStatus} = metadata;

    if (!ALL_STATES.has(desiredStatus)) {
      this.#lc.warn?.('unexpected DesiredStatus value in', metadata);
      return;
    }
    if (this.#lastStatus !== desiredStatus) {
      this.#lc.info?.(`desired task status:`, desiredStatus);
      if (
        PRE_DRAIN_STATES.includes(this.#lastStatus) &&
        POST_DRAIN_STATES.includes(desiredStatus)
      ) {
        this.#lc.info?.('initiating drain');
        this.#terminator.startDrain();
      }
      this.#lastStatus = desiredStatus;
    }
  }

  async run() {
    const containerMetadataURI = process.env['ECS_CONTAINER_METADATA_URI_V4'];
    if (!containerMetadataURI) {
      this.#lc.debug?.(`no task state endpoint to watch`);
      return;
    }

    // https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v4.html
    this.#lc.info?.(`watching task state at ${containerMetadataURI}`);
    const taskStateURI = `${containerMetadataURI}/task`;

    while (this.#runningState.shouldRun()) {
      try {
        const resp = await fetch(taskStateURI);
        if (resp.ok) {
          const metadata = await resp.json();
          this.updateDesiredStatus(metadata);
        }
      } catch (e) {
        this.#lc.warn?.(`error fetching task metadata`, e);
      }
      await Promise.race([
        sleep(this.#pollIntervalMs),
        this.#runningState.stopped(),
      ]);
    }
  }

  stop() {
    this.#runningState.stop(this.#lc);
    return promiseVoid;
  }
}
