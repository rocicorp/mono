import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {zeroForTest, MockSocket} from './test-utils.ts';
import {ConnectionStatus} from './connection-status.ts';
import {
  getInternalReplicacheImplForTesting,
  exposedToTestingSymbol,
} from './zero.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {ClientError} from './error.ts';
