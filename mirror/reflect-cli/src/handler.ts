import {getFirestore, terminate} from 'firebase/firestore';
import type {WarmupCaller} from 'mirror-protocol/src/call.js';
import color from 'picocolors';
import {assert} from 'shared/src/asserts.js';
import type {ArgumentsCamelCase} from 'yargs';
import {AuthenticatedUser, authenticate} from './auth-config.js';
import {reportE} from './error.js';
import {sendAnalyticsEvent} from './metrics/send-ga-event.js';
import {Requester, makeRequester} from './requester.js';
import type {CommonYargsOptions} from './yarg-types.js';
import {getLogger, setLoggerType} from './logger.js';
// Wraps a command handler with cleanup code (e.g. terminating any Firestore client)
// to ensure that the process exits after the handler completes.

export type AuthContext = {requester: Requester; user: AuthenticatedUser};

export function authenticateAndHandleWith<
  T extends ArgumentsCamelCase<CommonYargsOptions>,
>(handler: (args: T, context: AuthContext) => void | Promise<void>) {
  let callers: WarmupCaller[] = [];
  const builder = {
    withWarmup: (...c: WarmupCaller[]) => {
      assert(callers.length === 0); // Don't call withWarmup() more than once.
      callers = c;
      return builder;
    },

    andCleanup: () => async (args: T) => {
      callers.forEach(caller => caller.warm());

      const eventName =
        args._ && args._.length ? `cmd_${args._[0]}` : 'cmd_unknown';

      if (args['output'] === 'json' || args['output'] === 'text') {
        setLoggerType(args['output']);
      }
      const user = await authenticate(args);
      const requester = makeRequester(user.userID);

      const context = {requester, user};
      // It is tempting to send analytics in parallel with running
      // the handler, but that appears to cause problems for some commands
      // for reasons unknown.
      // https://github.com/rocicorp/mono/issues/1078
      // we do not send analytic events for a non logged in user
      try {
        // Promise race to handle sendAnalyticsEvent with a 3-second timeout
        await Promise.race([
          sendAnalyticsEvent(eventName, user),
          new Promise(resolve => setTimeout(resolve, 3_000)),
        ]);
      } catch (e) {
        await reportE(args, eventName, e, 'WARNING');
      }

      await handleWith<T>(args1 => handler(args1, context)).andCleanup()(args);
    },
  };
  return builder;
}

export function handleWith<T extends ArgumentsCamelCase<CommonYargsOptions>>(
  handler: (args: T) => void | Promise<void>,
) {
  return {
    andCleanup: () => async (args: T) => {
      let success = false;
      const eventName =
        args._ && args._.length ? `cmd_${args._[0]}` : 'cmd_unknown';
      // Execute the handler and handle any errors
      try {
        await handler(args);
        success = true;
      } catch (e) {
        await reportE(args, eventName, e);
        const message = e instanceof Error ? e.message : String(e);
        getLogger().error(`\n${color.red(color.bold('Error'))}: ${message}`);
      } finally {
        await terminate(getFirestore());
      }

      if (!success) {
        process.exit(-1);
      }
    },
  };
}
