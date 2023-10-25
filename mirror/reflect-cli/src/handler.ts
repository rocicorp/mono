import {getFirestore, terminate} from 'firebase/firestore';
import color from 'picocolors';
import type {ArgumentsCamelCase} from 'yargs';
import {reportE} from './error.js';
import {sendAnalyticsEvent} from './metrics/send-ga-event.js';
import type {CommonYargsOptions} from './yarg-types.js';

// Wraps a command handler with cleanup code (e.g. terminating any Firestore client)
// to ensure that the process exits after the handler completes.

export function handleWith<T extends ArgumentsCamelCase<CommonYargsOptions>>(
  handler: (args: T) => Promise<void>,
) {
  return {
    andCleanup: () => async (args: T) => {
      let success = false;
      const eventName =
        args._ && args._.length ? `cmd_${args._[0]}` : 'cmd_unknown';
      try {
        await handler(args);
        success = true;
      } catch (e) {
        await reportE(args, eventName, e, 'ERROR');
        const message = e instanceof Error ? e.message : String(e);
        console.error(`\n${color.red(color.bold('Error'))}: ${message}`);
      } finally {
        await terminate(getFirestore());
      }

      // It is tempting to send analytics in parallel with running
      // the handler, but that appears to cause problems for some commands
      // for reasons unknown.
      // https://github.com/rocicorp/mono/issues/1078
      try {
        await sendAnalyticsEvent(eventName);
      } catch (e) {
        await reportE(args, eventName, e, 'WARNING');
      }

      if (!success) {
        process.exit(-1);
      }
    },
  };
}
