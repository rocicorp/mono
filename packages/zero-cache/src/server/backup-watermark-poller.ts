import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {must} from '../../../shared/src/must.ts';
import {getZeroConfig} from '../config/zero-config.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {VfsBackupWatermarkPoller} from '../services/litestream/vfs-watermark-poller.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {createLogContext} from './logging.ts';

let lc = new LogContext('info', {}, consoleLogSink);

export default async function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...argv: string[]
): Promise<void> {
  const config = getZeroConfig({env, argv});
  lc = createLogContext(config, 'backup-watermark-poller');
  const {litestream, replica} = config;

  parent.send(['ready', {ready: true}]);

  await runUntilKilled(
    lc,
    parent,
    new VfsBackupWatermarkPoller(lc, parent, {
      localDbFile: replica.file,
      backupURL: must(
        litestream.backupURL,
        'Missing --litestream-backup-url for backup watermark reader',
      ),
      endpoint: litestream.endpoint,
      region: litestream.region,
      extensionPath: litestream.vfsExtensionPath,
      remotePollIntervalMs: litestream.vfsProbeIntervalMs,
    }),
  );
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(
    () => lc,
    () => runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
