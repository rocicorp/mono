import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {must} from '../../../shared/src/must.ts';
import {getZeroConfig} from '../config/zero-config.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {
  buildLitestreamVfsReplicaURL,
  VfsBackupWatermarkReader,
} from '../services/litestream/vfs-watermark-reader.ts';
import {VfsBackupWatermarkWorkerService} from '../services/litestream/vfs-watermark-worker.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {createLogContext} from './logging.ts';

let lc = new LogContext('info', {}, consoleLogSink);

export default async function runWorker(
  parent: Worker | null,
  env: NodeJS.ProcessEnv,
  ...argv: string[]
): Promise<void> {
  const config = getZeroConfig({env, argv});
  lc = createLogContext(config, 'backup-watermark-reader');
  const {litestream} = config;

  const replicaURL = buildLitestreamVfsReplicaURL({
    backupURL: must(
      litestream.backupURL,
      'Missing --litestream-backup-url for backup watermark reader',
    ),
    endpoint: litestream.endpoint,
    region: litestream.region,
  });

  const createReader = () =>
    new VfsBackupWatermarkReader(lc, {
      replicaURL,
      extensionPath: litestream.vfsExtensionPath,
      logLevel: litestream.logLevel,
      logFile: litestream.vfsLogFile,
    });

  await runUntilKilled(
    lc,
    parent ?? process,
    new VfsBackupWatermarkWorkerService(
      lc,
      parent,
      createReader,
      litestream.vfsProbeIntervalMs,
    ),
  );
}

if (!singleProcessMode()) {
  void exitAfter(lc, () =>
    runWorker(parentWorker, process.env, ...process.argv.slice(2)),
  );
}
