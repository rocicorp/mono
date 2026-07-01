import {spawn} from 'node:child_process';
import {existsSync, mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, '..');
const root =
  process.env.ZMAIL_ARTIFACT_ROOT ??
  join(repoRoot, 'tmp/results/zmail-initial-sync');
const workdir = process.env.ZMAIL_WORKDIR ?? repoRoot;
const pgURI =
  process.env.ZMAIL_PG_URI ?? 'postgres://postgres:pass@localhost:5547/zmail';
const nodeUsing = process.env.ZMAIL_NODE_USING;
const startupTimeoutMs = Number(
  process.env.ZMAIL_STARTUP_TIMEOUT_MS ?? '120000',
);
const heartbeatMs = Number(process.env.ZMAIL_HEARTBEAT_MS ?? '60000');
const expectedRows = '403091';
const payloadMB = '27318.261686590635';
const gitSHA = process.env.ZMAIL_GIT_SHA ?? 'unknown';

const allExperiments = [
  {
    name: 'smoke_sample_no_indexes',
    suffix: 'smk',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_MODE: 'none',
      ZMAIL_SAMPLE_RATE: '0.01',
      ZMAIL_MAX_ROWS_PER_TABLE: '1000',
    },
    expectedRows: undefined,
    smoke: true,
  },
  {
    name: 'baseline_workers5_no_chunk',
    suffix: 'b5nc',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_MODE: 'all',
    },
  },
  {
    name: 'no_chunk_workers5_index_threads8',
    suffix: 'nct8',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
    },
  },
  {
    name: 'ctid_768m_workers5_no_index_threads',
    suffix: 'c768',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '805306368',
      ZMAIL_INDEX_MODE: 'all',
    },
  },
  {
    name: 'ctid_768m_workers5_index_threads8',
    suffix: 'c768t8',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '805306368',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
    },
  },
  {
    name: 'ctid_512m_workers5_index_threads8',
    suffix: 'c512t8',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '536870912',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
    },
  },
  {
    name: 'ctid_1g_workers5_index_threads8',
    suffix: 'c1gt8',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '1073741824',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
    },
  },
  {
    name: 'ctid_2g_workers5_index_threads8',
    suffix: 'c2gt8',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '2147483648',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
    },
  },
  {
    name: 'ctid_768m_workers8_index_threads8',
    suffix: 'c768w8',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '8',
      ZMAIL_CHUNK_TARGET_BYTES: '805306368',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
    },
  },
  {
    name: 'ctid_768m_workers10_index_threads8',
    suffix: 'c768w10',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '10',
      ZMAIL_CHUNK_TARGET_BYTES: '805306368',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
    },
  },
  {
    name: 'ctid_768m_workers5_no_indexes',
    suffix: 'c768ni',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '805306368',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
    },
  },
  {
    name: 'ctid_768m_workers5_required_indexes',
    suffix: 'c768ri',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '805306368',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'required',
    },
  },
  {
    name: 'ctid_768m_workers5_dedupe_indexes',
    suffix: 'c768di',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '805306368',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'dedupe',
    },
  },
  {
    name: 'textcopy_ctid_768m_workers5_index_threads8',
    suffix: 'tx768t8',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '805306368',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
      ZMAIL_TEXT_COPY: '1',
    },
  },
  {
    name: 'textcopy_ctid_768m_workers5_no_indexes',
    suffix: 'tx768ni',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '805306368',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
      ZMAIL_TEXT_COPY: '1',
    },
  },
  {
    name: 'no_chunk_workers5_no_indexes',
    suffix: 'ncni',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
    },
  },
  {
    name: 'no_chunk_workers5_dedupe_indexes',
    suffix: 'ncdi',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'dedupe',
    },
  },
  {
    name: 'ctid_512m_workers5_no_indexes',
    suffix: 'c512ni',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '536870912',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
    },
  },
  {
    name: 'ctid_1g_workers5_no_indexes',
    suffix: 'c1gni',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '1073741824',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
    },
  },
  {
    name: 'ctid_2g_workers5_no_indexes',
    suffix: 'c2gni',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '2147483648',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
    },
  },
  {
    name: 'ctid_768m_workers10_no_indexes',
    suffix: 'c768w10ni',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '10',
      ZMAIL_CHUNK_TARGET_BYTES: '805306368',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
    },
  },
  {
    name: 'ctid_768m_workers10_dedupe_indexes',
    suffix: 'c768w10di',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '10',
      ZMAIL_CHUNK_TARGET_BYTES: '805306368',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'dedupe',
    },
  },
  {
    name: 'repeat_no_chunk_workers5_index_threads8',
    suffix: 'nct8r2',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
    },
  },
  {
    name: 'repeat_ctid_768m_workers5_index_threads8',
    suffix: 'c768t8r2',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '805306368',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
    },
  },
  {
    name: 'repeat_no_chunk_workers5_dedupe_indexes',
    suffix: 'ncdir2',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'dedupe',
    },
  },
  {
    name: 'repeat_ctid_768m_workers5_dedupe_indexes',
    suffix: 'c768dir2',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '805306368',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'dedupe',
    },
  },
  {
    name: 'repeat_ctid_2g_workers5_no_indexes',
    suffix: 'c2gnir2',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '2147483648',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
    },
  },
  {
    name: 'tune_default_c2g_noidx',
    suffix: 'tdc2gni',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '2147483648',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
    },
  },
  {
    name: 'tune_batch100_c2g_noidx',
    suffix: 'tb100ni',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '2147483648',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
      ZMAIL_INSERT_BATCH_SIZE: '100',
    },
  },
  {
    name: 'tune_batch250_c2g_noidx',
    suffix: 'tb250ni',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '2147483648',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
      ZMAIL_INSERT_BATCH_SIZE: '250',
    },
  },
  {
    name: 'tune_batch500_c2g_noidx',
    suffix: 'tb500ni',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '2147483648',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
      ZMAIL_INSERT_BATCH_SIZE: '500',
    },
  },
  {
    name: 'tune_batch250_buf32_c2g_noidx',
    suffix: 'tb250b32',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '2147483648',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
      ZMAIL_INSERT_BATCH_SIZE: '250',
      ZMAIL_BUFFERED_SIZE_THRESHOLD_BYTES: '33554432',
    },
  },
  {
    name: 'tune_batch250_buf64_c2g_noidx',
    suffix: 'tb250b64',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '2147483648',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
      ZMAIL_INSERT_BATCH_SIZE: '250',
      ZMAIL_BUFFERED_SIZE_THRESHOLD_BYTES: '67108864',
    },
  },
  {
    name: 'tune_batch250_buf32_pragmas_c2g_noidx',
    suffix: 'tb250bp',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '2147483648',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'none',
      ZMAIL_INSERT_BATCH_SIZE: '250',
      ZMAIL_BUFFERED_SIZE_THRESHOLD_BYTES: '33554432',
      ZMAIL_SQLITE_CACHE_SIZE: '-1048576',
      ZMAIL_SQLITE_MMAP_SIZE: '1073741824',
      ZMAIL_SQLITE_TEMP_STORE: 'memory',
    },
  },
  {
    name: 'tune_batch250_buf32_c2g_dedupe',
    suffix: 'tb250bd',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '2147483648',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'dedupe',
      ZMAIL_INSERT_BATCH_SIZE: '250',
      ZMAIL_BUFFERED_SIZE_THRESHOLD_BYTES: '33554432',
    },
  },
  {
    name: 'tune_default_c2g_dedupe',
    suffix: 'tdc2gdi',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '2147483648',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'dedupe',
    },
  },
  {
    name: 'page_default_noidx',
    suffix: 'pgd',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_MODE: 'none',
    },
  },
  {
    name: 'page_16k_noidx',
    suffix: 'p16',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_MODE: 'none',
      ZMAIL_SQLITE_PAGE_SIZE: '16384',
    },
  },
  {
    name: 'page_32k_noidx',
    suffix: 'p32',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_MODE: 'none',
      ZMAIL_SQLITE_PAGE_SIZE: '32768',
    },
  },
  {
    name: 'page_64k_noidx',
    suffix: 'p64',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_MODE: 'none',
      ZMAIL_SQLITE_PAGE_SIZE: '65536',
    },
  },
  {
    name: 'skip_hash_allidx',
    suffix: 'ska',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
      ZMAIL_INDEX_EXCLUDE_REGEX: 'email_content_text_hash_idx',
    },
  },
  {
    name: 'page64_allidx',
    suffix: 'p6a',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
      ZMAIL_SQLITE_PAGE_SIZE: '65536',
    },
  },
  {
    name: 'page64_skip_hash',
    suffix: 'p6s',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
      ZMAIL_INDEX_EXCLUDE_REGEX: 'email_content_text_hash_idx',
      ZMAIL_SQLITE_PAGE_SIZE: '65536',
    },
  },
  {
    name: 'preindex_allidx',
    suffix: 'pia',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
      ZMAIL_INDEX_TIMING: 'before-copy',
    },
  },
  {
    name: 'preindex_skip_hash',
    suffix: 'pih',
    env: {
      ZMAIL_TABLE_COPY_WORKERS: '5',
      ZMAIL_CHUNK_TARGET_BYTES: '0',
      ZMAIL_INDEX_THREADS: '8',
      ZMAIL_INDEX_MODE: 'all',
      ZMAIL_INDEX_TIMING: 'before-copy',
      ZMAIL_INDEX_EXCLUDE_REGEX: 'email_content_text_hash_idx',
    },
  },
];

const selected = new Set(process.argv.slice(2));
const experiments = selected.size
  ? allExperiments.filter(e => selected.has(e.name))
  : allExperiments;

if (selected.size && experiments.length !== selected.size) {
  const known = new Set(allExperiments.map(e => e.name));
  const missing = [...selected].filter(name => !known.has(name));
  throw new Error(`Unknown experiment(s): ${missing.join(', ')}`);
}

mkdirSync(root, {recursive: true});

function nowISO() {
  return new Date().toISOString();
}

function runProcess(command, args, options) {
  return new Promise(resolve => {
    const child = spawn(command, args, options);
    let stdout = '';
    let stderr = '';
    let settled = false;
    let killedForStartupTimeout = false;
    let startupTimer;
    let heartbeatTimer;

    function cleanupTimers() {
      if (startupTimer) clearTimeout(startupTimer);
      if (heartbeatTimer) clearInterval(heartbeatTimer);
    }

    if (options.startupTimeoutMs && options.hasStarted) {
      startupTimer = setTimeout(() => {
        if (options.hasStarted()) return;
        killedForStartupTimeout = true;
        options.onStartupTimeout?.(child);
        child.kill('SIGTERM');
      }, options.startupTimeoutMs);
    }

    if (options.heartbeatMs && options.onHeartbeat) {
      heartbeatTimer = setInterval(
        () => options.onHeartbeat(child),
        options.heartbeatMs,
      );
    }

    child.stdout?.on('data', data => {
      const text = data.toString();
      stdout += text;
      options.onOutput?.(text, 'stdout');
    });
    child.stderr?.on('data', data => {
      const text = data.toString();
      stderr += text;
      options.onOutput?.(text, 'stderr');
    });
    child.on('error', error => {
      if (settled) return;
      settled = true;
      cleanupTimers();
      resolve({
        code: -1,
        signal: null,
        stdout,
        stderr,
        error: String(error),
        killedForStartupTimeout,
      });
    });
    child.on('close', (code, signal) => {
      if (settled) return;
      settled = true;
      cleanupTimers();
      resolve({code, signal, stdout, stderr, killedForStartupTimeout});
    });
  });
}

function maybeFnm(command, args) {
  if (nodeUsing === undefined || nodeUsing === '') {
    return {command, args, display: [command, ...args]};
  }
  const wrappedArgs = ['exec', `--using=${nodeUsing}`, command, ...args];
  return {command: 'fnm', args: wrappedArgs, display: ['fnm', ...wrappedArgs]};
}

async function cleanupCheck() {
  const sql = `SELECT 'schemas' AS kind, nspname AS name, '' AS detail FROM pg_namespace WHERE nspname LIKE 'zmail_bench%' UNION ALL SELECT 'publications', pubname, '' FROM pg_publication WHERE pubname LIKE 'zmail_bench%' UNION ALL SELECT 'slots', slot_name, active::text FROM pg_replication_slots WHERE slot_name LIKE 'zmail_bench%' ORDER BY kind, name;`;
  const result = await runProcess(
    'psql',
    [
      pgURI,
      '-v',
      'ON_ERROR_STOP=1',
      '-P',
      'pager=off',
      '-F',
      '\t',
      '-Atc',
      sql,
    ],
    {cwd: workdir, env: process.env},
  );
  const rows = result.stdout
    .trim()
    .split('\n')
    .filter(Boolean)
    .map(line => {
      const [kind, name, detail] = line.split('\t');
      return {kind, name, detail};
    });
  return {
    exitCode: result.code,
    rows,
    clean: result.code === 0 && rows.length === 0,
    stderr: result.stderr.trim(),
  };
}

function parseLog(log) {
  const summary = {
    benchmark: undefined,
    copyPlan: undefined,
    chunkPlan: [],
    downloadStates: [],
    copyTasks: [],
    indexes: [],
    indexTotalMs: undefined,
    synced: undefined,
    repRows: undefined,
    repElapsedMs: undefined,
    warnings: [],
  };

  const lines = log.split(/\r?\n/);
  for (const line of lines) {
    if (!line) continue;
    if (line.includes('Computed initial download state for ')) {
      const match = line.match(
        /Computed initial download state for ([^\s]+) \(([\d.]+) ms\)/,
      );
      if (match)
        summary.downloadStates.push({
          table: match[1],
          elapsedMs: Number(match[2]),
          line,
        });
    }
    {
      const match = line.match(
        /chunking table ([^:]+): estimatedCopyBytes=([\d.]+) totalBytes=([^\s]+) heapPages=([\d.]+) chunks=(\d+)/,
      );
      if (match) {
        summary.chunkPlan.push({
          table: match[1],
          estimatedCopyBytes: Number(match[2]),
          totalBytes: match[3] === 'unknown' ? undefined : Number(match[3]),
          heapPages: Number(match[4]),
          chunks: Number(match[5]),
        });
      }
    }
    {
      const match = line.match(
        /initial-sync copy plan: tables=(\d+) tasks=(\d+) workers=(\d+) chunkTargetBytes=(\d+) maxChunksPerTable=(\d+)/,
      );
      if (match) {
        summary.copyPlan = {
          tables: Number(match[1]),
          tasks: Number(match[2]),
          workers: Number(match[3]),
          chunkTargetBytes: Number(match[4]),
          maxChunksPerTable: Number(match[5]),
        };
      }
    }
    {
      const match = line.match(
        /Finished copying ([\d,]+) rows into (.+?) \(flush: ([\d.]+) ms\) \(total: ([\d.]+) ms\)/,
      );
      if (match) {
        const copyName = match[2].trim();
        const chunk = copyName.match(
          /^(.*?) chunk (\d+)\/(\d+) blocks=(\d+)-(\d+)$/,
        );
        summary.copyTasks.push({
          rows: Number(match[1].replaceAll(',', '')),
          copyName,
          table: chunk ? chunk[1] : copyName,
          chunkIndex: chunk ? Number(chunk[2]) : undefined,
          chunkTotal: chunk ? Number(chunk[3]) : undefined,
          startBlock: chunk ? Number(chunk[4]) : undefined,
          endBlock: chunk ? Number(chunk[5]) : undefined,
          flushMs: Number(match[3]),
          totalMs: Number(match[4]),
        });
      }
    }
    {
      const match = line.match(
        /Created index (\d+)\/(\d+) \(([\d.]+) ms\): (.+)$/,
      );
      if (match) {
        summary.indexes.push({
          index: Number(match[1]),
          total: Number(match[2]),
          ms: Number(match[3]),
          sql: match[4],
        });
      }
    }
    {
      const match = line.match(/Created indexes \(([\d.]+) ms\)/);
      if (match) summary.indexTotalMs = Number(match[1]);
    }
    {
      const match = line.match(
        /Synced ([\d,]+) rows of (\d+) tables .* \(flush: ([\d.]+), index: ([\d.]+), total: ([\d.]+) ms\)/,
      );
      if (match) {
        summary.synced = {
          rows: Number(match[1].replaceAll(',', '')),
          tables: Number(match[2]),
          flushMs: Number(match[3]),
          indexMs: Number(match[4]),
          totalMs: Number(match[5]),
        };
      }
    }
    {
      const match = line.match(
        /zmail initial sync rep \d+ copied ([\d,]+) rows in ([\d.]+) ms/,
      );
      if (match) {
        summary.repRows = Number(match[1].replaceAll(',', ''));
        summary.repElapsedMs = Number(match[2]);
      }
    }
    if (line.startsWith('{') && line.endsWith('}')) {
      try {
        const json = JSON.parse(line);
        if (json.benchmarks || json.name) summary.benchmark = json;
      } catch {
        // Ignore non-benchmark JSON-looking log lines.
      }
    }
    if (line.includes('WARN') || line.includes('Skipping '))
      summary.warnings.push(line);
  }

  const perTable = new Map();
  for (const task of summary.copyTasks) {
    const current = perTable.get(task.table) ?? {
      table: task.table,
      taskCount: 0,
      rows: 0,
      flushMs: 0,
      sumTaskMs: 0,
      maxTaskMs: 0,
      maxTaskName: undefined,
    };
    current.taskCount++;
    current.rows += task.rows;
    current.flushMs += task.flushMs;
    current.sumTaskMs += task.totalMs;
    if (task.totalMs > current.maxTaskMs) {
      current.maxTaskMs = task.totalMs;
      current.maxTaskName = task.copyName;
    }
    perTable.set(task.table, current);
  }

  return {
    ...summary,
    perTable: [...perTable.values()].sort((a, b) => b.sumTaskMs - a.sumTaskMs),
    slowestTasks: summary.copyTasks
      .toSorted((a, b) => b.totalMs - a.totalMs)
      .slice(0, 10),
    topIndexes: summary.indexes.toSorted((a, b) => b.ms - a.ms).slice(0, 10),
  };
}

function writeLedger(summaries) {
  const baseline = summaries.find(
    s => s.experiment === 'baseline_workers5_no_chunk',
  )?.parsed.synced?.totalMs;
  const lines = [
    '# Zmail Initial Sync Benchmark Ledger',
    '',
    `Generated: ${nowISO()}`,
    '',
  ];
  for (const summary of summaries) {
    const synced = summary.parsed.synced;
    const content = summary.parsed.perTable.find(
      t => t.table === 'email_content',
    );
    const ratio =
      baseline && synced?.totalMs ? baseline / synced.totalMs : undefined;
    lines.push(`## ${summary.experiment}`);
    lines.push('');
    lines.push(`- status: ${summary.exitCode === 0 ? 'completed' : 'failed'}`);
    lines.push(`- exit code: ${summary.exitCode}`);
    lines.push(
      `- command elapsed ms: ${summary.commandElapsedMs?.toFixed(3) ?? 'unknown'}`,
    );
    lines.push(
      `- total initial-sync ms: ${synced?.totalMs?.toFixed(3) ?? 'unknown'}`,
    );
    lines.push(`- baseline ratio: ${ratio?.toFixed(3) ?? 'n/a'}`);
    lines.push(
      `- user rows: ${summary.parsed.repRows ?? 'unknown'}${summary.expectedRows ? ` / ${summary.expectedRows}` : ''}`,
    );
    lines.push(
      `- synced rows including internal: ${synced?.rows ?? 'unknown'}`,
    );
    lines.push(`- flush ms: ${synced?.flushMs?.toFixed(3) ?? 'unknown'}`);
    lines.push(`- index ms: ${synced?.indexMs?.toFixed(3) ?? 'unknown'}`);
    lines.push(`- email_content tasks: ${content?.taskCount ?? 'unknown'}`);
    lines.push(
      `- email_content max task ms: ${content?.maxTaskMs?.toFixed(3) ?? 'unknown'}`,
    );
    lines.push(
      `- email_content sum task ms: ${content?.sumTaskMs?.toFixed(3) ?? 'unknown'}`,
    );
    lines.push(`- cleanup clean: ${summary.cleanup?.clean ?? 'unknown'}`);
    lines.push('');
  }
  writeFileSync(join(root, 'ledger.md'), `${lines.join('\n')}\n`);
}

const summaries = [];

async function preflight() {
  console.log(
    `[${nowISO()}] preflight: node${nodeUsing ? ` via fnm ${nodeUsing}` : ''}`,
  );
  const nodeCommand = maybeFnm('node', ['--version']);
  const node = await runProcess(nodeCommand.command, nodeCommand.args, {
    cwd: workdir,
    env: process.env,
  });
  if (node.code !== 0) {
    throw new Error(`node preflight failed: ${node.stderr || node.stdout}`);
  }
  console.log(`[${nowISO()}] preflight: ${node.stdout.trim()}`);

  const pg = await runProcess(
    'psql',
    [
      pgURI,
      '-v',
      'ON_ERROR_STOP=1',
      '-Atc',
      'SELECT current_database(), version();',
    ],
    {cwd: workdir, env: process.env},
  );
  if (pg.code !== 0) {
    throw new Error(`psql preflight failed: ${pg.stderr || pg.stdout}`);
  }
  console.log(`[${nowISO()}] preflight: ${pg.stdout.trim().split('\n')[0]}`);

  const cleanup = await cleanupCheck();
  if (!cleanup.clean) {
    throw new Error(
      `preflight cleanup check found leftovers or failed: ${JSON.stringify(cleanup)}`,
    );
  }
  console.log(`[${nowISO()}] preflight: no zmail_bench leftovers`);
}

await preflight();

for (const experiment of experiments) {
  const expDir = join(root, experiment.name);
  mkdirSync(expDir, {recursive: true});
  const summaryPath = join(expDir, 'summary.json');
  if (existsSync(summaryPath) && process.env.ZMAIL_FORCE !== '1') {
    const existing = JSON.parse(readFileSync(summaryPath, 'utf8'));
    summaries.push(existing);
    console.log(`[${nowISO()}] skip existing ${experiment.name}`);
    continue;
  }

  const env = {
    ...process.env,
    BENCH_OUTPUT_FORMAT: 'json',
    // The zmail benchmark uses ZMAIL_PG_URI, but vitest.config.bench.pg.ts has
    // a pg-17 global setup. Point it at the same already-running database so
    // testcontainers is not started before every zmail run.
    TEST_PG_17: pgURI,
    ZERO_BENCH_LOG: '1',
    ZMAIL_PG_URI: pgURI,
    ZMAIL_PAYLOAD_MB: payloadMB,
    ZMAIL_REPS: '1',
    ZMAIL_WARMUP_REPS: '0',
    ZMAIL_APP_SUFFIX: experiment.suffix,
    ...experiment.env,
  };
  const runExpectedRows =
    experiment.expectedRows === undefined && experiment.smoke
      ? undefined
      : expectedRows;
  if (runExpectedRows !== undefined) env.ZMAIL_EXPECTED_ROWS = runExpectedRows;
  else delete env.ZMAIL_EXPECTED_ROWS;

  const pnpmArgs = [
    '--filter',
    'zero-cache',
    'exec',
    'vitest',
    'run',
    '--config',
    'vitest.config.bench.pg.ts',
    'src/db/zmail-initial-sync.bench.pg.ts',
  ];
  const command = maybeFnm('pnpm', pnpmArgs);
  const config = {
    experiment: experiment.name,
    startedAt: nowISO(),
    root,
    workdir,
    gitSHA,
    command: command.display,
    env: Object.fromEntries(
      Object.entries(env).filter(
        ([key]) =>
          key.startsWith('ZMAIL_') ||
          key === 'TEST_PG_17' ||
          key === 'ZERO_BENCH_LOG' ||
          key === 'BENCH_OUTPUT_FORMAT',
      ),
    ),
  };
  writeFileSync(
    join(expDir, 'config.json'),
    `${JSON.stringify(config, null, 2)}\n`,
  );

  console.log(`[${nowISO()}] start ${experiment.name}`);
  const start = performance.now();
  let log = '';
  let pending = '';
  let sawBenchmarkStart = false;
  let lastMeaningfulLine = 'process spawned';
  const result = await runProcess(command.command, command.args, {
    cwd: workdir,
    env,
    startupTimeoutMs,
    heartbeatMs,
    hasStarted: () => sawBenchmarkStart,
    onStartupTimeout(child) {
      console.log(
        `[${experiment.name}] startup timeout after ${startupTimeoutMs}ms before benchmark logs; killing pid=${child.pid}. ` +
          `This usually means vitest global setup is stuck before zmail started.`,
      );
    },
    onHeartbeat(child) {
      const elapsedSeconds = ((performance.now() - start) / 1000).toFixed(0);
      console.log(
        `[${experiment.name}] heartbeat elapsed=${elapsedSeconds}s pid=${child.pid ?? 'unknown'} ` +
          `sawBenchmarkStart=${sawBenchmarkStart} last="${lastMeaningfulLine.slice(0, 160)}"`,
      );
    },
    onOutput(text) {
      log += text;
      pending += text;
      const parts = pending.split(/\r?\n/);
      pending = parts.pop() ?? '';
      for (const line of parts) {
        if (
          line.includes('zmail benchmark config') ||
          line.includes('Computed initial download state for ') ||
          line.includes('initial-sync copy plan:')
        ) {
          sawBenchmarkStart = true;
        }
        if (
          line.includes('zmail benchmark config') ||
          line.includes('Computed initial download state for ') ||
          line.includes('initial-sync copy plan:') ||
          line.includes('chunking table ') ||
          line.includes('Synced ') ||
          line.includes('zmail initial sync rep ') ||
          line.includes('Created indexes (') ||
          line.includes('FAIL') ||
          line.includes('Error')
        ) {
          lastMeaningfulLine = line;
          console.log(`[${experiment.name}] ${line}`);
        }
      }
    },
  });
  const elapsed = performance.now() - start;
  if (result.stderr) log += result.stderr;
  writeFileSync(join(expDir, 'run.log'), log);

  const cleanup = await cleanupCheck();
  writeFileSync(
    join(expDir, 'cleanup.json'),
    `${JSON.stringify(cleanup, null, 2)}\n`,
  );

  const parsed = parseLog(log);
  const summary = {
    experiment: experiment.name,
    suffix: experiment.suffix,
    expectedRows: runExpectedRows ? Number(runExpectedRows) : undefined,
    payloadMB: Number(payloadMB),
    exitCode: result.code,
    signal: result.signal,
    killedForStartupTimeout: result.killedForStartupTimeout,
    commandElapsedMs: elapsed,
    finishedAt: nowISO(),
    config: config.env,
    parsed,
    cleanup,
  };
  writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
  summaries.push(summary);
  writeLedger(summaries);

  const synced = parsed.synced;
  const content = parsed.perTable.find(t => t.table === 'email_content');
  console.log(
    `[${nowISO()}] done ${experiment.name} exit=${result.code} total=${synced?.totalMs?.toFixed(0) ?? 'unknown'}ms flush=${synced?.flushMs?.toFixed(0) ?? 'unknown'}ms index=${synced?.indexMs?.toFixed(0) ?? 'unknown'}ms contentTasks=${content?.taskCount ?? 'unknown'} cleanup=${cleanup.clean}`,
  );

  if (result.code !== 0) {
    console.log(
      `[${nowISO()}] stopping after failed experiment ${experiment.name}`,
    );
    break;
  }
}

writeLedger(summaries);
console.log(`[${nowISO()}] artifacts: ${root}`);
