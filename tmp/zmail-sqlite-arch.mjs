import {existsSync, mkdirSync, rmSync, statSync, writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {dirname, join, resolve} from 'node:path';
import {Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {
  isMainThread,
  parentPort,
  workerData,
  Worker,
} from 'node:worker_threads';

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(SCRIPT_DIR, '..');
const ZERO_CACHE = join(ROOT, 'packages/zero-cache');
const require = createRequire(join(ZERO_CACHE, 'package.json'));
const postgres = require('postgres');
const SQLiteDatabase = require('@rocicorp/zero-sqlite3');

const {BinaryCopyParser, makeBinaryDecoder} = await import(
  pathToFileURL(join(ZERO_CACHE, 'src/db/pg-copy-binary.ts')).href
);

const DEFAULT_URI = 'postgres://postgres:pass@localhost:5547/zmail';
const DEFAULT_OUT_DIR = join(ROOT, 'tmp/results/zmail-sqlite-arch');
const VERSION = '01';

const TYPE = {
  bool: {typeOID: 16, dataType: 'bool', elemPgTypeClass: null},
  int4: {typeOID: 23, dataType: 'int4', elemPgTypeClass: null},
  text: {typeOID: 25, dataType: 'text', elemPgTypeClass: null},
  bpchar: {typeOID: 1042, dataType: 'bpchar', elemPgTypeClass: null},
  timestamp: {
    typeOID: 1114,
    dataType: 'timestamp',
    elemPgTypeClass: null,
  },
  jsonb: {typeOID: 3802, dataType: 'jsonb', elemPgTypeClass: null},
  enum: {
    typeOID: 25,
    dataType: 'text',
    pgTypeClass: 'e',
    elemPgTypeClass: null,
  },
};

const TABLES = [
  {
    name: 'email_content',
    columns: [
      ['email_id', TYPE.text],
      ['html', TYPE.text],
      ['text', TYPE.text],
      ['image_urls', TYPE.jsonb],
      ['headers', TYPE.jsonb],
    ],
    create: `CREATE TABLE "email_content" (
      "email_id" TEXT NOT NULL,
      "html" TEXT NOT NULL,
      "text" TEXT NOT NULL,
      "image_urls" TEXT NOT NULL,
      "headers" TEXT NOT NULL,
      "_0_version" TEXT DEFAULT '${VERSION}'
    );`,
  },
  {
    name: 'email_metadata',
    columns: [
      ['id', TYPE.text],
      ['mailbox', TYPE.enum],
      ['category', TYPE.enum],
      ['thread_id', TYPE.text],
      ['message_id', TYPE.text],
      ['from_email', TYPE.text],
      ['to_email', TYPE.text],
      ['subject', TYPE.text],
      ['preview', TYPE.text],
      ['received_at', TYPE.timestamp],
      ['sent_at', TYPE.timestamp],
      ['is_read', TYPE.bool],
      ['read_at', TYPE.timestamp],
      ['is_starred', TYPE.bool],
      ['starred_at', TYPE.timestamp],
      ['body_size_bytes', TYPE.int4],
      ['content_sha256', TYPE.bpchar],
      ['created_at', TYPE.timestamp],
      ['updated_at', TYPE.timestamp],
    ],
    create: `CREATE TABLE "email_metadata" (
      "id" TEXT NOT NULL,
      "mailbox" TEXT NOT NULL,
      "category" TEXT NOT NULL,
      "thread_id" TEXT NOT NULL,
      "message_id" TEXT NOT NULL,
      "from_email" TEXT NOT NULL,
      "to_email" TEXT NOT NULL,
      "subject" TEXT NOT NULL,
      "preview" TEXT NOT NULL,
      "received_at" REAL NOT NULL,
      "sent_at" REAL NOT NULL,
      "is_read" INTEGER NOT NULL,
      "read_at" REAL,
      "is_starred" INTEGER NOT NULL,
      "starred_at" REAL,
      "body_size_bytes" INTEGER NOT NULL,
      "content_sha256" TEXT NOT NULL,
      "created_at" REAL NOT NULL,
      "updated_at" REAL NOT NULL,
      "_0_version" TEXT DEFAULT '${VERSION}'
    );`,
  },
  {
    name: 'email_address',
    columns: [
      ['email', TYPE.text],
      ['name', TYPE.text],
      ['company', TYPE.text],
      ['job_title', TYPE.text],
      ['location', TYPE.text],
      ['timezone', TYPE.text],
      ['website_url', TYPE.text],
      ['linkedin_url', TYPE.text],
      ['avatar_url', TYPE.text],
      ['created_at', TYPE.timestamp],
      ['updated_at', TYPE.timestamp],
    ],
    create: `CREATE TABLE "email_address" (
      "email" TEXT NOT NULL,
      "name" TEXT,
      "company" TEXT,
      "job_title" TEXT,
      "location" TEXT,
      "timezone" TEXT,
      "website_url" TEXT,
      "linkedin_url" TEXT,
      "avatar_url" TEXT,
      "created_at" REAL NOT NULL,
      "updated_at" REAL NOT NULL,
      "_0_version" TEXT DEFAULT '${VERSION}'
    );`,
  },
];

const TABLE_BY_NAME = new Map(TABLES.map(table => [table.name, table]));

const SQLITE_INDEXES = [
  'CREATE UNIQUE INDEX "email_content_pkey" ON "email_content" ("email_id" ASC);',
  'CREATE INDEX "email_content_text_hash_idx" ON "email_content" ("text" ASC);',
  'CREATE UNIQUE INDEX "email_metadata_pkey" ON "email_metadata" ("id" ASC);',
  'CREATE UNIQUE INDEX "email_metadata_message_id_unique" ON "email_metadata" ("message_id" ASC);',
  'CREATE INDEX "email_metadata_sender_received_idx" ON "email_metadata" ("from_email" ASC,"mailbox" ASC,"received_at" DESC,"id" DESC);',
  'CREATE INDEX "email_metadata_category_received_idx" ON "email_metadata" ("mailbox" ASC,"category" ASC,"received_at" DESC,"id" DESC);',
  'CREATE INDEX "email_metadata_read_received_idx" ON "email_metadata" ("mailbox" ASC,"is_read" ASC,"received_at" DESC,"id" DESC);',
  'CREATE INDEX "email_metadata_starred_received_idx" ON "email_metadata" ("mailbox" ASC,"is_starred" ASC,"received_at" DESC,"id" DESC);',
  'CREATE INDEX "email_metadata_to_email_received_idx" ON "email_metadata" ("to_email" ASC,"received_at" DESC,"id" DESC);',
  'CREATE INDEX "email_metadata_received_idx" ON "email_metadata" ("received_at" DESC,"id" DESC);',
  'CREATE INDEX "email_metadata_thread_received_idx" ON "email_metadata" ("thread_id" ASC,"received_at" DESC,"id" DESC);',
  'CREATE INDEX "email_metadata_mailbox_received_idx" ON "email_metadata" ("mailbox" ASC,"received_at" DESC,"id" DESC);',
  'CREATE INDEX "email_metadata_mailbox_updated_idx" ON "email_metadata" ("mailbox" ASC,"updated_at" DESC,"id" DESC);',
  'CREATE UNIQUE INDEX "email_address_pkey" ON "email_address" ("email" ASC);',
  'CREATE INDEX "email_address_name_idx" ON "email_address" ("name" ASC);',
  'CREATE INDEX "email_address_company_idx" ON "email_address" ("company" ASC);',
];

function nowMs() {
  return performance.now();
}

function elapsedMs(start) {
  return performance.now() - start;
}

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith('--')) {
      args._ ??= [];
      args._.push(arg);
      continue;
    }
    const eq = arg.indexOf('=');
    if (eq === -1) {
      const key = arg.slice(2);
      const next = argv[i + 1];
      if (next !== undefined && !next.startsWith('--')) {
        args[key] = next;
        i++;
      } else {
        args[key] = true;
      }
    } else {
      args[arg.slice(2, eq)] = arg.slice(eq + 1);
    }
  }
  return args;
}

function intArg(args, name, fallback) {
  const value = args[name];
  if (value === undefined || value === true || value === '') {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer, got ${value}`);
  }
  return parsed;
}

function strArg(args, name, fallback) {
  const value = args[name];
  if (value === undefined || value === true || value === '') {
    return fallback;
  }
  return String(value);
}

function boolArg(args, name, fallback = false) {
  const value = args[name];
  if (value === undefined || value === '') {
    return fallback;
  }
  return value === true || value === '1' || value === 'true';
}

function ensureDir(path) {
  mkdirSync(path, {recursive: true});
}

function removeSqlite(path) {
  for (const suffix of ['', '-wal', '-shm']) {
    rmSync(`${path}${suffix}`, {force: true});
  }
}

function configureUnsafeImport(db) {
  db.unsafeMode(true);
  db.pragma('locking_mode = EXCLUSIVE');
  db.pragma('foreign_keys = OFF');
  db.pragma('journal_mode = OFF');
  db.pragma('synchronous = OFF');
}

function configureConcurrentImport(db, journalMode) {
  db.unsafeMode(false);
  db.pragma(`journal_mode = ${journalMode}`);
  db.pragma('synchronous = OFF');
  db.pragma('busy_timeout = 5000');
  db.pragma('wal_autocheckpoint = 0');
}

function createTables(db) {
  for (const table of TABLES) {
    db.exec(table.create);
  }
}

function createIndexes(db, mode) {
  if (mode === 'none') {
    return {indexes: 0, ms: 0, details: []};
  }
  const details = [];
  const start = nowMs();
  for (const sql of SQLITE_INDEXES) {
    const indexStart = nowMs();
    db.exec(sql);
    details.push({sql, ms: elapsedMs(indexStart)});
  }
  return {indexes: SQLITE_INDEXES.length, ms: elapsedMs(start), details};
}

function allColumns(table) {
  return [...table.columns.map(([name]) => name), '_0_version'];
}

function qid(name) {
  return `"${name.replaceAll('"', '""')}"`;
}

function sqlString(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function insertStatements(db, table, batchSize, includeVersion = false) {
  const columns = includeVersion
    ? allColumns(table)
    : table.columns.map(([name]) => name);
  const columnList = columns.map(qid).join(',');
  const rowSql = `(${'?,'.repeat(columns.length - 1)}?)`;
  const insertSql = `INSERT INTO ${qid(table.name)} (${columnList}) VALUES ${rowSql}`;
  return {
    columns,
    single: db.prepare(insertSql),
    batch: db.prepare(insertSql + `,${rowSql}`.repeat(batchSize - 1)),
  };
}

function makeDecoders(table) {
  return table.columns.map(([, spec]) => makeBinaryDecoder(spec));
}

function selectForTask(task) {
  const table = TABLE_BY_NAME.get(task.table);
  if (!table) {
    throw new Error(`unknown table ${task.table}`);
  }
  const columns = table.columns.map(([name]) => qid(name)).join(',');
  let select = `SELECT ${columns} FROM public.${qid(task.table)}`;
  if (task.ctid) {
    select += ` WHERE ctid >= '(${task.ctid.startBlock},0)'::tid AND ctid < '(${task.ctid.endBlock},0)'::tid`;
  }
  if (task.limit) {
    select += ` LIMIT ${task.limit}`;
  }
  return select;
}

function countRows(dbPath) {
  const db = new SQLiteDatabase(dbPath, {readonly: true});
  try {
    const counts = {};
    let total = 0;
    for (const table of TABLES) {
      const row = db
        .prepare(`SELECT COUNT(*) AS n FROM ${qid(table.name)}`)
        .get();
      counts[table.name] = row.n;
      total += row.n;
    }
    return {total, counts};
  } finally {
    db.close();
  }
}

function dbBytes(path) {
  let total = 0;
  for (const suffix of ['', '-wal', '-shm']) {
    const file = `${path}${suffix}`;
    if (existsSync(file)) {
      total += statSync(file).size;
    }
  }
  return total;
}

async function copyTaskToDb({
  sql,
  pgTx,
  db,
  task,
  batchSize,
  commitRows = 0,
  txMode = 'outer',
}) {
  const table = TABLE_BY_NAME.get(task.table);
  const decoders = makeDecoders(table);
  const parser = new BinaryCopyParser();
  const valuesPerRow = table.columns.length;
  const insert = insertStatements(db, table, batchSize);
  const profile = {
    task,
    rows: 0,
    inputBytes: 0,
    fields: 0,
    parseDecodeMs: 0,
    insertMs: 0,
    txMs: 0,
    copyMs: 0,
    commits: 0,
    retries: 0,
    retryErrors: {},
  };
  const select = selectForTask(task);
  let row = new Array(valuesPerRow);
  let col = 0;
  let batch = [];
  let txRows = [];

  function runInsertRows(rows) {
    const start = nowMs();
    let offset = 0;
    while (rows.length - offset >= batchSize) {
      const flat = [];
      for (let i = 0; i < batchSize; i++) {
        flat.push(...rows[offset + i]);
      }
      insert.batch.run(flat);
      offset += batchSize;
    }
    for (; offset < rows.length; offset++) {
      insert.single.run(rows[offset]);
    }
    profile.insertMs += elapsedMs(start);
  }

  function directCommitRows(rows) {
    if (rows.length === 0) {
      return;
    }
    for (;;) {
      const txStart = nowMs();
      let begun = false;
      try {
        db.prepare(`BEGIN ${txMode}`).run();
        begun = true;
        runInsertRows(rows);
        db.prepare('COMMIT').run();
        profile.txMs += elapsedMs(txStart);
        profile.commits++;
        return;
      } catch (e) {
        profile.retries++;
        const code = e?.code ?? e?.name ?? 'unknown';
        profile.retryErrors[code] = (profile.retryErrors[code] ?? 0) + 1;
        if (begun) {
          try {
            db.prepare('ROLLBACK').run();
          } catch {}
        }
      }
    }
  }

  function flushBatch() {
    if (batch.length === 0) {
      return;
    }
    if (commitRows > 0) {
      txRows.push(...batch);
      batch = [];
      while (txRows.length >= commitRows) {
        const rows = txRows.splice(0, commitRows);
        directCommitRows(rows);
      }
    } else {
      runInsertRows(batch);
      batch = [];
    }
  }

  const copyStart = nowMs();
  await pipeline(
    await pgTx
      .unsafe(`COPY (${select}) TO STDOUT WITH (FORMAT binary)`)
      .readable(),
    new Writable({
      highWaterMark: 8 * 1024 * 1024,
      write(chunk, _encoding, callback) {
        try {
          profile.inputBytes += chunk.length;
          const parseStart = nowMs();
          for (const fieldBuf of parser.parse(chunk)) {
            profile.fields++;
            row[col] = fieldBuf === null ? null : decoders[col](fieldBuf);
            if (++col === valuesPerRow) {
              col = 0;
              batch.push(row);
              row = new Array(valuesPerRow);
              profile.rows++;
              if (batch.length >= batchSize) {
                profile.parseDecodeMs += elapsedMs(parseStart);
                flushBatch();
              }
            }
          }
          profile.parseDecodeMs += elapsedMs(parseStart);
          callback();
        } catch (e) {
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },
      final(callback) {
        try {
          flushBatch();
          if (commitRows > 0) {
            directCommitRows(txRows);
            txRows = [];
          }
          callback();
        } catch (e) {
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },
    }),
  );
  profile.copyMs = elapsedMs(copyStart);
  return profile;
}

async function acquireSnapshot(uri) {
  const holder = postgres(uri, {max: 1});
  let release;
  const releasePromise = new Promise(resolve => {
    release = resolve;
  });
  let readyResolve;
  let readyReject;
  const ready = new Promise((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });
  const held = holder
    .begin('isolation level repeatable read read only', async tx => {
      await tx`SET LOCAL idle_in_transaction_session_timeout = 0`.execute();
      const [row] = await tx`SELECT pg_export_snapshot() AS snapshot`;
      readyResolve(row.snapshot);
      await releasePromise;
    })
    .catch(e => readyReject(e));
  const snapshot = await ready;
  return {
    snapshot,
    release: async () => {
      release();
      await held.catch(() => {});
      await holder.end();
    },
  };
}

async function withSnapshotTx(uri, snapshot, fn) {
  const sql = postgres(uri, {max: 1, max_lifetime: 120 * 60});
  try {
    return await sql.begin(
      'isolation level repeatable read read only',
      async tx => {
        await tx.unsafe(`SET TRANSACTION SNAPSHOT ${sqlString(snapshot)}`);
        return fn(sql, tx);
      },
    );
  } finally {
    await sql.end();
  }
}

async function heapPages(uri) {
  const sql = postgres(uri, {max: 1});
  try {
    const [row] = await sql`
      SELECT CEIL(pg_relation_size('public.email_content'::regclass)::float8 /
                  current_setting('block_size')::int)::int AS pages`;
    return row.pages;
  } finally {
    await sql.end();
  }
}

async function expectedCounts(uri) {
  const sql = postgres(uri, {max: 1});
  try {
    const rows = await sql`
      SELECT 'email_content' AS table, count(*)::int AS rows FROM public.email_content
      UNION ALL
      SELECT 'email_metadata' AS table, count(*)::int AS rows FROM public.email_metadata
      UNION ALL
      SELECT 'email_address' AS table, count(*)::int AS rows FROM public.email_address`;
    const counts = Object.fromEntries(rows.map(row => [row.table, row.rows]));
    return {
      counts,
      total: Object.values(counts).reduce((sum, rows) => sum + rows, 0),
    };
  } finally {
    await sql.end();
  }
}

async function buildTasks(uri, contentChunks, limit) {
  const pages = await heapPages(uri);
  const tasks = [];
  for (let i = 0; i < contentChunks; i++) {
    const startBlock = Math.floor((pages * i) / contentChunks);
    const endBlock =
      i === contentChunks - 1
        ? pages
        : Math.floor((pages * (i + 1)) / contentChunks);
    tasks.push({
      table: 'email_content',
      ctid: {index: i + 1, total: contentChunks, startBlock, endBlock},
      limit,
    });
  }
  tasks.push({table: 'email_metadata', limit});
  tasks.push({table: 'email_address', limit});
  return tasks;
}

function assignTasks(tasks, workers) {
  const buckets = Array.from({length: workers}, () => []);
  for (const [i, task] of tasks.entries()) {
    buckets[i % workers].push(task);
  }
  return buckets;
}

async function runStageWorker(data) {
  const dbPath = data.dbPath;
  removeSqlite(dbPath);
  const db = new SQLiteDatabase(dbPath);
  configureUnsafeImport(db);
  createTables(db);
  const result = {
    worker: data.worker,
    dbPath,
    tasks: [],
    rows: 0,
    inputBytes: 0,
    importMs: 0,
    dbBytes: 0,
  };
  const start = nowMs();
  try {
    db.prepare('BEGIN EXCLUSIVE').run();
    await withSnapshotTx(data.uri, data.snapshot, async (_sql, tx) => {
      for (const task of data.tasks) {
        const profile = await copyTaskToDb({
          sql: _sql,
          pgTx: tx,
          db,
          task,
          batchSize: data.batchSize,
        });
        result.tasks.push(profile);
        result.rows += profile.rows;
        result.inputBytes += profile.inputBytes;
      }
    });
    db.prepare('COMMIT').run();
  } catch (e) {
    try {
      db.prepare('ROLLBACK').run();
    } catch {}
    throw e;
  } finally {
    db.close();
  }
  result.importMs = elapsedMs(start);
  result.dbBytes = dbBytes(dbPath);
  return result;
}

async function runDirectWorker(data) {
  const db = new SQLiteDatabase(data.dbPath);
  configureConcurrentImport(db, data.journalMode);
  const result = {
    worker: data.worker,
    dbPath: data.dbPath,
    tasks: [],
    rows: 0,
    inputBytes: 0,
    importMs: 0,
    retries: 0,
    retryErrors: {},
  };
  const start = nowMs();
  try {
    await withSnapshotTx(data.uri, data.snapshot, async (_sql, tx) => {
      for (const task of data.tasks) {
        const profile = await copyTaskToDb({
          sql: _sql,
          pgTx: tx,
          db,
          task,
          batchSize: data.batchSize,
          commitRows: data.commitRows,
          txMode: data.txMode,
        });
        result.tasks.push(profile);
        result.rows += profile.rows;
        result.inputBytes += profile.inputBytes;
        result.retries += profile.retries;
        for (const [code, count] of Object.entries(profile.retryErrors)) {
          result.retryErrors[code] = (result.retryErrors[code] ?? 0) + count;
        }
      }
    });
  } finally {
    db.close();
  }
  result.importMs = elapsedMs(start);
  return result;
}

function startWorker(kind, data) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL(import.meta.url), {
      workerData: {kind, data},
    });
    worker.on('message', msg => {
      if (msg.error) {
        reject(Object.assign(new Error(msg.error.message), msg.error));
      } else {
        resolve(msg.result);
      }
    });
    worker.on('error', reject);
    worker.on('exit', code => {
      if (code !== 0) {
        reject(new Error(`worker exited ${code}`));
      }
    });
  });
}

async function mergeStageDbs({stagePaths, finalPath, indexMode}) {
  removeSqlite(finalPath);
  const db = new SQLiteDatabase(finalPath);
  configureUnsafeImport(db);
  createTables(db);
  const result = {mergeMs: 0, perAttach: [], index: undefined, dbBytes: 0};
  const start = nowMs();
  try {
    for (const [stageIndex, path] of stagePaths.entries()) {
      const schema = `s${stageIndex}`;
      db.exec(`ATTACH DATABASE ${sqlString(path)} AS ${qid(schema)}`);
    }
    db.prepare('BEGIN EXCLUSIVE').run();
    for (const [stageIndex, path] of stagePaths.entries()) {
      const schema = `s${stageIndex}`;
      const attachResult = {stageIndex, path, tables: []};
      for (const table of TABLES) {
        const columns = allColumns(table).map(qid).join(',');
        const tableStart = nowMs();
        db.exec(
          `INSERT INTO main.${qid(table.name)} (${columns}) SELECT ${columns} FROM ${qid(schema)}.${qid(table.name)}`,
        );
        attachResult.tables.push({
          table: table.name,
          ms: elapsedMs(tableStart),
        });
      }
      result.perAttach.push(attachResult);
    }
    db.prepare('COMMIT').run();
    result.mergeMs = elapsedMs(start);
    for (const [stageIndex] of stagePaths.entries()) {
      db.exec(`DETACH DATABASE ${qid(`s${stageIndex}`)}`);
    }
    result.index = createIndexes(db, indexMode);
  } catch (e) {
    try {
      db.prepare('ROLLBACK').run();
    } catch {}
    throw e;
  } finally {
    db.close();
  }
  result.dbBytes = dbBytes(finalPath);
  return result;
}

async function runStaged(args) {
  const uri = strArg(args, 'uri', DEFAULT_URI);
  const workers = intArg(args, 'workers', 2);
  const contentChunks = intArg(args, 'content-chunks', workers);
  const batchSize = intArg(args, 'batch-size', 50);
  const quickRows = intArg(args, 'quick-rows', 0);
  const indexMode = strArg(args, 'index-mode', 'none');
  const outDir = resolve(strArg(args, 'out-dir', DEFAULT_OUT_DIR));
  const name = strArg(
    args,
    'name',
    `staged_w${workers}_c${contentChunks}_${Date.now()}`,
  );
  const runDir = join(outDir, name);
  ensureDir(runDir);
  const expected = await expectedCounts(uri);
  const tasks = await buildTasks(uri, contentChunks, quickRows || undefined);
  const buckets = assignTasks(tasks, workers);
  const stagePaths = buckets.map((_bucket, i) => join(runDir, `stage-${i}.db`));
  const finalPath = join(runDir, 'final.db');
  const snapshot = await acquireSnapshot(uri);
  const start = nowMs();
  let importResults;
  try {
    importResults = await Promise.all(
      buckets.map((bucket, i) =>
        startWorker('stage', {
          worker: i,
          uri,
          snapshot: snapshot.snapshot,
          tasks: bucket,
          dbPath: stagePaths[i],
          batchSize,
        }),
      ),
    );
  } finally {
    await snapshot.release();
  }
  const importMs = elapsedMs(start);
  const merge = await mergeStageDbs({stagePaths, finalPath, indexMode});
  const counts = countRows(finalPath);
  const totalMs = elapsedMs(start);
  const result = {
    kind: 'staged',
    config: {
      uri,
      workers,
      contentChunks,
      batchSize,
      indexMode,
      quickRows,
      runDir,
    },
    expected,
    tasks,
    importMs,
    mergeMs: merge.mergeMs,
    indexMs: merge.index?.ms ?? 0,
    totalMs,
    importResults,
    merge,
    counts,
    ok: quickRows ? undefined : counts.total === expected.total,
  };
  writeResult(runDir, result);
  return result;
}

async function runDirect(args) {
  const uri = strArg(args, 'uri', DEFAULT_URI);
  const workers = intArg(args, 'workers', 2);
  const contentChunks = intArg(args, 'content-chunks', workers);
  const batchSize = intArg(args, 'batch-size', 50);
  const commitRows = intArg(args, 'commit-rows', 250);
  const quickRows = intArg(args, 'quick-rows', 0);
  const journalMode = strArg(args, 'journal-mode', 'wal2');
  const txMode = strArg(args, 'tx-mode', 'CONCURRENT');
  const indexMode = strArg(args, 'index-mode', 'none');
  const outDir = resolve(strArg(args, 'out-dir', DEFAULT_OUT_DIR));
  const name = strArg(
    args,
    'name',
    `direct_w${workers}_c${contentChunks}_${txMode}_${Date.now()}`,
  );
  const runDir = join(outDir, name);
  ensureDir(runDir);
  const finalPath = join(runDir, 'final.db');
  removeSqlite(finalPath);
  const db = new SQLiteDatabase(finalPath);
  configureConcurrentImport(db, journalMode);
  createTables(db);
  db.close();
  const expected = await expectedCounts(uri);
  const tasks = await buildTasks(uri, contentChunks, quickRows || undefined);
  const buckets = assignTasks(tasks, workers);
  const snapshot = await acquireSnapshot(uri);
  const start = nowMs();
  let importResults;
  try {
    importResults = await Promise.all(
      buckets.map((bucket, i) =>
        startWorker('direct', {
          worker: i,
          uri,
          snapshot: snapshot.snapshot,
          tasks: bucket,
          dbPath: finalPath,
          batchSize,
          commitRows,
          journalMode,
          txMode,
        }),
      ),
    );
  } finally {
    await snapshot.release();
  }
  const importMs = elapsedMs(start);
  const indexDb = new SQLiteDatabase(finalPath);
  configureConcurrentImport(indexDb, journalMode);
  const index = createIndexes(indexDb, indexMode);
  indexDb.close();
  const counts = countRows(finalPath);
  const totalMs = elapsedMs(start);
  const result = {
    kind: 'direct',
    config: {
      uri,
      workers,
      contentChunks,
      batchSize,
      commitRows,
      journalMode,
      txMode,
      indexMode,
      quickRows,
      runDir,
    },
    expected,
    tasks,
    importMs,
    indexMs: index.ms,
    totalMs,
    importResults,
    index,
    counts,
    dbBytes: dbBytes(finalPath),
    ok: quickRows ? undefined : counts.total === expected.total,
  };
  writeResult(runDir, result);
  return result;
}

async function runSyncContent(args) {
  const uri = strArg(args, 'uri', DEFAULT_URI);
  const batchSize = intArg(args, 'batch-size', 50);
  const quickRows = intArg(args, 'quick-rows', 0);
  const outDir = resolve(strArg(args, 'out-dir', DEFAULT_OUT_DIR));
  const name = strArg(args, 'name', `sync_content_${Date.now()}`);
  const runDir = join(outDir, name);
  ensureDir(runDir);
  const dbPath = join(runDir, 'content.db');
  removeSqlite(dbPath);
  const db = new SQLiteDatabase(dbPath);
  configureUnsafeImport(db);
  db.exec(TABLE_BY_NAME.get('email_content').create);
  const snapshot = await acquireSnapshot(uri);
  const start = nowMs();
  let profile;
  try {
    db.prepare('BEGIN EXCLUSIVE').run();
    await withSnapshotTx(uri, snapshot.snapshot, async (_sql, tx) => {
      profile = await copyTaskToDb({
        sql: _sql,
        pgTx: tx,
        db,
        task: {table: 'email_content', limit: quickRows || undefined},
        batchSize,
      });
    });
    db.prepare('COMMIT').run();
  } catch (e) {
    try {
      db.prepare('ROLLBACK').run();
    } catch {}
    throw e;
  } finally {
    await snapshot.release();
    db.close();
  }
  const verify = new SQLiteDatabase(dbPath, {readonly: true});
  const {n} = verify.prepare('SELECT COUNT(*) AS n FROM email_content').get();
  verify.close();
  const result = {
    kind: 'sync-content',
    config: {uri, batchSize, quickRows, runDir},
    totalMs: elapsedMs(start),
    profile,
    rows: n,
    dbBytes: dbBytes(dbPath),
    ok: quickRows ? undefined : n === 200000,
  };
  writeResult(runDir, result);
  return result;
}

async function runQueuedContent(args) {
  const uri = strArg(args, 'uri', DEFAULT_URI);
  const batchSize = intArg(args, 'batch-size', 50);
  const quickRows = intArg(args, 'quick-rows', 0);
  const queueBytes = intArg(args, 'queue-mb', 256) * 1024 * 1024;
  const outDir = resolve(strArg(args, 'out-dir', DEFAULT_OUT_DIR));
  const name = strArg(args, 'name', `queued_content_${Date.now()}`);
  const runDir = join(outDir, name);
  ensureDir(runDir);
  const dbPath = join(runDir, 'content.db');
  removeSqlite(dbPath);

  let outstanding = 0;
  let notifyAck;
  let doneResolve;
  let doneReject;
  let readyResolve;
  let readyReject;
  const done = new Promise((resolve, reject) => {
    doneResolve = resolve;
    doneReject = reject;
  });
  const ready = new Promise((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });
  const worker = new Worker(new URL(import.meta.url), {
    workerData: {
      kind: 'queued-writer',
      data: {dbPath, batchSize},
    },
  });
  worker.on('message', msg => {
    if (msg.ready) {
      readyResolve();
      return;
    }
    if (msg.ack) {
      outstanding -= msg.bytes;
      if (notifyAck) {
        notifyAck();
        notifyAck = undefined;
      }
      return;
    }
    if (msg.result) {
      doneResolve(msg.result);
      return;
    }
    if (msg.error) {
      doneReject(Object.assign(new Error(msg.error.message), msg.error));
    }
  });
  worker.on('error', err => {
    readyReject(err);
    doneReject(err);
  });
  worker.on('exit', code => {
    if (code !== 0) {
      const err = new Error(`queued writer exited ${code}`);
      readyReject(err);
      doneReject(err);
    }
  });

  async function waitForQueue() {
    while (outstanding > queueBytes) {
      await new Promise(resolve => {
        notifyAck = resolve;
      });
    }
  }

  async function waitForDrain() {
    while (outstanding > 0) {
      await new Promise(resolve => {
        notifyAck = resolve;
      });
    }
  }

  const snapshot = await acquireSnapshot(uri);
  const start = nowMs();
  let inputBytes = 0;
  let chunks = 0;
  try {
    await ready;
    await withSnapshotTx(uri, snapshot.snapshot, async (_sql, tx) => {
      const select = selectForTask({
        table: 'email_content',
        limit: quickRows || undefined,
      });
      const readable = await tx
        .unsafe(`COPY (${select}) TO STDOUT WITH (FORMAT binary)`)
        .readable();
      for await (const chunk of readable) {
        inputBytes += chunk.length;
        chunks++;
        outstanding += chunk.length;
        const transferred = chunk.buffer.slice(
          chunk.byteOffset,
          chunk.byteOffset + chunk.byteLength,
        );
        worker.postMessage(
          {
            chunk: transferred,
            byteLength: transferred.byteLength,
          },
          [transferred],
        );
        await waitForQueue();
      }
    });
    await waitForDrain();
    worker.postMessage({final: true});
  } finally {
    await snapshot.release();
  }
  const writerResult = await done;
  const result = {
    kind: 'queued-content',
    config: {uri, batchSize, queueBytes, quickRows, runDir},
    totalMs: elapsedMs(start),
    inputBytes,
    chunks,
    writerResult,
    dbBytes: dbBytes(dbPath),
    ok: quickRows ? undefined : writerResult.rows === 200000,
  };
  writeResult(runDir, result);
  await worker.terminate();
  return result;
}

async function runQueuedWriter(data) {
  const table = TABLE_BY_NAME.get('email_content');
  const decoders = makeDecoders(table);
  const db = new SQLiteDatabase(data.dbPath);
  configureUnsafeImport(db);
  db.exec(table.create);
  const insert = insertStatements(db, table, data.batchSize);
  const parser = new BinaryCopyParser();
  const valuesPerRow = table.columns.length;
  let col = 0;
  let row = new Array(valuesPerRow);
  let batch = [];
  const result = {
    rows: 0,
    fields: 0,
    inputBytes: 0,
    chunks: 0,
    parseMs: 0,
    insertMs: 0,
  };

  function flush() {
    if (batch.length === 0) {
      return;
    }
    const start = nowMs();
    let offset = 0;
    while (batch.length - offset >= data.batchSize) {
      const flat = [];
      for (let i = 0; i < data.batchSize; i++) {
        flat.push(...batch[offset + i]);
      }
      insert.batch.run(flat);
      offset += data.batchSize;
    }
    for (; offset < batch.length; offset++) {
      insert.single.run(batch[offset]);
    }
    batch = [];
    result.insertMs += elapsedMs(start);
  }

  db.prepare('BEGIN EXCLUSIVE').run();
  return await new Promise((resolve, reject) => {
    parentPort.postMessage({ready: true});
    parentPort.on('message', msg => {
      try {
        if (msg.final) {
          flush();
          db.prepare('COMMIT').run();
          db.close();
          resolve(result);
          return;
        }
        const bytes = msg.byteLength;
        const chunk = Buffer.from(msg.chunk);
        result.inputBytes += bytes;
        result.chunks++;
        const parseStart = nowMs();
        for (const fieldBuf of parser.parse(chunk)) {
          result.fields++;
          row[col] = fieldBuf === null ? null : decoders[col](fieldBuf);
          if (++col === valuesPerRow) {
            col = 0;
            batch.push(row);
            row = new Array(valuesPerRow);
            result.rows++;
            if (batch.length >= data.batchSize) {
              result.parseMs += elapsedMs(parseStart);
              flush();
            }
          }
        }
        result.parseMs += elapsedMs(parseStart);
        parentPort.postMessage({ack: true, bytes});
      } catch (e) {
        try {
          db.prepare('ROLLBACK').run();
          db.close();
        } catch {}
        reject(e);
      }
    });
  });
}

async function runMergeOnly(args) {
  const indexMode = strArg(args, 'index-mode', 'none');
  const runDir = resolve(strArg(args, 'run-dir', ''));
  if (!runDir) {
    throw new Error('--run-dir is required');
  }
  const stagePaths = [];
  for (let i = 0; ; i++) {
    const path = join(runDir, `stage-${i}.db`);
    if (!existsSync(path)) {
      break;
    }
    stagePaths.push(path);
  }
  if (stagePaths.length === 0) {
    throw new Error(`no stage dbs found in ${runDir}`);
  }
  const finalPath = join(runDir, `merge-only-${indexMode}.db`);
  const start = nowMs();
  const merge = await mergeStageDbs({stagePaths, finalPath, indexMode});
  const counts = countRows(finalPath);
  const result = {
    kind: 'merge-only',
    config: {runDir, indexMode},
    totalMs: elapsedMs(start),
    merge,
    counts,
  };
  writeResult(runDir, result, `merge-only-${indexMode}.json`);
  return result;
}

function writeResult(runDir, result, file = 'result.json') {
  ensureDir(runDir);
  const jsonPath = join(runDir, file);
  writeFileSync(jsonPath, JSON.stringify(result, null, 2));
  writeFileSync(join(runDir, 'summary.md'), summarize(result));
}

function ms(ms) {
  return `${(ms / 1000).toFixed(3)}s`;
}

function summarize(result) {
  const lines = [];
  lines.push(`# ${result.kind}`);
  lines.push('');
  lines.push(`ok: ${result.ok ?? 'n/a'}`);
  lines.push(`total: ${ms(result.totalMs ?? 0)}`);
  if (result.importMs !== undefined)
    lines.push(`import: ${ms(result.importMs)}`);
  if (result.mergeMs !== undefined) lines.push(`merge: ${ms(result.mergeMs)}`);
  if (result.indexMs !== undefined) lines.push(`index: ${ms(result.indexMs)}`);
  if (result.counts) {
    lines.push(`rows: ${result.counts.total}`);
    for (const [table, rows] of Object.entries(result.counts.counts)) {
      lines.push(`${table}: ${rows}`);
    }
  }
  if (result.importResults) {
    lines.push('');
    lines.push('| worker | rows | import | insert | retries | db MB |');
    lines.push('|---:|---:|---:|---:|---:|---:|');
    for (const worker of result.importResults) {
      const insertMs = worker.tasks.reduce(
        (sum, task) => sum + (task.insertMs ?? 0),
        0,
      );
      lines.push(
        `| ${worker.worker} | ${worker.rows} | ${ms(worker.importMs)} | ${ms(insertMs)} | ${worker.retries ?? 0} | ${((worker.dbBytes ?? 0) / 1024 / 1024).toFixed(1)} |`,
      );
    }
  }
  return `${lines.join('\n')}\n`;
}

async function main() {
  const args = parseArgs(process.argv);
  const mode = strArg(args, 'mode', 'staged');
  let result;
  if (mode === 'staged') {
    result = await runStaged(args);
  } else if (mode === 'direct') {
    result = await runDirect(args);
  } else if (mode === 'sync-content') {
    result = await runSyncContent(args);
  } else if (mode === 'queued-content') {
    result = await runQueuedContent(args);
  } else if (mode === 'merge-only') {
    result = await runMergeOnly(args);
  } else {
    throw new Error(`unknown mode ${mode}`);
  }
  console.log(
    JSON.stringify(
      {
        kind: result.kind,
        ok: result.ok,
        totalMs: result.totalMs,
        importMs: result.importMs,
        mergeMs: result.mergeMs,
        indexMs: result.indexMs,
        counts: result.counts,
        config: result.config,
      },
      null,
      2,
    ),
  );
}

if (isMainThread) {
  await main();
} else {
  try {
    const {kind, data} = workerData;
    const result =
      kind === 'stage'
        ? await runStageWorker(data)
        : kind === 'queued-writer'
          ? await runQueuedWriter(data)
          : await runDirectWorker(data);
    parentPort.postMessage({result});
  } catch (e) {
    parentPort.postMessage({
      error: {
        name: e?.name ?? 'Error',
        message: e?.message ?? String(e),
        stack: e?.stack,
        code: e?.code,
      },
    });
  }
}
