/**
 * Streaming synthetic data generator for zbugs.
 *
 * Reads LLM-generated templates + source gigabugs CSVs, produces sharded CSVs
 * with ~1 billion rows of realistic synthetic data.
 *
 * Usage:
 *   npx tsx scripts/generate-synthetic.ts
 *
 * Env vars:
 *   NUM_PROJECTS          - total projects (default 100)
 *   NUM_USERS             - total users (default 100)
 *   MULTIPLICATION_FACTOR - batch count (default 345, yielding ~83M issues)
 *   SHARD_SIZE            - rows per CSV shard (default 500000)
 *   OUTPUT_DIR            - output directory (default db/seed-data/synthetic/)
 *   SEED                  - RNG seed for reproducibility (default 42)
 */

import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import {fileURLToPath} from 'url';
const __dirname = path.dirname(fileURLToPath(import.meta.url));

// --- Configuration ---
const NUM_PROJECTS = parseInt(process.env.NUM_PROJECTS ?? '100', 10);
const NUM_USERS = parseInt(process.env.NUM_USERS ?? '100', 10);
const MULTIPLICATION_FACTOR = parseInt(
  process.env.MULTIPLICATION_FACTOR ?? '345',
  10,
);
const SHARD_SIZE = parseInt(process.env.SHARD_SIZE ?? '500000', 10);
const OUTPUT_DIR = path.resolve(
  process.env.OUTPUT_DIR ?? path.join(__dirname, '../db/seed-data/synthetic/'),
);
const SEED = parseInt(process.env.SEED ?? '42', 10);
const TEMPLATES_DIR = path.join(__dirname, '../db/seed-data/templates');
const GIGABUGS_DIR = path.join(__dirname, '../db/seed-data/gigabugs');

// --- Seeded PRNG (mulberry32) ---
function mulberry32(seed: number): () => number {
  let s = seed | 0;
  return () => {
    s = (s + 0x6d2b79f5) | 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const rng = mulberry32(SEED);

function randomInt(min: number, max: number): number {
  return Math.floor(rng() * (max - min + 1)) + min;
}

function pick<T>(arr: readonly T[]): T {
  return arr[Math.floor(rng() * arr.length)];
}

// --- Template types ---
interface CategoryTemplates {
  category: string;
  projects: Array<{name: string; components: string[]}>;
  labels: string[];
  titleTemplates: string[];
  descriptionTemplates: string[];
  commentTemplates: string[];
}

// --- Slot values for template filling ---
const ACTIONS = [
  'clicking submit',
  'scrolling down',
  'loading the page',
  'refreshing',
  'navigating back',
  'opening a modal',
  'typing in search',
  'toggling dark mode',
  'uploading a file',
  'dragging an item',
  'right-clicking',
  'zooming in',
  'pressing enter',
  'copying text',
  'switching tabs',
  'logging in',
  'signing out',
  'filtering results',
  'sorting by date',
  'expanding a row',
];

const ENVIRONMENTS = [
  'production',
  'staging',
  'development',
  'CI',
  'Safari',
  'Chrome',
  'Firefox',
  'iOS Safari',
  'Android Chrome',
  'Edge',
  'Docker',
  'Kubernetes',
  'AWS',
  'GCP',
  'local',
];

const DEPENDENCIES = [
  'react',
  'typescript',
  'webpack',
  'vite',
  'express',
  'fastify',
  'postgres',
  'redis',
  'docker',
  'nginx',
  'openssl',
  'node',
  'deno',
  'lodash',
  'axios',
  'zod',
  'prisma',
  'drizzle',
  'vitest',
  'esbuild',
];

const ERRORS = [
  'TypeError',
  'ReferenceError',
  'timeout error',
  '404 Not Found',
  '500 Internal Server Error',
  'CORS error',
  'memory leak',
  'null pointer exception',
  'stack overflow',
  'connection refused',
  'ECONNRESET',
  'ENOENT',
  'permission denied',
  'deadlock detected',
  'out of memory',
];

const FEATURES = [
  'dark mode',
  'real-time sync',
  'offline support',
  'batch processing',
  'export to CSV',
  'webhook integration',
  'SSO login',
  'rate limiting',
  'caching layer',
  'audit logging',
  'search indexing',
  'pagination',
  'drag and drop',
  'keyboard shortcuts',
  'notifications',
];

const METRICS = [
  'load time',
  'memory usage',
  'CPU utilization',
  'response latency',
  'throughput',
  'error rate',
  'cache hit ratio',
  'query execution time',
  'bundle size',
  'time to interactive',
];

const USER_TYPES = [
  'admin',
  'guest',
  'new user',
  'power user',
  'API client',
  'service account',
  'moderator',
  'viewer',
  'editor',
  'owner',
];

const FINDINGS = [
  'the issue is caused by a race condition',
  'the root cause is a missing null check',
  'it appears to be a regression from the last release',
  'the problem is in the event handler lifecycle',
  'this is related to the caching strategy',
  'the issue only reproduces under high concurrency',
  'memory profiling shows a leak in the connection pool',
  'the error trace points to incorrect serialization',
  'this affects all users on the free tier',
  'the bug is in the third-party SDK',
];

const SUGGESTIONS = [
  'we should add a retry mechanism with exponential backoff',
  'switching to a connection pool would fix this',
  'we need to add proper error boundaries',
  'implementing a circuit breaker pattern would help',
  'we should migrate to the new API version',
  'adding an index on this column should resolve the perf issue',
  'we could use a write-ahead log for reliability',
  'refactoring the state management would prevent this',
  'adding input validation at the boundary would fix it',
  'we should implement proper cleanup in the teardown',
];

const WORKAROUNDS = [
  'restarting the service temporarily resolves the issue',
  'clearing the cache allows normal operation',
  'users can work around this by refreshing the page',
  'reducing the batch size prevents the timeout',
  'disabling the feature flag avoids the crash',
  'rolling back to the previous version fixes it',
  'increasing the timeout to 30s prevents failures',
  'using the alternative endpoint works for now',
  'manually triggering a sync resolves the stale data',
  'downgrading the dependency to v2.x is a temporary fix',
];

function fillTemplate(template: string, components: readonly string[]): string {
  return template.replace(/\{\{(\w+)\}\}/g, (_match, slot: string) => {
    switch (slot) {
      case 'component':
        return pick(components);
      case 'action':
        return pick(ACTIONS);
      case 'environment':
        return pick(ENVIRONMENTS);
      case 'dependency':
        return pick(DEPENDENCIES);
      case 'version':
        return `${randomInt(1, 9)}.${randomInt(0, 30)}.${randomInt(0, 20)}`;
      case 'error':
        return pick(ERRORS);
      case 'feature':
        return pick(FEATURES);
      case 'metric':
        return pick(METRICS);
      case 'user_type':
        return pick(USER_TYPES);
      case 'steps':
        return '1. Open the app 2. Navigate to the affected area 3. Perform the action 4. Observe the error';
      case 'expected':
        return 'The operation should complete successfully without errors';
      case 'actual':
        return 'An error occurs and the operation fails';
      case 'finding':
        return pick(FINDINGS);
      case 'suggestion':
        return pick(SUGGESTIONS);
      case 'workaround':
        return pick(WORKAROUNDS);
      case 'user':
        return `user_${randomInt(1, NUM_USERS)}`;
      default:
        return slot;
    }
  });
}

// --- Faker-lite name generation ---
const FIRST_NAMES = [
  'Alice',
  'Bob',
  'Carol',
  'Dave',
  'Eve',
  'Frank',
  'Grace',
  'Henry',
  'Iris',
  'Jack',
  'Kate',
  'Leo',
  'Maya',
  'Noah',
  'Olivia',
  'Pat',
  'Quinn',
  'Rose',
  'Sam',
  'Tara',
  'Uma',
  'Victor',
  'Wendy',
  'Xander',
  'Yuki',
  'Zara',
  'Aiden',
  'Bella',
  'Caleb',
  'Diana',
  'Ethan',
  'Fiona',
  'George',
  'Hannah',
  'Ivan',
  'Julia',
  'Kyle',
  'Luna',
  'Max',
  'Nora',
  'Oscar',
  'Priya',
  'Raj',
  'Sofia',
  'Tyler',
  'Uma',
  'Vera',
  'Will',
  'Ximena',
  'Yosef',
];

const LAST_NAMES = [
  'Smith',
  'Johnson',
  'Chen',
  'Garcia',
  'Kim',
  'Patel',
  'Brown',
  'Lee',
  'Wilson',
  'Anderson',
  'Taylor',
  'Thomas',
  'Moore',
  'Martin',
  'Jackson',
  'White',
  'Harris',
  'Clark',
  'Lewis',
  'Walker',
  'Hall',
  'Young',
  'Allen',
  'Wright',
  'King',
  'Lopez',
  'Hill',
  'Green',
  'Adams',
  'Baker',
  'Rivera',
  'Campbell',
  'Mitchell',
  'Roberts',
  'Carter',
  'Phillips',
  'Evans',
  'Turner',
  'Torres',
  'Parker',
  'Collins',
  'Edwards',
  'Stewart',
  'Flores',
  'Morris',
  'Nguyen',
  'Murphy',
  'Rivera',
  'Cook',
  'Rogers',
];

function generateLogin(first: string, last: string, idx: number): string {
  return `${first.toLowerCase()}${last.toLowerCase()}${idx}`;
}

// --- CSV helpers ---
function escapeCSV(value: string): string {
  if (
    value.includes(',') ||
    value.includes('"') ||
    value.includes('\n') ||
    value.includes('\r')
  ) {
    return '"' + value.replace(/"/g, '""') + '"';
  }
  return value;
}

// --- Sharded CSV Writer ---
class ShardedCSVWriter {
  private outputDir: string;
  private prefix: string;
  private header: string;
  private shardSize: number;
  private currentShard = 0;
  private currentRows = 0;
  private totalRows = 0;
  private stream: fs.WriteStream | null = null;

  constructor(
    outputDir: string,
    prefix: string,
    header: string,
    shardSize: number,
  ) {
    this.outputDir = outputDir;
    this.prefix = prefix;
    this.header = header;
    this.shardSize = shardSize;
  }

  private openNewShard(): void {
    if (this.stream) {
      this.stream.end();
    }
    const shardNum = String(this.currentShard).padStart(3, '0');
    const filePath = path.join(
      this.outputDir,
      `${this.prefix}_${shardNum}.csv`,
    );
    this.stream = fs.createWriteStream(filePath, {encoding: 'utf8'});
    this.stream.write(this.header + '\n');
    this.currentRows = 0;
    this.currentShard++;
  }

  async writeRow(row: string): Promise<void> {
    if (!this.stream || this.currentRows >= this.shardSize) {
      this.openNewShard();
    }
    const ok = this.stream!.write(row + '\n');
    this.currentRows++;
    this.totalRows++;
    if (!ok) {
      await new Promise<void>(resolve => this.stream!.once('drain', resolve));
    }
  }

  async close(): Promise<void> {
    if (this.stream) {
      await new Promise<void>((resolve, reject) => {
        this.stream!.end(() => resolve());
        this.stream!.on('error', reject);
      });
    }
    // oxlint-disable-next-line no-console
    console.log(
      `  ${this.prefix}: ${this.totalRows} rows in ${this.currentShard} shards`,
    );
  }
}

// --- Read source CSV rows as arrays of strings ---
async function readCSVRows(
  filePath: string,
): Promise<{header: string; rows: string[][]}> {
  const readStream = fs.createReadStream(filePath, {encoding: 'utf8'});
  const rl = readline.createInterface({
    input: readStream,
    crlfDelay: Infinity,
  });

  let header = '';
  const rows: string[][] = [];
  let isFirst = true;

  for await (const line of rl) {
    if (isFirst) {
      header = line;
      isFirst = false;
      continue;
    }
    if (line.trim()) {
      rows.push(parseCSVLine(line));
    }
  }

  return {header, rows};
}

function parseCSVLine(line: string): string[] {
  const fields: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        fields.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  fields.push(current);
  return fields;
}

// --- Load templates ---
function loadTemplates(): CategoryTemplates[] {
  const templates: CategoryTemplates[] = [];
  const files = fs
    .readdirSync(TEMPLATES_DIR)
    .filter(f => f.endsWith('.json') && f !== 'summary.json');
  for (const file of files.sort()) {
    const data = JSON.parse(
      fs.readFileSync(path.join(TEMPLATES_DIR, file), 'utf8'),
    ) as CategoryTemplates;
    templates.push(data);
  }
  if (templates.length === 0) {
    throw new Error(
      `No template files found in ${TEMPLATES_DIR}. Run generate-templates.ts first.`,
    );
  }
  return templates;
}

// --- ID generation ---
function batchHex(batch: number): string {
  return batch.toString(16).padStart(3, '0');
}

function syntheticID(originalID: string, batch: number): string {
  // Truncate original to fit: max 14 chars total = originalID prefix + '_' + 3-hex
  const prefix = originalID.slice(0, 10);
  return `${prefix}_${batchHex(batch)}`;
}

// --- Main generation ---
async function main() {
  fs.mkdirSync(OUTPUT_DIR, {recursive: true});

  // oxlint-disable-next-line no-console
  console.log('Loading templates...');
  const allTemplates = loadTemplates();

  // Build flat arrays for projects, labels
  const categories = allTemplates;
  const projectsPerCategory = Math.ceil(NUM_PROJECTS / categories.length);

  interface ProjectInfo {
    id: string;
    name: string;
    categoryIndex: number;
    components: string[];
    labelIDs: string[];
  }

  const allProjects: ProjectInfo[] = [];
  const allLabels: Array<{id: string; name: string; projectID: string}> = [];

  for (let ci = 0; ci < categories.length; ci++) {
    const cat = categories[ci];
    const projectCount = Math.min(projectsPerCategory, cat.projects.length);
    for (let pi = 0; pi < projectCount; pi++) {
      if (allProjects.length >= NUM_PROJECTS) break;
      const proj = cat.projects[pi];
      const projectID = `proj_${String(allProjects.length).padStart(3, '0')}`;
      const labelIDs: string[] = [];

      // Generate labels for this project
      for (let li = 0; li < cat.labels.length; li++) {
        const labelID = `lbl_${projectID}_${String(li).padStart(2, '0')}`;
        allLabels.push({
          id: labelID,
          name: cat.labels[li],
          projectID,
        });
        labelIDs.push(labelID);
      }

      allProjects.push({
        id: projectID,
        name: proj.name,
        categoryIndex: ci,
        components: proj.components,
        labelIDs,
      });
    }
    if (allProjects.length >= NUM_PROJECTS) break;
  }

  // Generate users
  // oxlint-disable-next-line no-console
  console.log(`Generating ${NUM_USERS} users...`);
  const users: Array<{
    id: string;
    login: string;
    name: string;
    avatar: string;
    role: string;
    githubID: number;
    email: string;
  }> = [];
  for (let i = 0; i < NUM_USERS; i++) {
    const first = FIRST_NAMES[i % FIRST_NAMES.length];
    const last = LAST_NAMES[i % LAST_NAMES.length];
    const login = generateLogin(first, last, i);
    users.push({
      id: `usr_${String(i).padStart(4, '0')}`,
      login,
      name: `${first} ${last}`,
      avatar: `https://robohash.org/${login}`,
      role: i < 10 ? 'crew' : 'user',
      githubID: 10000 + i,
      email: `${login}@synthetic.dev`,
    });
  }

  // Write users CSV
  const userWriter = new ShardedCSVWriter(
    OUTPUT_DIR,
    'user',
    'id,login,name,avatar,role,githubID,email',
    SHARD_SIZE,
  );
  for (const u of users) {
    await userWriter.writeRow(
      [
        u.id,
        u.login,
        escapeCSV(u.name),
        u.avatar,
        u.role,
        String(u.githubID),
        u.email,
      ].join(','),
    );
  }
  await userWriter.close();

  // Write projects CSV
  // oxlint-disable-next-line no-console
  console.log(`Generating ${allProjects.length} projects...`);
  const projectWriter = new ShardedCSVWriter(
    OUTPUT_DIR,
    'project',
    'id,name,lowerCaseName',
    SHARD_SIZE,
  );
  for (const p of allProjects) {
    await projectWriter.writeRow(
      [p.id, escapeCSV(p.name), escapeCSV(p.name.toLowerCase())].join(','),
    );
  }
  await projectWriter.close();

  // Write labels CSV
  // oxlint-disable-next-line no-console
  console.log(`Generating ${allLabels.length} labels...`);
  const labelWriter = new ShardedCSVWriter(
    OUTPUT_DIR,
    'label',
    'id,name,projectID',
    SHARD_SIZE,
  );
  for (const l of allLabels) {
    await labelWriter.writeRow(
      [l.id, escapeCSV(l.name), l.projectID].join(','),
    );
  }
  await labelWriter.close();

  // --- Load source CSVs ---
  // oxlint-disable-next-line no-console
  console.log('Loading source issue CSVs...');
  const sourceIssueFiles = fs
    .readdirSync(GIGABUGS_DIR)
    .filter(f => f.startsWith('issue_') && f.endsWith('.csv'))
    .sort();
  const allSourceIssues: string[][] = [];
  for (const file of sourceIssueFiles) {
    const {rows} = await readCSVRows(path.join(GIGABUGS_DIR, file));
    allSourceIssues.push(...rows);
  }
  // oxlint-disable-next-line no-console
  console.log(`  Loaded ${allSourceIssues.length} source issue rows`);

  // oxlint-disable-next-line no-console
  console.log('Loading source comment CSVs...');
  const sourceCommentFiles = fs
    .readdirSync(GIGABUGS_DIR)
    .filter(f => f.startsWith('comment_') && f.endsWith('.csv'))
    .sort();
  const allSourceComments: string[][] = [];
  for (const file of sourceCommentFiles) {
    const {rows} = await readCSVRows(path.join(GIGABUGS_DIR, file));
    allSourceComments.push(...rows);
  }
  // oxlint-disable-next-line no-console
  console.log(`  Loaded ${allSourceComments.length} source comment rows`);

  // oxlint-disable-next-line no-console
  console.log('Loading source issueLabel CSV...');
  const {rows: sourceIssueLabels} = await readCSVRows(
    path.join(GIGABUGS_DIR, 'issueLabel_000.csv'),
  );
  // oxlint-disable-next-line no-console
  console.log(`  Loaded ${sourceIssueLabels.length} source issueLabel rows`);

  // Source issue header: id,title,open,modified,created,creatorID,assigneeID,description,visibility,projectID
  // Source comment header: id,issueID,created,body,creatorID
  // Source issueLabel header: labelID,issueID,projectID

  // Load source labels to build a position map
  const {rows: sourceLabelsRows} = await readCSVRows(
    path.join(GIGABUGS_DIR, 'label_000.csv'),
  );
  const sourceLabelIDs = sourceLabelsRows.map(r => r[0]); // ordered label IDs

  // Build source labelID -> index map
  const sourceLabelIndexMap = new Map<string, number>();
  for (let i = 0; i < sourceLabelIDs.length; i++) {
    sourceLabelIndexMap.set(sourceLabelIDs[i], i);
  }

  // --- Generate issues ---
  const totalIssues = allSourceIssues.length * MULTIPLICATION_FACTOR;
  // oxlint-disable-next-line no-console
  console.log(
    `\nGenerating ~${totalIssues.toLocaleString()} issues (${MULTIPLICATION_FACTOR} batches x ${allSourceIssues.length.toLocaleString()} source rows)...`,
  );

  const issueWriter = new ShardedCSVWriter(
    OUTPUT_DIR,
    'issue',
    'id,title,open,modified,created,creatorID,assigneeID,description,visibility,projectID',
    SHARD_SIZE,
  );

  // Track issue -> project mapping for comments and issueLabels
  // We don't store all in memory; we use deterministic project assignment
  const TIME_BASE = 1577836800000; // 2020-01-01
  const TIME_SPREAD = 3600000; // 1 hour between batches

  for (let batch = 0; batch < MULTIPLICATION_FACTOR; batch++) {
    if (batch % 50 === 0) {
      // oxlint-disable-next-line no-console
      console.log(
        `  Issues batch ${batch}/${MULTIPLICATION_FACTOR} (${((batch / MULTIPLICATION_FACTOR) * 100).toFixed(1)}%)`,
      );
    }

    for (let si = 0; si < allSourceIssues.length; si++) {
      const sourceRow = allSourceIssues[si];
      const originalID = sourceRow[0];
      const newID = syntheticID(originalID, batch);

      // Deterministic project assignment
      const projectIdx = (si + batch) % allProjects.length;
      const project = allProjects[projectIdx];
      const cat = categories[project.categoryIndex];

      // Generate title from template
      const titleTemplate =
        cat.titleTemplates[(si + batch) % cat.titleTemplates.length];
      let title = fillTemplate(titleTemplate, project.components);
      if (title.length > 128) title = title.slice(0, 125) + '...';

      // Generate description from template
      const descTemplate =
        cat.descriptionTemplates[
          (si + batch * 3) % cat.descriptionTemplates.length
        ];
      let description = fillTemplate(descTemplate, project.components);
      if (description.length > 10240)
        description = description.slice(0, 10237) + '...';

      // Timestamps
      const created =
        TIME_BASE + batch * TIME_SPREAD + randomInt(0, TIME_SPREAD);
      const modified = created + randomInt(0, 86400000 * 30); // up to 30 days later

      // Open status - vary per batch (60-80% open)
      const openRatio = 0.6 + (batch % 20) * 0.01;
      const open = rng() < openRatio;

      const creatorID =
        users[Math.abs(hashStr(originalID + batch)) % users.length].id;
      const assigneeID =
        rng() < 0.7
          ? users[Math.abs(hashStr(originalID + batch + 'a')) % users.length].id
          : '';

      const visibility = rng() < 0.9 ? 'public' : 'internal';

      await issueWriter.writeRow(
        [
          newID,
          escapeCSV(title),
          String(open),
          String(modified),
          String(created),
          creatorID,
          assigneeID,
          escapeCSV(description),
          visibility,
          project.id,
        ].join(','),
      );
    }
  }
  await issueWriter.close();

  // --- Generate comments ---
  const totalComments = allSourceComments.length * MULTIPLICATION_FACTOR;
  // oxlint-disable-next-line no-console
  console.log(
    `\nGenerating ~${totalComments.toLocaleString()} comments (${MULTIPLICATION_FACTOR} batches x ${allSourceComments.length.toLocaleString()} source rows)...`,
  );

  const commentWriter = new ShardedCSVWriter(
    OUTPUT_DIR,
    'comment',
    'id,issueID,created,body,creatorID',
    SHARD_SIZE,
  );

  // Build source issueID -> index map for project lookup
  const sourceIssueIDMap = new Map<string, number>();
  for (let i = 0; i < allSourceIssues.length; i++) {
    sourceIssueIDMap.set(allSourceIssues[i][0], i);
  }

  for (let batch = 0; batch < MULTIPLICATION_FACTOR; batch++) {
    if (batch % 50 === 0) {
      // oxlint-disable-next-line no-console
      console.log(
        `  Comments batch ${batch}/${MULTIPLICATION_FACTOR} (${((batch / MULTIPLICATION_FACTOR) * 100).toFixed(1)}%)`,
      );
    }

    for (let ci = 0; ci < allSourceComments.length; ci++) {
      const sourceRow = allSourceComments[ci];
      const originalCommentID = sourceRow[0];
      const originalIssueID = sourceRow[1];

      const newCommentID = syntheticID(originalCommentID, batch);
      const newIssueID = syntheticID(originalIssueID, batch);

      // Find project for this comment's issue
      const sourceIssueIdx = sourceIssueIDMap.get(originalIssueID);
      let projectIdx = 0;
      if (sourceIssueIdx !== undefined) {
        projectIdx = (sourceIssueIdx + batch) % allProjects.length;
      }
      const project = allProjects[projectIdx];
      const cat = categories[project.categoryIndex];

      // Generate body from template
      const bodyTemplate =
        cat.commentTemplates[(ci + batch * 7) % cat.commentTemplates.length];
      let body = fillTemplate(bodyTemplate, project.components);
      if (body.length > 65536) body = body.slice(0, 65533) + '...';

      const creatorID =
        users[Math.abs(hashStr(originalCommentID + batch)) % users.length].id;

      // Comment created after issue created
      const issueCreated =
        TIME_BASE + batch * TIME_SPREAD + randomInt(0, TIME_SPREAD);
      const created = issueCreated + randomInt(3600000, 86400000 * 7); // 1hr to 7 days after issue

      await commentWriter.writeRow(
        [
          newCommentID,
          newIssueID,
          String(created),
          escapeCSV(body),
          creatorID,
        ].join(','),
      );
    }
  }
  await commentWriter.close();

  // --- Generate issueLabels ---
  const totalIssueLabels = sourceIssueLabels.length * MULTIPLICATION_FACTOR;
  // oxlint-disable-next-line no-console
  console.log(
    `\nGenerating ~${totalIssueLabels.toLocaleString()} issueLabels (${MULTIPLICATION_FACTOR} batches x ${sourceIssueLabels.length.toLocaleString()} source rows)...`,
  );

  const issueLabelWriter = new ShardedCSVWriter(
    OUTPUT_DIR,
    'issueLabel',
    'labelID,issueID,projectID',
    SHARD_SIZE,
  );

  for (let batch = 0; batch < MULTIPLICATION_FACTOR; batch++) {
    if (batch % 50 === 0) {
      // oxlint-disable-next-line no-console
      console.log(
        `  IssueLabels batch ${batch}/${MULTIPLICATION_FACTOR} (${((batch / MULTIPLICATION_FACTOR) * 100).toFixed(1)}%)`,
      );
    }

    for (let li = 0; li < sourceIssueLabels.length; li++) {
      const sourceRow = sourceIssueLabels[li];
      const sourceLabelID = sourceRow[0];
      const sourceIssueID = sourceRow[1];

      const newIssueID = syntheticID(sourceIssueID, batch);

      // Find project for this issue
      const sourceIssueIdx = sourceIssueIDMap.get(sourceIssueID);
      let projectIdx = 0;
      if (sourceIssueIdx !== undefined) {
        projectIdx = (sourceIssueIdx + batch) % allProjects.length;
      }
      const project = allProjects[projectIdx];

      // Map source label position to this project's label set
      const sourceLabelIdx = sourceLabelIndexMap.get(sourceLabelID) ?? 0;
      const newLabelIdx = sourceLabelIdx % project.labelIDs.length;
      const newLabelID = project.labelIDs[newLabelIdx];

      await issueLabelWriter.writeRow(
        [newLabelID, newIssueID, project.id].join(','),
      );
    }
  }
  await issueLabelWriter.close();

  // --- Summary ---
  // oxlint-disable-next-line no-console
  console.log('\n=== Generation Complete ===');
  // oxlint-disable-next-line no-console
  console.log(`Output directory: ${OUTPUT_DIR}`);
  // oxlint-disable-next-line no-console
  console.log(`Users: ${users.length}`);
  // oxlint-disable-next-line no-console
  console.log(`Projects: ${allProjects.length}`);
  // oxlint-disable-next-line no-console
  console.log(`Labels: ${allLabels.length}`);
  // oxlint-disable-next-line no-console
  console.log(`Issues: ~${totalIssues.toLocaleString()}`);
  // oxlint-disable-next-line no-console
  console.log(`Comments: ~${totalComments.toLocaleString()}`);
  // oxlint-disable-next-line no-console
  console.log(`IssueLabels: ~${totalIssueLabels.toLocaleString()}`);
  // oxlint-disable-next-line no-console
  console.log(
    `Total: ~${(users.length + allProjects.length + allLabels.length + totalIssues + totalComments + totalIssueLabels).toLocaleString()} rows`,
  );
}

// Simple string hash for deterministic selection
function hashStr(s: string): number {
  let hash = 0;
  for (let i = 0; i < s.length; i++) {
    const char = s.charCodeAt(i);
    hash = ((hash << 5) - hash + char) | 0;
  }
  return hash;
}

main().catch(err => {
  console.error('Synthetic data generation failed:', err);
  process.exit(1);
});
