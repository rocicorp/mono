/**
 * Streaming synthetic data generator for zbugs.
 *
 * Self-contained generator that produces sharded CSVs with synthetic data
 * based on configuration parameters and LLM-generated templates.
 *
 * Usage:
 *   npx tsx scripts/generate-synthetic.ts
 *
 * Env vars:
 *   NUM_ISSUES            - total issues to generate (default 1000000)
 *   NUM_PROJECTS          - total projects (default 100)
 *   NUM_USERS             - total users (default 100)
 *   COMMENTS_PER_ISSUE    - average comments per issue (default 3.0)
 *   LABELS_PER_ISSUE      - average labels per issue (default 1.5)
 *   SHARD_SIZE            - rows per CSV shard (default 500000)
 *   OUTPUT_DIR            - output directory (default db/seed-data/synthetic/)
 *   SEED                  - RNG seed for reproducibility (default 42)
 */

import * as fs from 'fs';
import * as path from 'path';
import {fileURLToPath} from 'url';
import {faker} from '@faker-js/faker';
const __dirname = path.dirname(fileURLToPath(import.meta.url));

// --- Configuration ---
const NUM_ISSUES = parseInt(process.env.NUM_ISSUES ?? '1000000', 10);
const NUM_PROJECTS = parseInt(process.env.NUM_PROJECTS ?? '100', 10);
const NUM_USERS = parseInt(process.env.NUM_USERS ?? '100', 10);
const COMMENTS_PER_ISSUE = parseFloat(process.env.COMMENTS_PER_ISSUE ?? '3.0');
const LABELS_PER_ISSUE = parseFloat(process.env.LABELS_PER_ISSUE ?? '1.5');
const SHARD_SIZE = parseInt(process.env.SHARD_SIZE ?? '500000', 10);
const OUTPUT_DIR = path.resolve(
  process.env.OUTPUT_DIR ?? path.join(__dirname, '../db/seed-data/synthetic/'),
);
const SEED = parseInt(process.env.SEED ?? '42', 10);
const TEMPLATES_DIR = path.join(__dirname, '../db/seed-data/templates');

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

// Seed faker for reproducibility (use same SEED as mulberry32)
faker.seed(SEED);

function randomInt(min: number, max: number): number {
  return Math.floor(rng() * (max - min + 1)) + min;
}

function pick<T>(arr: readonly T[]): T {
  return arr[Math.floor(rng() * arr.length)];
}

// --- Pareto/Power-law distribution helpers ---
/**
 * Assigns numItems to buckets using power-law distribution.
 * Lower alpha = more uniform, higher alpha = more skewed.
 * Returns array where result[i] = bucket index for item i.
 */
function assignWithPareto(
  numItems: number,
  numBuckets: number,
  alpha = 2,
): number[] {
  const result = new Array<number>(numItems);
  for (let i = 0; i < numItems; i++) {
    result[i] = Math.floor(Math.pow(rng(), alpha) * numBuckets);
  }
  return result;
}

/**
 * Distributes totalItems across numBuckets with Pareto distribution.
 * Returns array where result[i] = count for bucket i.
 */
function distributeWithPareto(
  numBuckets: number,
  totalItems: number,
  alpha = 2,
): number[] {
  const result = new Array<number>(numBuckets).fill(0);
  for (let i = 0; i < totalItems; i++) {
    const idx = Math.floor(Math.pow(rng(), alpha) * numBuckets);
    result[idx]++;
  }
  return result;
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
    const firstName = faker.person.firstName();
    const lastName = faker.person.lastName();
    const login = `${firstName.toLowerCase()}${lastName.toLowerCase()}${i}`;
    users.push({
      id: `usr_${String(i).padStart(4, '0')}`,
      login,
      name: `${firstName} ${lastName}`,
      avatar: `https://robohash.org/${login}`,
      role: i < 10 ? 'crew' : 'user',
      githubID: 10000 + i,
      email: faker.internet.email({firstName, lastName}),
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

  // --- Pre-compute distributions ---
  // oxlint-disable-next-line no-console
  console.log('\nPre-computing distributions...');

  // Assign issues to projects using Pareto distribution (some projects get many more issues)
  const issueProjects = assignWithPareto(NUM_ISSUES, allProjects.length, 2);

  // Distribute comments across issues using Pareto distribution (some issues get many comments)
  const totalComments = Math.floor(NUM_ISSUES * COMMENTS_PER_ISSUE);
  const commentsPerIssue = distributeWithPareto(NUM_ISSUES, totalComments, 2);

  // Calculate total issueLabels (average LABELS_PER_ISSUE per issue)
  const totalIssueLabels = Math.floor(NUM_ISSUES * LABELS_PER_ISSUE);

  // --- Generate issues ---
  // oxlint-disable-next-line no-console
  console.log(`\nGenerating ${NUM_ISSUES.toLocaleString()} issues...`);

  const issueWriter = new ShardedCSVWriter(
    OUTPUT_DIR,
    'issue',
    'id,title,open,modified,created,creatorID,assigneeID,description,visibility,projectID',
    SHARD_SIZE,
  );

  // Use faker for realistic date spread across 4 years
  const startDate = new Date('2020-01-01');
  const endDate = new Date('2024-01-01');

  for (let i = 0; i < NUM_ISSUES; i++) {
    if (i % 100000 === 0) {
      // oxlint-disable-next-line no-console
      console.log(
        `  Issues: ${i.toLocaleString()}/${NUM_ISSUES.toLocaleString()} (${((i / NUM_ISSUES) * 100).toFixed(1)}%)`,
      );
    }

    const issueID = `iss_${String(i).padStart(9, '0')}`;
    const projectIdx = issueProjects[i];
    const project = allProjects[projectIdx];
    const cat = categories[project.categoryIndex];

    // Generate title from template
    const titleTemplate = cat.titleTemplates[i % cat.titleTemplates.length];
    let title = fillTemplate(titleTemplate, project.components);
    if (title.length > 128) title = title.slice(0, 125) + '...';

    // Generate description from template
    const descTemplate =
      cat.descriptionTemplates[(i * 3) % cat.descriptionTemplates.length];
    let description = fillTemplate(descTemplate, project.components);
    if (description.length > 10240)
      description = description.slice(0, 10237) + '...';

    // Timestamps - use faker for realistic date spread
    const created = faker.date
      .between({from: startDate, to: endDate})
      .getTime();
    const modified = faker.date.between({from: created, to: endDate}).getTime();

    // Open status (70% open)
    const open = rng() < 0.7;

    const creatorID = users[Math.floor(rng() * users.length)].id;
    const assigneeID =
      rng() < 0.7 ? users[Math.floor(rng() * users.length)].id : '';

    const visibility = rng() < 0.9 ? 'public' : 'internal';

    await issueWriter.writeRow(
      [
        issueID,
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
  await issueWriter.close();

  // --- Generate comments ---
  // oxlint-disable-next-line no-console
  console.log(
    `\nGenerating ${totalComments.toLocaleString()} comments (avg ${COMMENTS_PER_ISSUE} per issue)...`,
  );

  const commentWriter = new ShardedCSVWriter(
    OUTPUT_DIR,
    'comment',
    'id,issueID,created,body,creatorID',
    SHARD_SIZE,
  );

  let commentIdx = 0;
  for (let issueIdx = 0; issueIdx < NUM_ISSUES; issueIdx++) {
    if (issueIdx % 100000 === 0) {
      // oxlint-disable-next-line no-console
      console.log(
        `  Comments: processing issue ${issueIdx.toLocaleString()}/${NUM_ISSUES.toLocaleString()} (${((issueIdx / NUM_ISSUES) * 100).toFixed(1)}%)`,
      );
    }

    const issueID = `iss_${String(issueIdx).padStart(9, '0')}`;
    const projectIdx = issueProjects[issueIdx];
    const project = allProjects[projectIdx];
    const cat = categories[project.categoryIndex];

    const numComments = commentsPerIssue[issueIdx];

    // Issue created timestamp (use faker for realistic spread)
    const issueCreated = faker.date
      .between({from: startDate, to: endDate})
      .getTime();

    for (let c = 0; c < numComments; c++) {
      const commentID = `cmt_${String(commentIdx++).padStart(9, '0')}`;

      // Generate body from template
      const bodyTemplate =
        cat.commentTemplates[(commentIdx * 7) % cat.commentTemplates.length];
      let body = fillTemplate(bodyTemplate, project.components);
      if (body.length > 65536) body = body.slice(0, 65533) + '...';

      const creatorID = users[Math.floor(rng() * users.length)].id;

      // Comment created after issue (1 hour to 30 days later)
      const created = faker.date
        .between({from: issueCreated, to: endDate})
        .getTime();

      await commentWriter.writeRow(
        [commentID, issueID, String(created), escapeCSV(body), creatorID].join(
          ',',
        ),
      );
    }
  }
  await commentWriter.close();

  // --- Generate issueLabels ---
  // oxlint-disable-next-line no-console
  console.log(
    `\nGenerating ~${totalIssueLabels.toLocaleString()} issueLabels (avg ${LABELS_PER_ISSUE} per issue)...`,
  );

  const issueLabelWriter = new ShardedCSVWriter(
    OUTPUT_DIR,
    'issueLabel',
    'labelID,issueID,projectID',
    SHARD_SIZE,
  );

  let issueLabelCount = 0;
  for (let issueIdx = 0; issueIdx < NUM_ISSUES; issueIdx++) {
    if (issueIdx % 100000 === 0) {
      // oxlint-disable-next-line no-console
      console.log(
        `  IssueLabels: processing issue ${issueIdx.toLocaleString()}/${NUM_ISSUES.toLocaleString()} (${((issueIdx / NUM_ISSUES) * 100).toFixed(1)}%)`,
      );
    }

    const issueID = `iss_${String(issueIdx).padStart(9, '0')}`;
    const projectIdx = issueProjects[issueIdx];
    const project = allProjects[projectIdx];

    // Uniform distribution: 1-3 labels per issue (or fewer if project has fewer labels)
    const maxLabels = Math.min(3, project.labelIDs.length);
    const numLabels = 1 + Math.floor(rng() * maxLabels);

    const usedLabelIndices = new Set<number>();
    for (let l = 0; l < numLabels && l < project.labelIDs.length; l++) {
      // Pick a random label, avoiding duplicates
      let labelIdx: number;
      let attempts = 0;
      do {
        labelIdx = Math.floor(rng() * project.labelIDs.length);
        attempts++;
      } while (usedLabelIndices.has(labelIdx) && attempts < 10);

      if (!usedLabelIndices.has(labelIdx)) {
        usedLabelIndices.add(labelIdx);
        const labelID = project.labelIDs[labelIdx];
        await issueLabelWriter.writeRow(
          [labelID, issueID, project.id].join(','),
        );
        issueLabelCount++;
      }
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
  console.log(`Issues: ${NUM_ISSUES.toLocaleString()}`);
  // oxlint-disable-next-line no-console
  console.log(`Comments: ${commentIdx.toLocaleString()}`);
  // oxlint-disable-next-line no-console
  console.log(`IssueLabels: ${issueLabelCount.toLocaleString()}`);
  // oxlint-disable-next-line no-console
  console.log(
    `Total: ${(users.length + allProjects.length + allLabels.length + NUM_ISSUES + commentIdx + issueLabelCount).toLocaleString()} rows`,
  );
}

main().catch(err => {
  console.error('Synthetic data generation failed:', err);
  process.exit(1);
});
