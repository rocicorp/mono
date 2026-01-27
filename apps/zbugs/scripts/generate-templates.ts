/**
 * Generates parameterized template pools via Claude API for synthetic data generation.
 *
 * Templates use {{slot}} placeholders that get filled by faker during CSV generation.
 *
 * Usage:
 *   ANTHROPIC_API_KEY=sk-... npx tsx scripts/generate-templates.ts
 *
 * Env vars:
 *   ANTHROPIC_API_KEY - required
 *   NUM_PROJECTS      - projects per category (default 10, total = NUM_PROJECTS_PER_CAT * 10)
 */

import * as fs from 'fs';
import * as path from 'path';
import {fileURLToPath} from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TEMPLATES_DIR = path.join(__dirname, '../db/seed-data/templates');

const NUM_PROJECTS_PER_CATEGORY = Math.ceil(
  (parseInt(process.env.NUM_PROJECTS ?? '100', 10) || 100) / 10,
);

interface CategoryTemplates {
  category: string;
  projects: Array<{name: string; components: string[]}>;
  labels: string[];
  titleTemplates: string[];
  descriptionTemplates: string[];
  commentTemplates: string[];
}

const CATEGORIES = [
  'web-frontend',
  'mobile-app',
  'api-service',
  'infrastructure',
  'data-platform',
  'developer-tools',
  'security',
  'ml-ai',
  'embedded-iot',
  'game-media',
] as const;

type Category = (typeof CATEGORIES)[number];

const CATEGORY_DESCRIPTIONS: Record<Category, string> = {
  'web-frontend':
    'Web frontend applications like dashboards, e-commerce sites, news readers, admin panels, landing page builders',
  'mobile-app':
    'Mobile applications like field trackers, health apps, sync tools, inventory managers, fitness trackers',
  'api-service':
    'Backend API services like gateways, webhook processors, data bridges, notification services, payment processors',
  'infrastructure':
    'Infrastructure tools like CI/CD pipelines, monitoring dashboards, cloud provisioners, network managers, container orchestrators',
  'data-platform':
    'Data platforms like query engines, ETL pipelines, metrics databases, data warehouses, stream processors',
  'developer-tools':
    'Developer tools like CLIs, linters, code generators, schema validators, documentation generators',
  'security':
    'Security tools like auth services, vault managers, audit loggers, vulnerability scanners, access control systems',
  'ml-ai':
    'ML/AI platforms like model servers, training pipelines, prediction APIs, feature stores, experiment trackers',
  'embedded-iot':
    'Embedded/IoT systems like sensor grids, firmware managers, edge routers, device provisioners, telemetry collectors',
  'game-media':
    'Game/media software like render engines, physics simulators, audio mixers, asset pipelines, animation editors',
};

async function callClaude(prompt: string): Promise<string> {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    throw new Error('ANTHROPIC_API_KEY environment variable is required');
  }

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 8192,
      messages: [{role: 'user', content: prompt}],
    }),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Claude API error ${response.status}: ${body}`);
  }

  const data = (await response.json()) as {
    content: Array<{type: string; text: string}>;
  };
  return data.content[0].text;
}

function parseJsonFromResponse(text: string): unknown {
  // Extract JSON from markdown code block if present
  const codeBlockMatch = text.match(/```(?:json)?\s*\n?([\s\S]*?)\n?```/);
  const jsonStr = codeBlockMatch ? codeBlockMatch[1] : text;
  return JSON.parse(jsonStr.trim());
}

async function generateProjectsAndComponents(
  category: Category,
): Promise<Array<{name: string; components: string[]}>> {
  const prompt = `Generate exactly ${NUM_PROJECTS_PER_CATEGORY} fictional software project names for the category "${category}" (${CATEGORY_DESCRIPTIONS[category]}).

For each project, also generate 50 unique component/module names that would exist in that kind of project (e.g. for a web frontend: "checkout page", "search bar", "user profile modal", "navigation menu", "product carousel").

Return ONLY a JSON array, no other text:
[
  {"name": "ProjectName", "components": ["component1", "component2", ...]}
]

Requirements:
- Project names should be 1-2 words, CamelCase, realistic software names
- Components should be lowercase, 1-4 words, specific to the project's domain
- No duplicates within a project or across projects in this category`;

  const response = await callClaude(prompt);
  return parseJsonFromResponse(response) as Array<{
    name: string;
    components: string[];
  }>;
}

async function generateLabels(category: Category): Promise<string[]> {
  const prompt = `Generate exactly 48 issue label names appropriate for software projects in the "${category}" category (${CATEGORY_DESCRIPTIONS[category]}).

Include a mix of:
- Priority labels (critical, high, medium, low)
- Type labels (bug, feature, enhancement, refactor, docs, test, chore)
- Status labels (needs-triage, in-review, blocked, wontfix)
- Domain-specific labels relevant to ${category} projects

Return ONLY a JSON array of strings, no other text:
["label1", "label2", ...]

Requirements:
- Exactly 48 labels
- All lowercase with hyphens for spaces
- No duplicates`;

  const response = await callClaude(prompt);
  return parseJsonFromResponse(response) as string[];
}

async function generateTitleTemplates(category: Category): Promise<string[]> {
  const prompt = `Generate exactly 200 unique issue title templates for software projects in the "${category}" category (${CATEGORY_DESCRIPTIONS[category]}).

Each template should use {{slot}} placeholders that will be filled later:
- {{component}} - a module/component name
- {{action}} - a user action like "clicking", "submitting", "loading"
- {{environment}} - like "production", "staging", "CI", "Safari", "Chrome"
- {{dependency}} - a library/package name
- {{version}} - a version number
- {{error}} - an error type like "TypeError", "timeout", "404"
- {{feature}} - a feature name
- {{metric}} - a performance metric like "load time", "memory usage"
- {{user_type}} - a user role like "admin", "guest", "new user"

Return ONLY a JSON array of strings, no other text:
["{{component}} crashes when {{action}} in {{environment}}", ...]

Requirements:
- Exactly 200 templates
- Each under 128 characters after slot filling (slots average ~15 chars)
- Mix of bugs, features, performance issues, tasks
- Realistic software issue titles for ${category}
- Each template must use at least one {{slot}}`;

  const response = await callClaude(prompt);
  return parseJsonFromResponse(response) as string[];
}

async function generateDescriptionTemplates(
  category: Category,
): Promise<string[]> {
  const prompt = `Generate exactly 100 unique issue description templates for software projects in the "${category}" category (${CATEGORY_DESCRIPTIONS[category]}).

Each template should use {{slot}} placeholders:
- {{component}} - a module/component name
- {{action}} - a user action
- {{environment}} - deployment environment or browser
- {{error}} - error type/message
- {{steps}} - reproduction steps
- {{expected}} - expected behavior
- {{actual}} - actual behavior
- {{version}} - version number

Return ONLY a JSON array of strings, no other text.

Requirements:
- Exactly 100 templates
- Each 100-500 characters
- Include bug reports, feature requests, improvement proposals
- Realistic, detailed descriptions for ${category}
- Use standard bug report structure where appropriate`;

  const response = await callClaude(prompt);
  return parseJsonFromResponse(response) as string[];
}

async function generateCommentTemplates(category: Category): Promise<string[]> {
  const prompt = `Generate exactly 100 unique issue comment templates for software projects in the "${category}" category (${CATEGORY_DESCRIPTIONS[category]}).

Each template should use {{slot}} placeholders:
- {{component}} - a module/component name
- {{action}} - what was done to fix/investigate
- {{finding}} - what was discovered
- {{suggestion}} - a proposed fix
- {{workaround}} - a temporary workaround
- {{version}} - version number
- {{user}} - a person's name

Return ONLY a JSON array of strings, no other text.

Requirements:
- Exactly 100 templates
- Each 50-400 characters
- Mix of: investigation updates, proposed fixes, workarounds, status updates, questions, code review comments, test results
- Realistic developer conversation for ${category}`;

  const response = await callClaude(prompt);
  return parseJsonFromResponse(response) as string[];
}

async function generateCategoryTemplates(
  category: Category,
): Promise<CategoryTemplates> {
  // oxlint-disable-next-line no-console
  console.log(`Generating templates for category: ${category}`);

  // Run all generations for this category in parallel
  const [
    projects,
    labels,
    titleTemplates,
    descriptionTemplates,
    commentTemplates,
  ] = await Promise.all([
    generateProjectsAndComponents(category),
    generateLabels(category),
    generateTitleTemplates(category),
    generateDescriptionTemplates(category),
    generateCommentTemplates(category),
  ]);

  // oxlint-disable-next-line no-console
  console.log(
    `  ${category}: ${projects.length} projects, ${labels.length} labels, ` +
      `${titleTemplates.length} titles, ${descriptionTemplates.length} descriptions, ` +
      `${commentTemplates.length} comments`,
  );

  return {
    category,
    projects,
    labels,
    titleTemplates,
    descriptionTemplates,
    commentTemplates,
  };
}

async function main() {
  fs.mkdirSync(TEMPLATES_DIR, {recursive: true});

  // oxlint-disable-next-line no-console
  console.log(
    `Generating templates for ${CATEGORIES.length} categories, ${NUM_PROJECTS_PER_CATEGORY} projects each...`,
  );

  // Process categories in batches of 3 to avoid rate limits
  const BATCH_SIZE = 3;
  const allTemplates: CategoryTemplates[] = [];

  for (let i = 0; i < CATEGORIES.length; i += BATCH_SIZE) {
    const batch = CATEGORIES.slice(i, i + BATCH_SIZE);
    // oxlint-disable-next-line no-console
    console.log(
      `\nBatch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(CATEGORIES.length / BATCH_SIZE)}: ${batch.join(', ')}`,
    );
    const results = await Promise.all(
      batch.map(cat => generateCategoryTemplates(cat)),
    );
    allTemplates.push(...results);
  }

  // Write individual category files
  for (const templates of allTemplates) {
    const outPath = path.join(TEMPLATES_DIR, `${templates.category}.json`);
    fs.writeFileSync(outPath, JSON.stringify(templates, null, 2));
    // oxlint-disable-next-line no-console
    console.log(`Wrote ${outPath}`);
  }

  // Write summary
  const summary = {
    categories: allTemplates.map(t => ({
      category: t.category,
      projects: t.projects.length,
      labels: t.labels.length,
      titleTemplates: t.titleTemplates.length,
      descriptionTemplates: t.descriptionTemplates.length,
      commentTemplates: t.commentTemplates.length,
    })),
    totalProjects: allTemplates.reduce((s, t) => s + t.projects.length, 0),
    totalLabels: allTemplates.reduce((s, t) => s + t.labels.length, 0),
    totalTitleTemplates: allTemplates.reduce(
      (s, t) => s + t.titleTemplates.length,
      0,
    ),
    totalDescriptionTemplates: allTemplates.reduce(
      (s, t) => s + t.descriptionTemplates.length,
      0,
    ),
    totalCommentTemplates: allTemplates.reduce(
      (s, t) => s + t.commentTemplates.length,
      0,
    ),
  };

  fs.writeFileSync(
    path.join(TEMPLATES_DIR, 'summary.json'),
    JSON.stringify(summary, null, 2),
  );
  // oxlint-disable-next-line no-console
  console.log('\nTemplate generation complete!');
  // oxlint-disable-next-line no-console
  console.log(JSON.stringify(summary, null, 2));
}

main().catch(err => {
  console.error('Template generation failed:', err);
  process.exit(1);
});
