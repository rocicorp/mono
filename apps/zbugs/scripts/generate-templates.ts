/**
 * Generates parameterized template pools via Claude API for synthetic data generation.
 *
 * Templates use {{slot}} placeholders that get filled during CSV generation.
 * Designed to support up to 100M issues with max 300K per project and minimal repetition.
 *
 * Template counts per category:
 *   - 200 title templates × ~15K slot combinations = 3M unique titles per project
 *   - 100 description templates × ~1.5M slot combinations = 150M unique descriptions
 *   - 150 comment templates × ~50K slot combinations = 7.5M unique comments per project
 *
 * Usage:
 *   ANTHROPIC_API_KEY=sk-... npx tsx scripts/generate-templates.ts
 *
 * Env vars:
 *   ANTHROPIC_API_KEY - required
 *   NUM_PROJECTS      - total projects (default 100, divided across 10 categories)
 *   START_CATEGORY    - 1-indexed category to start from for resuming (default 1)
 */

import * as fs from 'fs';
import * as path from 'path';
import {fileURLToPath} from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TEMPLATES_DIR = path.join(__dirname, '../db/seed-data/templates');

const NUM_PROJECTS_PER_CATEGORY = Math.ceil(
  (parseInt(process.env.NUM_PROJECTS ?? '100', 10) || 100) / 10,
);

// 1-indexed category number to start from (for resuming after failures)
const START_CATEGORY = parseInt(process.env.START_CATEGORY ?? '1', 10);

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

async function callClaude(
  prompt: string,
  maxTokens: number = 16384,
): Promise<string> {
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
      max_tokens: maxTokens,
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

For each project, also generate 100 unique component/module names that would exist in that kind of project (e.g. for a web frontend: "checkout page", "search bar", "user profile modal", "navigation menu", "product carousel", "notification badge", "settings panel", etc.).

Return ONLY a JSON array, no other text:
[
  {"name": "ProjectName", "components": ["component1", "component2", ...]}
]

Requirements:
- Project names should be 1-2 words, CamelCase, realistic software names
- Components should be lowercase, 1-4 words, specific to the project's domain
- Exactly 100 components per project
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

async function generateTitleTemplatesBatch(
  category: Category,
  batchNum: number,
  batchSize: number,
): Promise<string[]> {
  const focusAreas = [
    'bugs, crashes, and errors',
    'features, enhancements, and improvements',
  ];
  const focus = focusAreas[batchNum % focusAreas.length];

  const prompt = `Generate exactly ${batchSize} unique issue title templates for software projects in the "${category}" category (${CATEGORY_DESCRIPTIONS[category]}).

Focus on: ${focus}

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
- Exactly ${batchSize} templates
- Each under 128 characters after slot filling (slots average ~15 chars)
- All templates should be unique and different from common patterns
- Realistic software issue titles for ${category}
- Each template must use at least 2 {{slot}} placeholders for variety`;

  const response = await callClaude(prompt);
  return parseJsonFromResponse(response) as string[];
}

async function generateTitleTemplates(category: Category): Promise<string[]> {
  // Generate 200 titles in 2 batches of 100 to avoid truncation
  const batch1 = await generateTitleTemplatesBatch(category, 0, 100);
  const batch2 = await generateTitleTemplatesBatch(category, 1, 100);
  return [...batch1, ...batch2];
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
- {{dependency}} - a library/package name
- {{feature}} - a feature name
- {{metric}} - a performance metric

Return ONLY a JSON array of strings, no other text.

Requirements:
- Exactly 100 templates
- Each 100-500 characters
- Include bug reports, feature requests, improvement proposals, performance issues
- Realistic, detailed descriptions specific to ${category}
- Use standard bug report structure where appropriate
- Each template must use at least 3 {{slot}} placeholders for maximum variety`;

  const response = await callClaude(prompt);
  return parseJsonFromResponse(response) as string[];
}

async function generateCommentTemplatesBatch(
  category: Category,
  batchNum: number,
  batchSize: number,
): Promise<string[]> {
  const focusAreas = [
    'investigation updates and debugging findings',
    'proposed fixes, workarounds, and solutions',
    'questions, status updates, and code review feedback',
  ];
  const focus = focusAreas[batchNum % focusAreas.length];

  const prompt = `Generate exactly ${batchSize} unique issue comment templates for software projects in the "${category}" category (${CATEGORY_DESCRIPTIONS[category]}).

Focus on: ${focus}

Each template should use {{slot}} placeholders:
- {{component}} - a module/component name
- {{action}} - what was done to fix/investigate
- {{finding}} - what was discovered
- {{suggestion}} - a proposed fix
- {{workaround}} - a temporary workaround
- {{version}} - version number
- {{user}} - a person's name
- {{error}} - an error type
- {{dependency}} - a library/package name

Return ONLY a JSON array of strings, no other text.

Requirements:
- Exactly ${batchSize} templates
- Each 50-400 characters
- Realistic developer conversation for ${category}
- Each template must use at least 2 {{slot}} placeholders`;

  const response = await callClaude(prompt);
  return parseJsonFromResponse(response) as string[];
}

async function generateCommentTemplates(category: Category): Promise<string[]> {
  // Generate 150 comments in 3 batches of 50 to avoid truncation
  const batch1 = await generateCommentTemplatesBatch(category, 0, 50);
  const batch2 = await generateCommentTemplatesBatch(category, 1, 50);
  const batch3 = await generateCommentTemplatesBatch(category, 2, 50);
  return [...batch1, ...batch2, ...batch3];
}

async function generateCategoryTemplates(
  category: Category,
): Promise<CategoryTemplates> {
  // oxlint-disable-next-line no-console
  console.log(`Generating templates for category: ${category}`);

  // Generate projects, labels, and descriptions in parallel (single API calls)
  // oxlint-disable-next-line no-console
  console.log(`  ${category}: generating projects, labels, descriptions...`);
  const [projects, labels, descriptionTemplates] = await Promise.all([
    generateProjectsAndComponents(category),
    generateLabels(category),
    generateDescriptionTemplates(category),
  ]);

  // Generate titles (2 batches) and comments (3 batches) sequentially
  // oxlint-disable-next-line no-console
  console.log(`  ${category}: generating title templates (2 batches)...`);
  const titleTemplates = await generateTitleTemplates(category);

  // oxlint-disable-next-line no-console
  console.log(`  ${category}: generating comment templates (3 batches)...`);
  const commentTemplates = await generateCommentTemplates(category);

  // oxlint-disable-next-line no-console
  console.log(
    `  ${category}: ${projects.length} projects (${projects[0]?.components?.length ?? 0} components each), ` +
      `${labels.length} labels, ${titleTemplates.length} titles, ` +
      `${descriptionTemplates.length} descriptions, ${commentTemplates.length} comments`,
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
  // oxlint-disable-next-line no-console
  console.log(
    'Each category: 100 components/project, 200 titles, 100 descriptions, 150 comments',
  );

  // Process categories one at a time since each makes 8 API calls
  const allTemplates: CategoryTemplates[] = [];

  // Start from START_CATEGORY (1-indexed, so subtract 1 for array index)
  const startIndex = Math.max(0, START_CATEGORY - 1);
  if (startIndex > 0) {
    // oxlint-disable-next-line no-console
    console.log(
      `Resuming from category ${START_CATEGORY} (${CATEGORIES[startIndex]}), skipping ${startIndex} categories`,
    );
  }

  for (let i = startIndex; i < CATEGORIES.length; i++) {
    const category = CATEGORIES[i];
    // oxlint-disable-next-line no-console
    console.log(
      `\n=== Category ${i + 1}/${CATEGORIES.length}: ${category} ===`,
    );
    const result = await generateCategoryTemplates(category);
    allTemplates.push(result);

    // Write each category file as we go (in case of failure)
    const outPath = path.join(TEMPLATES_DIR, `${result.category}.json`);
    fs.writeFileSync(outPath, JSON.stringify(result, null, 2));
    // oxlint-disable-next-line no-console
    console.log(`  Wrote ${outPath}`);
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
