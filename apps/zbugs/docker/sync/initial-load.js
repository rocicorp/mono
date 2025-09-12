#!/usr/bin/env node

const { Client: PGClient } = require('pg');
const { Client: OSClient } = require('@opensearch-project/opensearch');

const pgConfig = {
  host: process.env.PG_HOST || 'localhost',
  port: process.env.PG_PORT || 6434,
  database: process.env.PG_DATABASE || 'postgres',
  user: process.env.PG_USER || 'user',
  password: process.env.PG_PASSWORD || 'password',
};

const osClient = new OSClient({
  node: process.env.OPENSEARCH_URL || 'http://localhost:9200',
});

const INDEX_NAME = 'issues';
const BATCH_SIZE = 100;

async function createIndex() {
  const indexExists = await osClient.indices.exists({ index: INDEX_NAME });
  
  if (indexExists.body) {
    console.log(`Index ${INDEX_NAME} already exists. Deleting...`);
    await osClient.indices.delete({ index: INDEX_NAME });
  }

  const mapping = require('../opensearch/index-mapping.json');
  await osClient.indices.create({
    index: INDEX_NAME,
    body: mapping
  });
  
  console.log(`Index ${INDEX_NAME} created successfully`);
}

async function loadIssues(pgClient) {
  const issuesQuery = `
    SELECT 
      i.id,
      i."shortID",
      i.title,
      i.description,
      i.open,
      i.created,
      i.modified,
      i."creatorID",
      creator.login as "creatorName",
      i."assigneeID",
      assignee.login as "assigneeName",
      i.visibility,
      COALESCE(
        ARRAY_AGG(DISTINCT l.name) FILTER (WHERE l.name IS NOT NULL),
        ARRAY[]::VARCHAR[]
      ) as labels
    FROM issue i
    LEFT JOIN "user" creator ON i."creatorID" = creator.id
    LEFT JOIN "user" assignee ON i."assigneeID" = assignee.id
    LEFT JOIN "issueLabel" il ON i.id = il."issueID"
    LEFT JOIN label l ON il."labelID" = l.id
    GROUP BY 
      i.id, i."shortID", i.title, i.description, i.open, 
      i.created, i.modified, i."creatorID", creator.login, 
      i."assigneeID", assignee.login, i.visibility
    ORDER BY i.created DESC
  `;

  const commentsQuery = `
    SELECT 
      c.id,
      c."issueID",
      c.body,
      c.created,
      c."creatorID",
      u.login as "creatorName"
    FROM comment c
    LEFT JOIN "user" u ON c."creatorID" = u.id
    ORDER BY c."issueID", c.created
  `;

  console.log('Fetching issues from PostgreSQL...');
  const issuesResult = await pgClient.query(issuesQuery);
  console.log(`Found ${issuesResult.rows.length} issues`);

  console.log('Fetching comments from PostgreSQL...');
  const commentsResult = await pgClient.query(commentsQuery);
  console.log(`Found ${commentsResult.rows.length} comments`);

  // Group comments by issue
  const commentsByIssue = {};
  for (const comment of commentsResult.rows) {
    if (!commentsByIssue[comment.issueID]) {
      commentsByIssue[comment.issueID] = [];
    }
    commentsByIssue[comment.issueID].push({
      id: comment.id,
      body: comment.body,
      created: comment.created,
      creatorID: comment.creatorID,
      creatorName: comment.creatorName
    });
  }

  // Prepare bulk operations
  const operations = [];
  for (const issue of issuesResult.rows) {
    // Prepare document with nested comments
    const doc = {
      id: issue.id,
      shortID: issue.shortID,
      title: issue.title,
      description: issue.description || '',
      open: issue.open,
      created: issue.created,
      modified: issue.modified,
      creatorID: issue.creatorID,
      creatorName: issue.creatorName,
      assigneeID: issue.assigneeID,
      assigneeName: issue.assigneeName,
      visibility: issue.visibility,
      labels: issue.labels,
      comments: commentsByIssue[issue.id] || []
    };

    operations.push(
      { index: { _index: INDEX_NAME, _id: issue.id } },
      doc
    );
  }

  // Bulk index in batches
  console.log('Indexing documents to OpenSearch...');
  for (let i = 0; i < operations.length; i += BATCH_SIZE * 2) {
    const batch = operations.slice(i, i + BATCH_SIZE * 2);
    const response = await osClient.bulk({ body: batch });
    
    if (response.body.errors) {
      console.error('Bulk indexing errors:', JSON.stringify(response.body.errors, null, 2));
    } else {
      console.log(`Indexed batch ${Math.floor(i / (BATCH_SIZE * 2)) + 1}/${Math.ceil(operations.length / (BATCH_SIZE * 2))}`);
    }
  }

  console.log(`Successfully indexed ${issuesResult.rows.length} issues with their comments`);
}

async function main() {
  const pgClient = new PGClient(pgConfig);
  
  try {
    await pgClient.connect();
    console.log('Connected to PostgreSQL');

    await createIndex();
    await loadIssues(pgClient);

    // Refresh index to make documents searchable immediately
    await osClient.indices.refresh({ index: INDEX_NAME });
    console.log('Index refreshed');

    // Get count to verify
    const count = await osClient.count({ index: INDEX_NAME });
    console.log(`Total documents in index: ${count.body.count}`);

  } catch (error) {
    console.error('Error during migration:', error);
    process.exit(1);
  } finally {
    await pgClient.end();
  }
}

main().catch(console.error);