#!/usr/bin/env node
//
// Rebuilds zero-stability-report.html by injecting report-data.json into it.
// Edit report-data.json, then run:  node build-report.js
//
// The HTML is both template and output: this script replaces the
// <script id="report-data"> block in place, so re-running is idempotent.

const fs = require('fs');
const path = require('path');

const root = __dirname;
const htmlPath = path.join(root, 'zero-stability-report.html');
const dataPath = path.join(root, 'report-data.json');

if (!fs.existsSync(dataPath)) {
  console.error('Error: report-data.json not found.');
  process.exit(1);
}

const template = fs.readFileSync(htmlPath, 'utf8');
const data = fs.readFileSync(dataPath, 'utf8');

try {
  JSON.parse(data);
} catch (e) {
  console.error('Error: report-data.json is not valid JSON.');
  console.error(e.message);
  process.exit(1);
}

const result = template.replace(
  /<script id="report-data" type="application\/json">[\s\S]*?<\/script>/,
  `<script id="report-data" type="application/json">\n${data}\n  </script>`,
);

fs.writeFileSync(htmlPath, result);
console.log('Rebuilt: zero-stability-report.html');
