/* eslint-disable no-console */
import * as chokidar from 'chokidar';
import {ChildProcess, execSync, spawn} from 'node:child_process';
import * as fs from 'node:fs';
import * as http from 'node:http';
import * as path from 'node:path';
import * as url from 'node:url';
import {WebSocket, WebSocketServer} from 'ws';

// Get the directory name using import.meta.url
const dirname = url.fileURLToPath(new URL('.', import.meta.url));

// Store TypeDoc process reference
let typeDocProcess: ChildProcess | null = null;

// Start TypeDoc in watch mode
function startTypedocWatcher() {
  // Create output directory if it doesn't exist
  const outputDir = path.resolve(dirname, '../docs');
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, {recursive: true});
  }

  const entryPoint = path.resolve(dirname, '../src/mod.ts');

  // Build the TypeDoc CLI command with --watch flag
  const args = [
    'typedoc',
    entryPoint,
    '--out',
    outputDir,
    '--name',
    '@rocicorp/zero API Documentation',
    '--excludePrivate',
    '--excludeProtected',
    '--watch', // Enable incremental watch mode
    '--preserveWatchOutput',
    '--logLevel',
    'Warn',
  ];

  console.log(`Starting TypeDoc in watch mode: npx ${args.join(' ')}`);

  // Run TypeDoc as a child process instead of using execSync
  typeDocProcess = spawn('npx', args, {
    cwd: path.resolve(dirname, '..'),
    stdio: 'pipe', // Capture output
  });

  // Forward stdout/stderr with prefixes
  typeDocProcess.stdout?.on('data', data => {
    const output = data.toString().trim();
    if (output) console.log(`[TypeDoc] ${output}`);
  });

  typeDocProcess.stderr?.on('data', data => {
    const output = data.toString().trim();
    if (output) console.error(`[TypeDoc] ${output}`);
  });

  typeDocProcess.on('close', code => {
    console.log(`TypeDoc process exited with code ${code}`);
    typeDocProcess = null;
  });

  return typeDocProcess;
}

// Generate docs once without watch mode
function generateDocsOnce() {
  // Create output directory if it doesn't exist
  const outputDir = path.resolve(dirname, '../docs');
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, {recursive: true});
  }

  try {
    const entryPoint = path.resolve(dirname, '../src/mod.ts');

    // Build the TypeDoc CLI command
    const command = [
      'npx typedoc',
      `"${entryPoint}"`,
      `--out "${outputDir}"`,
      '--name "Zero Client API Documentation"',
      '--excludePrivate',
      '--excludeProtected',
    ].join(' ');

    console.log(`Executing: ${command}`);

    // Execute TypeDoc via CLI
    execSync(command, {
      stdio: 'inherit',
      cwd: path.resolve(dirname, '..'),
    });

    // Verify files were created
    const files = fs
      .readdirSync(outputDir)
      .filter(file => !file.startsWith('.'));

    if (files.length === 0) {
      throw new Error('No documentation files were generated');
    }

    console.log(`Documentation generated successfully at ${outputDir}`);
    console.log(`Generated ${files.length} files`);
    return 0;
  } catch (error) {
    console.error(
      'Error generating documentation:',
      error instanceof Error ? error.message : String(error),
    );
    return 1;
  }
}

// Create a simple web server for the documentation
function createServer(docsDir: string, port = 3000) {
  const server = http.createServer((req, res) => {
    // Add live reload script to HTML files
    if (req.url === '/livereload.js') {
      res.writeHead(200, {'Content-Type': 'text/javascript'});
      res.end(`
        const socket = new WebSocket('ws://localhost:${port + 1}');
        socket.addEventListener('message', () => {
          console.log('Reloading page...');
          window.location.reload();
        });
      `);
      return;
    }

    // Default to index.html for root path
    let filePath = path.join(docsDir, req.url || '');
    if (req.url === '/' || req.url === '') {
      filePath = path.join(docsDir, 'index.html');
    }

    // Handle file serving
    fs.readFile(filePath, (err, data) => {
      if (err) {
        if (err.code === 'ENOENT') {
          res.writeHead(404);
          res.end('File not found');
        } else {
          res.writeHead(500);
          res.end('Server error');
        }
        return;
      }

      const ext = path.extname(filePath);
      const contentType = getContentType(ext);
      res.writeHead(200, {'Content-Type': contentType});

      // Inject live reload script into HTML files
      if (ext === '.html') {
        const html = data.toString();
        const injectedHtml = html.replace(
          '</head>',
          '<script src="/livereload.js"></script></head>',
        );
        res.end(injectedHtml);
      } else {
        res.end(data);
      }
    });
  });

  server.listen(port, () => {
    console.log(`Documentation server running at http://localhost:${port}`);
  });

  return server;
}

// Define a custom WebSocket server type with our notifyClients method
interface LiveReloadServer {
  clients: Set<WebSocket>;
  notifyClients: () => void;
}

// Create WebSocket server for live reload
function createWebSocketServer(port = 3001): LiveReloadServer {
  const wss = new WebSocketServer({port});

  console.log(`WebSocket server for live reload running on port ${port}`);

  return {
    clients: wss.clients,
    notifyClients: () => {
      for (const client of wss.clients) {
        if (client.readyState === WebSocket.OPEN) {
          client.send('reload');
        }
      }
    },
  };
}

// Watch for changes to the docs directory to trigger browser reload
function watchDocsForChanges(wsServer: LiveReloadServer) {
  const docsDir = path.resolve(dirname, '../docs');

  console.log('Setting up watcher for documentation output...');

  // Watch generated HTML files for changes
  const watcher = chokidar.watch(path.join(docsDir, '**/*.html'), {
    ignoreInitial: true,
    persistent: true,
  });

  // When docs are updated, notify clients to reload
  watcher.on('change', () => {
    wsServer.notifyClients();
  });

  return watcher;
}

function getContentType(ext: string) {
  const contentTypes: Record<string, string> = {
    '.html': 'text/html',
    '.css': 'text/css',
    '.js': 'text/javascript',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.svg': 'image/svg+xml',
  };

  return contentTypes[ext] || 'text/plain';
}

// Parse command line arguments
const args = process.argv.slice(2);
const runServer = args.includes('--server') || args.includes('-s');
const watchMode = args.includes('--watch') || args.includes('-w') || runServer;

// Run documentation generation based on mode
if (watchMode) {
  startTypedocWatcher();
} else {
  generateDocsOnce();
}

// Start server if requested
if (runServer) {
  const docsDir = path.resolve(dirname, '../docs');
  const httpServer = createServer(docsDir);
  const wsServer = createWebSocketServer();
  const docsWatcher = watchDocsForChanges(wsServer);

  // Handle termination
  process.on('SIGINT', async () => {
    console.log('Stopping servers and processes...');
    if (typeDocProcess) {
      typeDocProcess.kill();
    }
    httpServer.close();
    await docsWatcher.close();
    process.exit(0);
  });
}
