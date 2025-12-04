/* oxlint-disable no-console */
import * as fs from 'node:fs';
import * as http from 'node:http';
import * as path from 'node:path';
import * as url from 'node:url';
import {Application, TSConfigReader} from 'typedoc';
import {WebSocket, WebSocketServer} from 'ws';

// Get the directory name using import.meta.url
const dirname = url.fileURLToPath(new URL('.', import.meta.url));

// Define a custom WebSocket server type with our notifyClients method
interface LiveReloadServer {
  clients: Set<WebSocket>;
  notifyClients: () => void;
}

// Create output directory if it doesn't exist
const outputDir = path.resolve(dirname, '../docs');
if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir, {recursive: true});
}

const entryPoints = [
  path.resolve(dirname, '../src/zero.ts'),
  path.resolve(dirname, '../src/react.ts'),
  path.resolve(dirname, '../src/solid.ts'),
];

// Explicitly set the tsconfig path to use tsconfig.client.json which includes the entry points
const tsconfigPath = path.resolve(dirname, '../tsconfig.client.json');

const options = {
  entryPoints,
  out: outputDir,
  name: 'Zero API Documentation',
  excludePrivate: true,
  excludeProtected: true,
  preserveWatchOutput: true,
  tsconfig: tsconfigPath,
};

let wsServer: LiveReloadServer | null = null;

// Start TypeDoc in watch mode
async function startTypedocWatcher() {
  // Create TypeDoc application using the static factory method
  const app = await Application.bootstrap(options);

  // Add TSConfig reader
  app.options.addReader(new TSConfigReader());

  console.log(`Starting TypeDoc in watch mode`);

  // Use convertAndWatch to watch for changes and regenerate documentation
  await app.convertAndWatch(async project => {
    if (project) {
      console.log(`[TypeDoc] Documentation updated at ${outputDir}`);
      await app.generateDocs(project, outputDir);

      // Notify WebSocket clients to reload
      if (wsServer) {
        console.log('Notifying clients to reload');
        wsServer.notifyClients();
      }
    } else {
      console.error('[TypeDoc] Failed to generate documentation');
    }
  });
}

// Generate docs once without watch mode
async function generateDocsOnce() {
  try {
    // Create TypeDoc application using the static factory method
    const app = await Application.bootstrap(options);

    // Add TSConfig reader
    app.options.addReader(new TSConfigReader());

    // Generate docs
    const project = await app.convert();

    if (project) {
      await app.generateDocs(project, outputDir);

      // Verify files were created
      const files = fs
        .readdirSync(outputDir)
        .filter(file => !file.startsWith('.'));

      if (files.length === 0) {
        throw new Error('No documentation files were generated');
      }

      console.log(`Documentation generated successfully at ${outputDir}`);
      console.log(`Generated ${files.length} files`);
    } else {
      console.error('Failed to generate documentation');
    }
  } catch (error) {
    console.error(
      'Error generating documentation:',
      error instanceof Error ? error.message : String(error),
    );
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
  void startTypedocWatcher();
} else {
  void generateDocsOnce();
}

// Start server if requested
if (runServer) {
  const docsDir = path.resolve(dirname, '../docs');
  const httpServer = createServer(docsDir);
  wsServer = createWebSocketServer();

  // Handle termination
  process.on('SIGINT', () => {
    console.log('Stopping servers...');
    httpServer.close();
    process.exit(0);
  });
}
