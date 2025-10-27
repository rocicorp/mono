import {readFileSync} from 'fs';
import {join} from 'path';
import type {IncomingMessage, ServerResponse} from 'http';

// Cache the HTML content to avoid reading the file on every request
let cachedHtml: string | null = null;

function getIndexHtml(): string {
  if (cachedHtml) {
    return cachedHtml;
  }

  // Try different possible locations for index.html in Vercel deployment
  const possiblePaths = [
    // Vercel build output location
    join(process.cwd(), '.vercel', 'output', 'static', 'index.html'),
    // Alternative Vercel location
    join(process.cwd(), 'public', 'index.html'),
    // Local dev
    join(process.cwd(), 'index.html'),
    // Built output
    join(process.cwd(), 'dist', 'index.html'),
  ];

  for (const path of possiblePaths) {
    try {
      cachedHtml = readFileSync(path, 'utf-8');
      console.log(`Successfully loaded index.html from: ${path}`);
      return cachedHtml;
    } catch {
      // Try next path
    }
  }

  throw new Error(
    'Could not find index.html in any expected location: ' +
      possiblePaths.join(', '),
  );
}

export default function handler(
  req: IncomingMessage & {
    query?: Record<string, string | string[]>;
  },
  res: ServerResponse,
) {
  try {
    // Get the project name from the URL
    const pathParts = (req.query?.path as string[]) || [];
    const projectName = pathParts[0]?.toLowerCase() || '';

    // Get the base HTML
    let html = getIndexHtml();

    // Check if this is the Roci project
    const isRoci = projectName === 'roci';

    if (isRoci) {
      // Replace all occurrences of og-image.png with og-gigabugs.png
      html = html.replace(
        /https:\/\/bugs\.rocicorp\.dev\/og-image\.png/g,
        'https://bugs.rocicorp.dev/og-gigabugs.png',
      );
    }

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.setHeader('Cache-Control', 's-maxage=3600, stale-while-revalidate');
    res.statusCode = 200;
    res.end(html);
  } catch (error) {
    console.error('Error serving HTML:', error);
    res.statusCode = 500;
    res.end(`Internal Server Error: ${error}`);
  }
}
