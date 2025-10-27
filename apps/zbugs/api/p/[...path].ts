import {readFileSync} from 'fs';
import {join} from 'path';
import type {IncomingMessage, ServerResponse} from 'http';

export default function handler(
  req: IncomingMessage & {query?: Record<string, string | string[]>},
  res: ServerResponse,
) {
  try {
    // Get the project name from the URL
    const pathParts = (req.query?.path as string[]) || [];
    const projectName = pathParts[0]?.toLowerCase() || '';

    // Read the index.html file from the dist directory
    // In production, files are in the .vercel/output directory
    let html: string;
    try {
      // Try production path first
      html = readFileSync(join(process.cwd(), 'index.html'), 'utf-8');
    } catch {
      // Fallback to dev path
      html = readFileSync(join(process.cwd(), 'dist', 'index.html'), 'utf-8');
    }

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
    res.end('Internal Server Error');
  }
}
