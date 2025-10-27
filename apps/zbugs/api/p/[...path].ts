import type {IncomingMessage, ServerResponse} from 'http';

export default async function handler(
  req: IncomingMessage & {
    query?: Record<string, string | string[]>;
    headers: IncomingMessage['headers'];
  },
  res: ServerResponse,
) {
  try {
    // Get the project name from the URL
    const pathParts = (req.query?.path as string[]) || [];
    const projectName = pathParts[0]?.toLowerCase() || '';

    // Get the host from the request to fetch index.html
    const host = req.headers.host || 'bugs.rocicorp.dev';
    const protocol = host.includes('localhost') ? 'http' : 'https';
    const indexUrl = `${protocol}://${host}/index.html`;

    // Fetch the index.html file
    const response = await fetch(indexUrl);
    if (!response.ok) {
      throw new Error(`Failed to fetch index.html: ${response.statusText}`);
    }
    let html = await response.text();

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
