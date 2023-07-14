import type {NextApiRequest, NextApiResponse} from 'next';

async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    return res.status(405).end(); // Method Not Allowed
  }

  const endpoint =
    'https://app.loops.so/api/newsletter-form/cliw1dqmq030ul20nil8cz4h2';
  const options = {
    method: 'POST',
    body: req.body,
    headers: {'Content-Type': 'application/json'},
  };

  const response = await fetch(endpoint, options);
  if (!response.ok) {
    return res.status(response.status).json({
      message: `Could not forward the request to the server: ${response.statusText}`,
    });
  }

  const data = await response.json();
  return res.status(200).json(data);
}

export {handler as default};
