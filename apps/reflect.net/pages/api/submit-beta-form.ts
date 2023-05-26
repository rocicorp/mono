import type {NextApiRequest, NextApiResponse} from 'next';

async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    return res.status(405).end(); // Method Not Allowed
  }

  const endpoint =
    'https://script.google.com/macros/s/AKfycbyldDa4OZdGRRj-Mkrl4PkZUmIj-6XmHiNRoBjnorltIDAf5h0GYzIbVIMr5m-FN05i3Q/exec';
  const options = {
    method: 'POST',
    body: req.body,
    headers: {'Content-Type': 'application/json'},
  };

  const response = await fetch(endpoint, options);
  if (!response.ok) {
    return res.status(response.status).json({
      message: `Could not forward the request to the Google Script: ${response.statusText}`,
    });
  }

  const data = await response.json();
  return res.status(200).json(data);
}

export {handler as default};
