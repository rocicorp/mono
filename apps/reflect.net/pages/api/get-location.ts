import {NextRequest, NextResponse} from 'next/server';
import {cors} from '../../api-utils/cors.js';

export const config = {
  runtime: 'edge',
};

export default function getLocation(req: NextRequest) {
  const country = (req.headers.get('x-vercel-ip-country') as string) ?? '';
  const city = (req.headers.get('x-vercel-ip-city') as string) ?? '';
  const region =
    (req.headers.get('x-vercel-ip-country-region') as string) ?? '';

  const result =
    !country || !city || !region
      ? null
      : {
          country,
          city,
          region,
        };

  return cors(req, NextResponse.json(result));
}
