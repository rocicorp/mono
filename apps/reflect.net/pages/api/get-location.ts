import {NextRequest, NextResponse} from 'next/server';

export const config = {
  runtime: 'edge',
};

function foo(req: NextRequest) {
  const country = (req.headers.get('x-vercel-ip-country') as string) ?? '';
  const city = (req.headers.get('x-vercel-ip-city') as string) ?? '';
  const region =
    (req.headers.get('x-vercel-ip-country-region') as string) ?? '';

  if (!country || !city || !region) {
    return NextResponse.json(null);
  }

  return NextResponse.json({
    country,
    city,
    region,
  });
}

export {foo as default};
