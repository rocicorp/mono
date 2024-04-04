import { NextRequest, NextResponse } from "next/server";

// Force-redirect every HTTP request to HTTPS
function forceHTTPS(req: NextRequest) {
  if (
    process.env.NODE_ENV === "production" &&
    req.headers.get("x-forwarded-proto") !== "https" &&
    !req.headers.get("host")?.includes("localhost")
  ) {
    return NextResponse.redirect(
      `https://${req.headers.get("host")}${req.nextUrl.pathname}`,
      301
    );
  }
  return;
}

function processMiddlewareFunctions(
  req: NextRequest,
  middlewareFns: ((req: NextRequest) => NextResponse | undefined)[]
) {
  for (const middlewareFn of middlewareFns) {
    const fnResponse = middlewareFn(req);
    if (fnResponse) {
      return fnResponse;
    }
  }
  return NextResponse.next();
}

export function middleware(req: NextRequest) {
  return processMiddlewareFunctions(req, [forceHTTPS]);
}
