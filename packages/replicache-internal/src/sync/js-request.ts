export function callJSRequest<Body, Return>(
  func: (req: Request) => Return,
  url: string,
  body: Body,
  auth: string,
  requestID: string,
): Return {
  const init = {
    headers: {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Content-type': 'application/json',
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Authorization': auth,
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'X-Replicache-RequestID': requestID,
    },
    body: JSON.stringify(body),
    method: 'POST',
  };

  const request = new Request(url, init);
  return func(request);
}
