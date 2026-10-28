import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { randomUUID } from 'node:crypto';
import { createLocalJWKSet, createRemoteJWKSet, jwtVerify, type JWTPayload } from 'jose';
import type { APIGatewayProxyEventV2WithJWTAuthorizer, APIGatewayProxyStructuredResultV2, Context } from 'aws-lambda';
import { API_ROUTES, type ApiFunctionName } from '../services/src/routes';
import { LOCAL_ISSUER, loadKeys } from './local-auth';
import { fakeContext } from './fake-context';

// OTel must start before anything that it patches (http, mongodb, AWS SDK) is loaded, so the app is imported
// dynamically. A static import would not wait for otel's top-level await. (No-op without NEW_RELIC_LICENSE_KEY.)
await import('./otel');
const { handler: ordersApi } = await import('../services/src/functions/orders-api');

type Handler = (e: APIGatewayProxyEventV2WithJWTAuthorizer, c: Context) => Promise<APIGatewayProxyStructuredResultV2>;
const HANDLERS: Record<ApiFunctionName, Handler> = { 'orders-api': ordersApi };

const local = process.env.AUTH_MODE === 'local';
const ISSUER = local ? LOCAL_ISSUER : `https://${process.env.AUTH0_DOMAIN}/`;
const JWKS = local
  ? createLocalJWKSet({ keys: [(await loadKeys()).publicJwk] })
  : createRemoteJWKSet(new URL(`${ISSUER}.well-known/jwks.json`));

const compiled = API_ROUTES.map((r) => {
  const keys: string[] = [];
  const re = new RegExp('^' + r.path.replace(/\{(\w+)\}/g, (_, k: string) => (keys.push(k), '([^/]+)')) + '$');
  return { ...r, re, keys };
});

// Mimic API Gateway: every claim becomes a string. Arrays → "[a b]", objects → JSON, the rest → String(v).
const flatten = (p: JWTPayload) =>
  Object.fromEntries(Object.entries(p).map(([k, v]) => [k, Array.isArray(v) ? `[${v.join(' ')}]` : v !== null && typeof v === 'object' ? JSON.stringify(v) : String(v)]));

const readBody = (req: IncomingMessage) =>
  new Promise<string>((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c)).on('end', () => resolve(Buffer.concat(chunks).toString('utf8'))).on('error', reject);
  });

const send = (res: ServerResponse, r: APIGatewayProxyStructuredResultV2) => {
  res.writeHead(r.statusCode ?? 200, { 'content-type': 'application/json', ...(r.headers as Record<string, string>) });
  res.end(r.body ?? '');
};

createServer(async (req, res) => {
  try {
    const method = req.method ?? 'GET';
    const url = new URL(req.url ?? '/', 'http://localhost');
    let params: Record<string, string> = {};
    const route = compiled.find((r) => {
      const m = r.method === method && r.re.exec(url.pathname);
      if (m) params = Object.fromEntries(r.keys.map((k, i) => [k, decodeURIComponent(m[i + 1]!)]));
      return Boolean(m);
    });
    if (!route) return send(res, { statusCode: 404, body: '{"message":"Not Found"}' });

    const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
    let payload: JWTPayload;
    try {
      ({ payload } = await jwtVerify(token ?? '', JWKS, { issuer: ISSUER, audience: process.env.AUTH0_AUDIENCE, algorithms: ['RS256'] }));
    } catch {
      return send(res, { statusCode: 401, body: '{"message":"Unauthorized"}' });
    }

    const body = await readBody(req);
    const routeKey = `${method} ${route.path}`;
    const event: APIGatewayProxyEventV2WithJWTAuthorizer = {
      version: '2.0',
      routeKey,
      rawPath: url.pathname,
      rawQueryString: url.searchParams.toString(),
      headers: Object.fromEntries(Object.entries(req.headers).map(([k, v]) => [k, Array.isArray(v) ? v.join(',') : v])),
      queryStringParameters: url.searchParams.size ? Object.fromEntries(url.searchParams) : undefined,
      pathParameters: Object.keys(params).length ? params : undefined,
      body: body || undefined,
      isBase64Encoded: false,
      requestContext: {
        accountId: '000000000000', apiId: 'local', domainName: 'localhost', domainPrefix: 'localhost',
        http: { method, path: url.pathname, protocol: 'HTTP/1.1', sourceIp: '127.0.0.1', userAgent: req.headers['user-agent'] ?? '' },
        requestId: randomUUID(), routeKey, stage: '$default', time: new Date().toISOString(), timeEpoch: Date.now(),
        authorizer: {
          principalId: '', integrationLatency: 0,
          jwt: { claims: flatten(payload), scopes: typeof payload.scope === 'string' ? payload.scope.split(' ') : [] },
        },
      },
    };
    send(res, await HANDLERS[route.fn](event, fakeContext(route.fn, 10_000)));
  } catch (err) {
    console.error(err);
    send(res, { statusCode: 500, body: '{"message":"dev-server error"}' });
  }
}).listen(Number(process.env.LOCAL_API_PORT ?? 3001), () => console.log('Local API on :3001'));
