import type { APIGatewayProxyEventV2WithJWTAuthorizer } from 'aws-lambda';
import { HttpError } from './http';

export type ApiEvent = APIGatewayProxyEventV2WithJWTAuthorizer;

export function getUserId(event: ApiEvent): string {
  const sub = event.requestContext.authorizer?.jwt?.claims?.sub;
  if (typeof sub !== 'string' || !sub) throw new HttpError(401, 'Unauthorized');
  return sub;
}

/**
 * Auth0 RBAC puts permissions in a `permissions` array claim.
 * HTTP API's JWT authorizer flattens array claims into a string like "[read:orders write:orders]",
 * so handle both forms.
 */
export function getPermissions(event: ApiEvent): Set<string> {
  const raw = event.requestContext.authorizer?.jwt?.claims?.permissions;
  if (Array.isArray(raw)) return new Set(raw.map(String));
  if (typeof raw === 'string') {
    return new Set(raw.replace(/^\[|\]$/g, '').split(/[\s,]+/).filter(Boolean));
  }
  return new Set();
}

export function requirePermission(event: ApiEvent, permission: string): void {
  if (!getPermissions(event).has(permission)) throw new HttpError(403, `Missing permission: ${permission}`);
}
