import { useAuth0 } from '@auth0/auth0-react';
import { useQuery } from '@tanstack/react-query';
import { useCallback } from 'react';

export class ApiError extends Error {
  constructor(public status: number, message: string) { super(message); }
}

/** Message to show for a failed API call. */
export function describeError(err: unknown): string {
  if (err instanceof ApiError) {
    if (err.status === 403) return "You don't have permission to do this.";
    if (err.status === 401) return 'Your session expired. Sign in again.';
  }
  return err instanceof Error ? err.message : String(err);
}

/** Reads the `permissions` claim from a JWT payload. Unverified: for display only, the API enforces. */
function permissionsFromToken(token: string): Set<string> | undefined {
  try {
    const b64 = token.split('.')[1]!.replace(/-/g, '+').replace(/_/g, '/');
    const bytes = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
    const claims = JSON.parse(new TextDecoder().decode(bytes)) as { permissions?: unknown };
    return Array.isArray(claims.permissions) ? new Set(claims.permissions.map(String)) : undefined;
  } catch {
    return undefined; // opaque token or no claim: unknown, let the API decide
  }
}

/**
 * What the signed-in user may do, so the UI can hide actions up front.
 * While unknown (loading, or no `permissions` claim) `can()` is true and the API's 403 decides.
 */
export function usePermissions() {
  const { getAccessTokenSilently } = useAuth0();
  const { data } = useQuery({
    queryKey: ['permissions'],
    queryFn: async () => {
      const token = await getAccessTokenSilently();
      return (token && permissionsFromToken(token)) || null;
    },
    staleTime: 5 * 60_000,
  });
  return { can: (permission: string) => !data || data.has(permission) };
}

export function useApi() {
  const { getAccessTokenSilently } = useAuth0();
  return useCallback(
    async <T,>(path: string, init: RequestInit = {}): Promise<T> => {
      const token = await getAccessTokenSilently();
      const headers = new Headers(init.headers);
      headers.set('Authorization', `Bearer ${token}`);
      if (init.body) headers.set('Content-Type', 'application/json');
      const res = await fetch(`${import.meta.env.VITE_API_URL}${path}`, { ...init, headers });
      if (!res.ok) throw new ApiError(res.status, await res.text());
      return (res.status === 204 ? undefined : await res.json()) as T;
    },
    [getAccessTokenSilently],
  );
}
