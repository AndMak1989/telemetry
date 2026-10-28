import { describe, expect, it } from 'vitest';
import { getPermissions, getUserId, requirePermission } from '../src/lib/auth';
import { HttpError, parseBody } from '../src/lib/http';
import { apiEvent } from './helpers';

describe('getPermissions', () => {
  it.each([
    ['the flattened string from the HTTP API authorizer', '[read:orders write:orders]', ['read:orders', 'write:orders']],
    ['a comma-separated string', '[read:orders,write:orders]', ['read:orders', 'write:orders']],
    ['a single permission', '[read:orders]', ['read:orders']],
    ['an empty list', '[]', []],
    ['a real array claim', ['read:orders', 'write:orders'], ['read:orders', 'write:orders']],
  ])('reads %s', (_, claim, expected) => {
    expect([...getPermissions(apiEvent('GET /orders', { permissions: claim }))].sort()).toEqual(expected);
  });

  it('treats a missing claim as no permissions', () => {
    const event = apiEvent('GET /orders');
    delete (event.requestContext.authorizer.jwt.claims as Record<string, unknown>).permissions;
    expect(getPermissions(event).size).toBe(0);
  });

  it('does not match on prefixes', () => {
    const event = apiEvent('GET /orders', { permissions: '[read:orders-archive]' });
    expect(() => requirePermission(event, 'read:orders')).toThrow(HttpError);
  });
});

describe('getUserId', () => {
  it('returns the subject', () => {
    expect(getUserId(apiEvent('GET /orders', { sub: 'auth0|abc' }))).toBe('auth0|abc');
  });

  it.each([null, ''])('throws 401 for sub=%j', (sub) => {
    expect(() => getUserId(apiEvent('GET /orders', { sub }))).toThrow(expect.objectContaining({ status: 401 }));
  });
});

describe('parseBody', () => {
  it('parses plain and base64 JSON', () => {
    expect(parseBody(apiEvent('POST /orders', { body: { a: 1 } }))).toEqual({ a: 1 });
    expect(parseBody(apiEvent('POST /orders', { body: { a: 1 }, base64: true }))).toEqual({ a: 1 });
  });

  it('returns {} for no body, so validation reports the missing fields', () => {
    expect(parseBody(apiEvent('POST /orders'))).toEqual({});
  });

  it('throws 400 for invalid JSON', () => {
    expect(() => parseBody(apiEvent('POST /orders', { rawBody: '{' }))).toThrow(expect.objectContaining({ status: 400 }));
  });
});
