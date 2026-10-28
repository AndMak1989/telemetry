import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { exportJWK, generateKeyPair, importJWK, type JWK } from 'jose';

const DIR = new URL('./.keys/', import.meta.url);
const FILE = new URL('dev-key.json', DIR);
export const LOCAL_ISSUER = 'https://local.dev/';

export async function loadKeys() {
  if (!existsSync(FILE)) {
    const { privateKey } = await generateKeyPair('RS256', { extractable: true });
    mkdirSync(DIR, { recursive: true });
    writeFileSync(FILE, JSON.stringify({ ...(await exportJWK(privateKey)), kid: 'local-dev', alg: 'RS256' }));
  }
  const priv = JSON.parse(readFileSync(FILE, 'utf8')) as JWK;
  const { d, p, q, dp, dq, qi, ...publicJwk } = priv; // strip private parts
  return { privateKey: await importJWK(priv, 'RS256'), publicJwk };
}
