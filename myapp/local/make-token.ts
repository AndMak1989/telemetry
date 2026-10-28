// pnpm token --sub user-1 --perms read:orders,write:orders
import { parseArgs } from 'node:util';
import { SignJWT } from 'jose';
import { LOCAL_ISSUER, loadKeys } from './local-auth';

const { values } = parseArgs({ options: { sub: { type: 'string', default: 'local|user-1' }, perms: { type: 'string', default: 'read:orders,write:orders' } } });
const { privateKey } = await loadKeys();
const token = await new SignJWT({ permissions: values.perms!.split(','), scope: 'openid profile' })
  .setProtectedHeader({ alg: 'RS256', kid: 'local-dev' })
  .setIssuer(LOCAL_ISSUER)
  .setAudience(process.env.AUTH0_AUDIENCE!)
  .setSubject(values.sub!)
  .setIssuedAt()
  .setExpirationTime('8h')
  .sign(privateKey);
console.log(token);
