import { getParameter } from '@aws-lambda-powertools/parameters/ssm';

export function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var ${name}`);
  return v;
}

/** Local: MONGO_URI. AWS: SecureString in SSM (cached by Powertools for 5 min). */
export async function getMongoUri(): Promise<string> {
  if (process.env.MONGO_URI) return process.env.MONGO_URI;
  const name = requireEnv('MONGO_URI_PARAM');
  const value = await getParameter(name, { decrypt: true, maxAge: 300 });
  if (!value) throw new Error(`SSM parameter ${name} is empty`);
  return value;
}
