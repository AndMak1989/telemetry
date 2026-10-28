// Starts local emulators under `tsx watch`, with prefixed output. Usage: node local/run.mjs <api|workers>
// Not `concurrently` for these: it hands each child an open stdin pipe, and with tracing enabled the
// processes then start extremely slowly or not at all. Here stdin is ignored.
//
// With NEW_RELIC_LICENSE_KEY set, every output line is also sent to New Relic's Log API. Powertools lines are
// JSON and carry trace.id / span.id, so the Logs page links each line to its trace.
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { createRequire } from 'node:module';

try {
  process.loadEnvFile('.env.local'); // the children get env through --env-file; the launcher needs the key too
} catch {
  /* no .env.local: forwarding stays off */
}

const tsx = createRequire(import.meta.url).resolve('tsx/cli');
const groups = {
  api: [['api', 'myapp-api', 'local/dev-server.ts', [], '\x1b[34m']],
  workers: [
    ['orders', 'myapp-orders-worker', 'local/queue-runner.ts', ['orders-worker'], '\x1b[35m'],
    ['notify', 'myapp-notifications-worker', 'local/queue-runner.ts', ['notifications-worker'], '\x1b[33m'],
    ['sweeper', 'myapp-outbox-sweeper', 'local/queue-runner.ts', ['outbox-sweeper'], '\x1b[36m'],
  ],
};

const group = groups[process.argv[2]];
if (!group) {
  console.error('usage: node local/run.mjs <api|workers>');
  process.exit(1);
}

// --- New Relic log forwarding ------------------------------------------------------------------------
const nrKey = process.env.NEW_RELIC_LICENSE_KEY;
const eu = (process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? '').includes('eu01');
const logApi = eu ? 'https://log-api.eu.newrelic.com/log/v1' : 'https://log-api.newrelic.com/log/v1';
const ANSI = /\x1b\[[0-9;]*m/g;
let pending = [];
let warned = false;

function toLogEntry(service, raw) {
  const line = raw.replace(ANSI, '').trim();
  if (!line) return undefined;
  try {
    const { message, timestamp, ...attributes } = JSON.parse(line);
    return { timestamp: Date.parse(timestamp) || Date.now(), message: String(message ?? line), attributes: { ...attributes, 'service.name': service } };
  } catch {
    return { timestamp: Date.now(), message: line, attributes: { 'service.name': service } };
  }
}

async function flush() {
  if (!pending.length) return;
  const batch = pending;
  pending = [];
  try {
    const res = await fetch(logApi, {
      method: 'POST',
      headers: { 'Api-Key': nrKey, 'Content-Type': 'application/json' },
      body: JSON.stringify([{ common: { attributes: { 'deployment.environment': 'local' } }, logs: batch }]),
    });
    if (!res.ok && !warned) {
      warned = true;
      console.error(`[log-forwarder] New Relic Log API answered ${res.status}; further failures are silent`);
    }
  } catch (err) {
    if (!warned) {
      warned = true;
      console.error(`[log-forwarder] cannot reach New Relic Log API: ${err.message}; further failures are silent`);
    }
  }
}

const forwarding = Boolean(nrKey);
const timer = forwarding ? setInterval(flush, 2000) : undefined;
if (forwarding) console.log(`▶ forwarding logs to ${logApi}`);

// --- processes ---------------------------------------------------------------------------------------
const children = group.map(([label, service, script, args, color]) => {
  const child = spawn(
    process.execPath,
    [tsx, 'watch', '--env-file=.env.local', '--env-file=.env.generated', script, ...args],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  );
  for (const stream of [child.stdout, child.stderr]) {
    createInterface({ input: stream }).on('line', (line) => {
      console.log(`${color}[${label}]\x1b[0m ${line}`);
      if (!forwarding) return;
      const entry = toLogEntry(service, line);
      if (entry) pending.push(entry);
      if (pending.length >= 100) void flush();
    });
  }
  return child;
});

const stop = () => {
  clearInterval(timer);
  children.forEach((c) => c.kill());
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
process.on('exit', stop);
