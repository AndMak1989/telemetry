import { BrowserAgent } from '@newrelic/browser-agent/loaders/browser-agent';
import { getWebInstrumentations, initializeFaro, ReactIntegration } from '@grafana/faro-react';
import { TracingInstrumentation } from '@grafana/faro-web-tracing';

const mode = import.meta.env.VITE_RUM ?? 'none';
// New Relic region endpoint: EU accounts use bam.eu01.nr-data.net.
const nrBeacon = import.meta.env.VITE_NR_BEACON ?? 'bam.nr-data.net';
const apiOrigin = new RegExp(import.meta.env.VITE_API_ORIGIN_REGEX ?? '^$');
// New Relic matches exact origins. In dev VITE_API_URL is "/api" (same origin, always allowed).
const apiOriginUrl = new URL(import.meta.env.VITE_API_URL, window.location.href).origin;

if (mode === 'newrelic' || mode === 'both') {
  const key = import.meta.env.VITE_NR_BROWSER_KEY;
  const appId = import.meta.env.VITE_NR_APPLICATION_ID;
  new BrowserAgent({
    init: {
      // Only one agent should inject trace headers, or they overwrite each other.
      // Cross-origin calls (CloudFront → execute-api) only get trace headers if listed here.
      distributed_tracing: {
        enabled: mode === 'newrelic',
        allowed_origins: [apiOriginUrl],
        cors_use_newrelic_header: true,
        cors_use_tracecontext_headers: true,
      },
      privacy: { cookies_enabled: true },
      ajax: { deny_list: [nrBeacon] },
    },
    info: { beacon: nrBeacon, errorBeacon: nrBeacon, licenseKey: key, applicationID: appId, sa: 1 },
    loader_config: {
      accountID: import.meta.env.VITE_NR_ACCOUNT_ID,
      trustKey: import.meta.env.VITE_NR_TRUST_KEY,
      agentID: import.meta.env.VITE_NR_AGENT_ID,
      licenseKey: key,
      applicationID: appId,
    },
  });
}

if (mode === 'grafana' || mode === 'both') {
  initializeFaro({
    url: import.meta.env.VITE_FARO_URL,
    app: { name: 'myapp-web', version: import.meta.env.VITE_APP_VERSION ?? 'dev', environment: import.meta.env.MODE },
    instrumentations: [
      ...getWebInstrumentations(),
      new TracingInstrumentation({ instrumentationOptions: { propagateTraceHeaderCorsUrls: [apiOrigin] } }),
      new ReactIntegration(),
    ],
  });
}
