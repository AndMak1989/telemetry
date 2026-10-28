export const API_ROUTES = [
  { method: 'GET', path: '/orders', fn: 'orders-api' },
  { method: 'POST', path: '/orders', fn: 'orders-api' },
  { method: 'GET', path: '/orders/{id}', fn: 'orders-api' },
] as const;

export type ApiFunctionName = (typeof API_ROUTES)[number]['fn'];
