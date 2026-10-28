import { useState } from 'react';
import { useAuth0, withAuthenticationRequired } from '@auth0/auth0-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { CreateOrderInput } from '@myapp/shared';
import { ApiError, describeError, useApi, usePermissions } from './api';

interface Order {
  id: string;
  customerId: string;
  status: string;
  createdAt: string;
}

function Orders() {
  const api = useApi();
  const queryClient = useQueryClient();
  const { logout, user } = useAuth0();
  const [customerId, setCustomerId] = useState('c-1');
  const [error, setError] = useState<string>();

  const { can } = usePermissions();
  const canRead = can('read:orders');
  const canWrite = can('write:orders');

  const orders = useQuery({
    queryKey: ['orders'],
    queryFn: () => api<{ items: Order[] }>('/orders'),
    refetchInterval: 5_000,
    enabled: canRead,
    retry: (count, err) => !(err instanceof ApiError && (err.status === 401 || err.status === 403)) && count < 1,
  });
  const create = useMutation({
    mutationFn: (input: CreateOrderInput) => api('/orders', { method: 'POST', body: JSON.stringify(input) }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['orders'] }),
  });

  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    // Same Zod rules as the API.
    const parsed = CreateOrderInput.safeParse({ customerId, items: [{ sku: 'A-1', qty: 1 }] });
    if (!parsed.success) return setError(parsed.error.issues[0]?.message);
    setError(undefined);
    create.mutate(parsed.data);
  };

  return (
    <main style={{ maxWidth: 640, margin: '2rem auto', padding: '0 16px', fontFamily: 'system-ui' }}>
      <header style={{ display: 'flex', justifyContent: 'space-between' }}>
        <h1>Orders</h1>
        <button onClick={() => logout({ logoutParams: { returnTo: window.location.origin } })}>
          Sign out {user?.email}
        </button>
      </header>
      {canWrite ? (
        <form onSubmit={submit}>
          <input value={customerId} onChange={(e) => setCustomerId(e.target.value)} placeholder="customerId" />
          <button type="submit" disabled={create.isPending}>New order</button>
          {error && <p role="alert">{error}</p>}
          {create.error && <p role="alert">{describeError(create.error)}</p>}
        </form>
      ) : (
        <p>You can't create orders. Ask an admin for the write:orders permission.</p>
      )}
      {!canRead ? (
        <p role="alert">You don't have access to orders. Ask an admin for the read:orders permission.</p>
      ) : orders.error ? (
        <p role="alert">{describeError(orders.error)}</p>
      ) : (
        <ul>
          {orders.data?.items.map((o) => (
            <li key={o.id}>
              {o.id.slice(-6)} · {o.customerId} · <strong>{o.status}</strong>
            </li>
          ))}
        </ul>
      )}
    </main>
  );
}

export const App = withAuthenticationRequired(Orders);
