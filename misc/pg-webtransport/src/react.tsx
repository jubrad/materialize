// React hooks and context for pg-webtransport.
// Requires React 18+. Browser-only — WebTransport is not available in SSR.

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
  type ComponentType,
} from "react";
import { Client, type ClientOptions, type QueryResult, type PgError } from "./client.js";

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

interface MaterializeContextValue {
  client: Client | null;
  error: Error | null;
}

const MaterializeContext = createContext<MaterializeContextValue>({
  client: null,
  error: null,
});

// ---------------------------------------------------------------------------
// Provider component
// ---------------------------------------------------------------------------

interface ProviderProps {
  children: ReactNode;
}

function createProvider(options: ClientOptions): ComponentType<ProviderProps> {
  function MaterializeProvider({ children }: ProviderProps) {
    const [client, setClient] = useState<Client | null>(null);
    const [error, setError] = useState<Error | null>(null);

    useEffect(() => {
      const ac = new AbortController();
      const c = new Client({ ...options, signal: ac.signal });

      c.connect()
        .then(() => {
          if (!ac.signal.aborted) {
            setClient(c);
            setError(null);
          }
        })
        .catch((err: unknown) => {
          if (!ac.signal.aborted) {
            setError(err instanceof Error ? err : new Error(String(err)));
          }
        });

      return () => {
        ac.abort();
        void c.close();
        setClient(null);
      };
    }, []); // eslint-disable-line react-hooks/exhaustive-deps

    const value = useMemo<MaterializeContextValue>(() => ({ client, error }), [client, error]);

    return (
      <MaterializeContext.Provider value={value}>
        {children}
      </MaterializeContext.Provider>
    );
  }

  MaterializeProvider.displayName = "MaterializeProvider";
  return MaterializeProvider;
}

// ---------------------------------------------------------------------------
// createMaterializeConnection
// ---------------------------------------------------------------------------

export interface MaterializeConnection {
  Provider: ComponentType<ProviderProps>;
  useClient: () => { client: Client | null; error: Error | null };
}

/**
 * Create a Materialize connection configuration.
 *
 * @example
 * ```tsx
 * const mz = createMaterializeConnection({
 *   url: "https://localhost:6875/pgwire",
 *   user: "materialize",
 *   password: "secret",
 * });
 *
 * function App() {
 *   return (
 *     <mz.Provider>
 *       <MyComponent />
 *     </mz.Provider>
 *   );
 * }
 *
 * function MyComponent() {
 *   const { data, loading, error } = useQuery("SELECT 1");
 * }
 * ```
 */
export function createMaterializeConnection(options: ClientOptions): MaterializeConnection {
  const Provider = createProvider(options);

  function useClient() {
    return useContext(MaterializeContext);
  }

  return { Provider, useClient };
}

// ---------------------------------------------------------------------------
// useQuery
// ---------------------------------------------------------------------------

export interface UseQueryOptions {
  /** Polling interval in milliseconds. If not set, query runs once. */
  refreshInterval?: number;
  /** If false, the query will not run. Default: true. */
  enabled?: boolean;
}

export interface UseQueryResult {
  data: (string | null)[][] | null;
  fields: QueryResult["fields"] | null;
  loading: boolean;
  error: Error | null;
  commandTag: string | null;
  refetch: () => void;
}

/**
 * Run a SQL query and return the results.
 *
 * @example
 * ```tsx
 * const { data, loading, error } = useQuery("SELECT id, name FROM items", {
 *   refreshInterval: 5000,
 * });
 * ```
 */
export function useQuery(
  sql: string,
  options: UseQueryOptions = {},
): UseQueryResult {
  const { refreshInterval, enabled = true } = options;
  const { client } = useContext(MaterializeContext);

  const [data, setData] = useState<(string | null)[][] | null>(null);
  const [fields, setFields] = useState<QueryResult["fields"] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [commandTag, setCommandTag] = useState<string | null>(null);
  const [fetchTick, setFetchTick] = useState(0);

  const refetch = useCallback(() => {
    setFetchTick((n) => n + 1);
  }, []);

  useEffect(() => {
    if (!enabled || !client || !client.connected) return;

    let cancelled = false;

    const run = async () => {
      setLoading(true);
      try {
        const result = await client.query(sql);
        if (!cancelled) {
          setData(result.rows);
          setFields(result.fields);
          setCommandTag(result.commandTag);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err : new Error(String(err)));
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    void run();

    let intervalId: ReturnType<typeof setInterval> | undefined;
    if (refreshInterval !== undefined && refreshInterval > 0) {
      intervalId = setInterval(() => { void run(); }, refreshInterval);
    }

    return () => {
      cancelled = true;
      if (intervalId !== undefined) clearInterval(intervalId);
    };
  }, [sql, client, enabled, refreshInterval, fetchTick]); // eslint-disable-line react-hooks/exhaustive-deps

  return { data, fields, loading, error, commandTag, refetch };
}

// ---------------------------------------------------------------------------
// useMutation
// ---------------------------------------------------------------------------

export interface UseMutationOptions {
  onSuccess?: (result: QueryResult) => void;
  onError?: (err: Error) => void;
}

export interface UseMutationResult {
  execute: (sql: string, params?: (string | null)[]) => Promise<QueryResult>;
  loading: boolean;
  error: Error | null;
  data: QueryResult | null;
  reset: () => void;
}

/**
 * Execute a SQL command (INSERT, UPDATE, DELETE, etc.) on demand.
 *
 * @example
 * ```tsx
 * const { execute, loading, error } = useMutation({
 *   onSuccess: (result) => console.log("Done:", result.commandTag),
 * });
 *
 * async function handleClick() {
 *   await execute("INSERT INTO items (name) VALUES ($1)", ["foo"]);
 * }
 * ```
 */
export function useMutation(options: UseMutationOptions = {}): UseMutationResult {
  const { onSuccess, onError } = options;
  const { client } = useContext(MaterializeContext);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [data, setData] = useState<QueryResult | null>(null);

  // Keep callbacks in a ref so the execute function is stable across renders
  const callbacksRef = useRef({ onSuccess, onError });
  callbacksRef.current = { onSuccess, onError };

  const execute = useCallback(
    async (sql: string, params: (string | null)[] = []): Promise<QueryResult> => {
      if (!client) {
        const err = new Error("Not connected to Materialize");
        setError(err);
        throw err;
      }

      setLoading(true);
      setError(null);

      try {
        const result = await client.execute(sql, params);
        setData(result);
        callbacksRef.current.onSuccess?.(result);
        return result;
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        setError(e);
        callbacksRef.current.onError?.(e);
        throw e;
      } finally {
        setLoading(false);
      }
    },
    [client],
  );

  const reset = useCallback(() => {
    setData(null);
    setError(null);
  }, []);

  return { execute, loading, error, data, reset };
}

export type { PgError };
