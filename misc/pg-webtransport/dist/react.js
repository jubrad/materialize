import { jsx as _jsx } from "react/jsx-runtime";
// React hooks and context for pg-webtransport.
// Requires React 18+. Browser-only — WebTransport is not available in SSR.
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, } from "react";
import { Client } from "./client.js";
const MaterializeContext = createContext({
    client: null,
    error: null,
});
function createProvider(options) {
    function MaterializeProvider({ children }) {
        const [client, setClient] = useState(null);
        const [error, setError] = useState(null);
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
                .catch((err) => {
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
        const value = useMemo(() => ({ client, error }), [client, error]);
        return (_jsx(MaterializeContext.Provider, { value: value, children: children }));
    }
    MaterializeProvider.displayName = "MaterializeProvider";
    return MaterializeProvider;
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
export function createMaterializeConnection(options) {
    const Provider = createProvider(options);
    function useClient() {
        return useContext(MaterializeContext);
    }
    return { Provider, useClient };
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
export function useQuery(sql, options = {}) {
    const { refreshInterval, enabled = true } = options;
    const { client } = useContext(MaterializeContext);
    const [data, setData] = useState(null);
    const [fields, setFields] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [commandTag, setCommandTag] = useState(null);
    const [fetchTick, setFetchTick] = useState(0);
    const refetch = useCallback(() => {
        setFetchTick((n) => n + 1);
    }, []);
    useEffect(() => {
        if (!enabled || !client || !client.connected)
            return;
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
            }
            catch (err) {
                if (!cancelled) {
                    setError(err instanceof Error ? err : new Error(String(err)));
                }
            }
            finally {
                if (!cancelled) {
                    setLoading(false);
                }
            }
        };
        void run();
        let intervalId;
        if (refreshInterval !== undefined && refreshInterval > 0) {
            intervalId = setInterval(() => { void run(); }, refreshInterval);
        }
        return () => {
            cancelled = true;
            if (intervalId !== undefined)
                clearInterval(intervalId);
        };
    }, [sql, client, enabled, refreshInterval, fetchTick]); // eslint-disable-line react-hooks/exhaustive-deps
    return { data, fields, loading, error, commandTag, refetch };
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
export function useMutation(options = {}) {
    const { onSuccess, onError } = options;
    const { client } = useContext(MaterializeContext);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [data, setData] = useState(null);
    // Keep callbacks in a ref so the execute function is stable across renders
    const callbacksRef = useRef({ onSuccess, onError });
    callbacksRef.current = { onSuccess, onError };
    const execute = useCallback(async (sql, params = []) => {
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
        }
        catch (err) {
            const e = err instanceof Error ? err : new Error(String(err));
            setError(e);
            callbacksRef.current.onError?.(e);
            throw e;
        }
        finally {
            setLoading(false);
        }
    }, [client]);
    const reset = useCallback(() => {
        setData(null);
        setError(null);
    }, []);
    return { execute, loading, error, data, reset };
}
//# sourceMappingURL=react.js.map