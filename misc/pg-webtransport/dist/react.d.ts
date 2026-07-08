import { type ReactNode, type ComponentType } from "react";
import { Client, type ClientOptions, type QueryResult, type PgError } from "./client.js";
interface ProviderProps {
    children: ReactNode;
}
export interface MaterializeConnection {
    Provider: ComponentType<ProviderProps>;
    useClient: () => {
        client: Client | null;
        error: Error | null;
    };
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
export declare function createMaterializeConnection(options: ClientOptions): MaterializeConnection;
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
export declare function useQuery(sql: string, options?: UseQueryOptions): UseQueryResult;
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
export declare function useMutation(options?: UseMutationOptions): UseMutationResult;
export type { PgError };
//# sourceMappingURL=react.d.ts.map