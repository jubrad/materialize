import { type ErrorResponse } from "./codec.js";
export interface ClientOptions {
    /** WebTransport URL, e.g. "https://mz.example.com:6875/pgwire" */
    url: string;
    user: string;
    /**
     * Password for SCRAM-SHA-256 or cleartext authentication.
     * For OIDC/SSO, pass the JWT access token via `token` instead.
     */
    password?: string;
    /**
     * OIDC/SSO JWT access token. Materialize passes this through the cleartext
     * password authentication mechanism — the token is sent as the password when
     * the server requests cleartext authentication.
     */
    token?: string;
    database?: string;
    applicationName?: string;
    /** Abort signal to close the transport */
    signal?: AbortSignal;
    /**
     * Pin the server certificate by hash instead of CA chain verification.
     * Useful for self-signed or locally-trusted certs (e.g. mkcert).
     * Each entry: { algorithm: "sha-256", value: Uint8Array (32 bytes) }
     */
    serverCertificateHashes?: Array<{
        algorithm: string;
        value: Uint8Array<ArrayBuffer>;
    }>;
}
export interface Field {
    name: string;
    tableOid: number;
    colAttrNum: number;
    dataTypeOid: number;
    dataTypeSize: number;
    typeModifier: number;
    format: number;
}
export interface QueryResult {
    fields: Field[];
    rows: (string | null)[][];
    commandTag: string;
}
export declare class PgError extends Error {
    readonly code?: string;
    readonly fields: Record<string, string>;
    constructor(err: ErrorResponse);
}
export declare class Client {
    private readonly options;
    private transport;
    private _connected;
    private _activeCancelKey;
    constructor(options: ClientOptions);
    get connected(): boolean;
    /**
     * Open the WebTransport session. Must be called before query() or execute().
     */
    connect(): Promise<void>;
    /**
     * Run a simple SQL query. Opens a new bidirectional stream per call.
     */
    query(sql: string): Promise<QueryResult>;
    /**
     * Stream a SUBSCRIBE query, calling onRow for each incoming row.
     * Resolves when the signal is aborted or the server ends the stream.
     */
    subscribe(sql: string, onRow: (fields: Field[], row: (string | null)[]) => void, signal: AbortSignal): Promise<void>;
    /**
     * Run a parameterized query using the extended query protocol.
     * Parameters are passed as text strings.
     */
    execute(sql: string, params?: (string | null)[]): Promise<QueryResult>;
    /**
     * Send a CancelRequest for the currently running query.
     * No-op if no query is in flight or the cancel key is unavailable.
     */
    cancel(): Promise<void>;
    /**
     * Listen for NOTIFY notifications on a channel.
     * Opens a dedicated session, sends LISTEN, and calls onNotification for each
     * NotificationResponse until the signal is aborted.
     */
    listen(channel: string, onNotification: (notification: {
        channel: string;
        payload: string;
    }) => void, signal: AbortSignal): Promise<void>;
    /**
     * Close the WebTransport session.
     */
    close(): Promise<void>;
    /**
     * Open a bidirectional stream, run startup+auth, call fn, send Terminate, close stream.
     */
    private withSession;
}
//# sourceMappingURL=client.d.ts.map