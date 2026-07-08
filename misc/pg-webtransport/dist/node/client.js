// pgwire Client over WebTransport.
// Each connect() call opens a WebTransport session; each query/execute call
// opens a new bidirectional stream that runs a full pgwire sub-session
// (startup → auth → ready → query → terminate).
import { MessageReader, encodeFrontendMessage, encodeCancelRequest, columnAsString, ProtocolError, } from "./codec.js";
import { scram256ClientFirst, scram256ClientFinal, scram256VerifyServerFinal, cleartextPasswordMessage, ScramError, } from "./auth.js";
export class PgError extends Error {
    code;
    fields;
    constructor(err) {
        super(err.message);
        this.name = "PgError";
        if (err.code !== undefined)
            this.code = err.code;
        this.fields = err.fields;
    }
}
/**
 * Run the startup + authentication exchange on a bidirectional stream.
 * Resolves when ReadyForQuery is received.
 *
 * Note: no SslRequest is sent — WebTransport is already encrypted.
 */
async function runStartup(writer, reader, params) {
    await writer.write(encodeFrontendMessage({
        type: "StartupMessage",
        user: params.user,
        database: params.database,
        params: {
            application_name: params.applicationName,
        },
    }));
    let scramState;
    let serverSignature;
    authLoop: while (true) {
        const msg = await reader.readMessage();
        switch (msg.type) {
            case "AuthenticationOk":
                break authLoop;
            case "AuthenticationCleartextPassword": {
                // For OIDC/SSO, the JWT token is passed as the cleartext password.
                const cleartextValue = params.token ?? params.password;
                if (!cleartextValue) {
                    throw new PgError({
                        type: "ErrorResponse",
                        message: "Password or token required for cleartext authentication",
                        fields: {},
                    });
                }
                await writer.write(encodeFrontendMessage(cleartextPasswordMessage(cleartextValue)));
                break;
            }
            case "AuthenticationMD5Password":
                // MD5 is not available in crypto.subtle.
                throw new Error("MD5 authentication is not supported in browser environments. " +
                    "Configure the server to use SCRAM-SHA-256.");
            case "AuthenticationSASL": {
                if (!msg.mechanisms.includes("SCRAM-SHA-256")) {
                    throw new Error(`No supported SASL mechanism. Server offered: ${msg.mechanisms.join(", ")}`);
                }
                const { message, state } = scram256ClientFirst(params.user);
                scramState = state;
                await writer.write(encodeFrontendMessage(message));
                break;
            }
            case "AuthenticationSASLContinue": {
                if (!scramState) {
                    throw new ScramError("Unexpected SASLContinue without prior SASLInitialResponse");
                }
                const result = await scram256ClientFinal(msg, params.password ?? "", scramState);
                serverSignature = result.serverSignature;
                await writer.write(encodeFrontendMessage(result.message));
                break;
            }
            case "AuthenticationSASLFinal": {
                if (!serverSignature)
                    throw new ScramError("Unexpected SASLFinal");
                scram256VerifyServerFinal(msg.data, serverSignature);
                // Next message will be AuthenticationOk
                break;
            }
            case "ParameterStatus":
            case "BackendKeyData":
            case "NoticeResponse":
                break; // ignore during auth (BackendKeyData captured in drain loop below)
            case "ReadyForQuery":
                return undefined; // some servers skip AuthOk and go straight to RFQ
            case "ErrorResponse":
                throw new PgError(msg);
            default:
                throw new ProtocolError(`Unexpected message during startup: ${msg.type}`);
        }
    }
    // Drain ParameterStatus / BackendKeyData until ReadyForQuery
    let cancelKey;
    while (true) {
        const msg = await reader.readMessage();
        if (msg.type === "ReadyForQuery")
            return cancelKey;
        if (msg.type === "ErrorResponse")
            throw new PgError(msg);
        if (msg.type === "BackendKeyData")
            cancelKey = { pid: msg.pid, secretKey: msg.secretKey };
        // ParameterStatus, NoticeResponse — ignore
    }
}
/**
 * Stream a SUBSCRIBE (or any indefinitely-streaming query).
 * Calls onRow for each DataRow until the signal is aborted or the server ends
 * the query. Closing the stream signals the server to stop.
 */
async function runSubscribeQuery(writer, reader, query, onRow, signal) {
    await writer.write(encodeFrontendMessage({ type: "Query", query }));
    let fields = [];
    const abortHandler = () => {
        // Close the write side to signal the server to stop.
        writer.close().catch(() => { });
    };
    signal.addEventListener("abort", abortHandler, { once: true });
    try {
        while (true) {
            const msg = await reader.readMessage();
            switch (msg.type) {
                case "RowDescription":
                    fields = msg.fields.map(fieldFromProto);
                    break;
                case "DataRow":
                    onRow(fields, msg.columns.map(columnAsString));
                    break;
                case "CommandComplete":
                case "ReadyForQuery":
                    return;
                case "ErrorResponse":
                    throw new PgError(msg);
                case "NoticeResponse":
                    break;
                default:
                    throw new ProtocolError(`Unexpected message during subscribe: ${msg.type}`);
            }
        }
    }
    finally {
        signal.removeEventListener("abort", abortHandler);
    }
}
/**
 * Run a simple query on an already-authenticated stream.
 */
async function runSimpleQuery(writer, reader, query) {
    await writer.write(encodeFrontendMessage({ type: "Query", query }));
    let fields = [];
    let rows = [];
    let commandTag = "";
    let pendingError;
    let sawReadyForQuery = false;
    try {
        while (!sawReadyForQuery) {
            const msg = await reader.readMessage();
            switch (msg.type) {
                case "RowDescription":
                    fields = msg.fields.map(fieldFromProto);
                    rows = [];
                    break;
                case "DataRow":
                    rows.push(msg.columns.map(columnAsString));
                    break;
                case "CommandComplete":
                    commandTag = msg.tag;
                    break;
                case "EmptyQueryResponse":
                    commandTag = "";
                    break;
                case "ReadyForQuery":
                    sawReadyForQuery = true;
                    break;
                case "ErrorResponse":
                    pendingError = new PgError(msg);
                    break;
                case "NoticeResponse":
                case "NotificationResponse":
                    break; // ignore
                default:
                    throw new ProtocolError(`Unexpected message during simple query: ${msg.type}`);
            }
        }
    }
    catch (streamErr) {
        // If the stream closed after an ErrorResponse (e.g. server closes on cancel
        // instead of sending ReadyForQuery), surface the PgError, not the stream error.
        if (pendingError)
            throw pendingError;
        throw streamErr;
    }
    if (pendingError)
        throw pendingError;
    return { fields, rows, commandTag };
}
/**
 * Run an extended query (Parse/Bind/Describe/Execute/Sync pipeline).
 * Parameters are passed as text-format values.
 */
async function runExtendedQuery(writer, reader, query, params) {
    const paramBytes = params.map((p) => {
        if (p === null)
            return null;
        return new TextEncoder().encode(p);
    });
    // Send the full pipeline at once
    await writer.write(encodeFrontendMessage({ type: "Parse", name: "", query, paramTypes: [] }));
    await writer.write(encodeFrontendMessage({
        type: "Bind",
        portal: "",
        statement: "",
        paramFormats: [],
        params: paramBytes,
        resultFormats: [],
    }));
    await writer.write(encodeFrontendMessage({ type: "Describe", which: "P", name: "" }));
    await writer.write(encodeFrontendMessage({ type: "Execute", portal: "", maxRows: 0 }));
    await writer.write(encodeFrontendMessage({ type: "Sync" }));
    let fields = [];
    let rows = [];
    let commandTag = "";
    let pendingError;
    let sawReadyForQuery = false;
    while (!sawReadyForQuery) {
        const msg = await reader.readMessage();
        switch (msg.type) {
            case "ParseComplete":
            case "BindComplete":
            case "NoData":
                break;
            case "RowDescription":
                fields = msg.fields.map(fieldFromProto);
                rows = [];
                break;
            case "DataRow":
                rows.push(msg.columns.map(columnAsString));
                break;
            case "CommandComplete":
                commandTag = msg.tag;
                break;
            case "EmptyQueryResponse":
                commandTag = "";
                break;
            case "ReadyForQuery":
                sawReadyForQuery = true;
                break;
            case "ErrorResponse":
                pendingError = new PgError(msg);
                break;
            case "NoticeResponse":
            case "NotificationResponse":
                break;
            default:
                throw new ProtocolError(`Unexpected message during extended query: ${msg.type}`);
        }
    }
    if (pendingError)
        throw pendingError;
    return { fields, rows, commandTag };
}
function fieldFromProto(f) {
    return {
        name: f.name,
        tableOid: f.tableOid,
        colAttrNum: f.colAttrNum,
        dataTypeOid: f.dataTypeOid,
        dataTypeSize: f.dataTypeSize,
        typeModifier: f.typeModifier,
        format: f.format,
    };
}
// ---------------------------------------------------------------------------
// Client class
// ---------------------------------------------------------------------------
export class Client {
    options;
    transport = null;
    _connected = false;
    _activeCancelKey;
    constructor(options) {
        this.options = options;
    }
    get connected() {
        return this._connected;
    }
    /**
     * Open the WebTransport session. Must be called before query() or execute().
     */
    async connect() {
        if (this._connected)
            return;
        const wtOptions = {};
        if (this.options.serverCertificateHashes !== undefined) {
            wtOptions.serverCertificateHashes = this.options.serverCertificateHashes;
        }
        this.transport = new WebTransport(this.options.url, wtOptions);
        if (this.options.signal) {
            const signal = this.options.signal;
            const onAbort = () => {
                this.transport?.close({ closeCode: 0, reason: "aborted" });
            };
            if (signal.aborted) {
                onAbort();
                return;
            }
            signal.addEventListener("abort", onAbort, { once: true });
        }
        await this.transport.ready;
        this._connected = true;
        // Auto-mark disconnected when the transport closes for any reason.
        this.transport.closed.catch(() => { }).finally(() => {
            this._connected = false;
            this.transport = null;
        });
    }
    /**
     * Run a simple SQL query. Opens a new bidirectional stream per call.
     */
    async query(sql) {
        return this.withSession((writer, reader) => runSimpleQuery(writer, reader, sql));
    }
    /**
     * Stream a SUBSCRIBE query, calling onRow for each incoming row.
     * Resolves when the signal is aborted or the server ends the stream.
     */
    async subscribe(sql, onRow, signal) {
        return this.withSession((writer, reader) => runSubscribeQuery(writer, reader, sql, onRow, signal));
    }
    /**
     * Run a parameterized query using the extended query protocol.
     * Parameters are passed as text strings.
     */
    async execute(sql, params = []) {
        return this.withSession((writer, reader) => runExtendedQuery(writer, reader, sql, params));
    }
    /**
     * Send a CancelRequest for the currently running query.
     * No-op if no query is in flight or the cancel key is unavailable.
     */
    async cancel() {
        const key = this._activeCancelKey;
        if (!key || !this.transport || !this._connected)
            return;
        // CancelRequest is sent on a brand-new stream with no prior startup.
        const stream = await this.transport.createBidirectionalStream();
        const writer = stream.writable.getWriter();
        try {
            await writer.write(encodeCancelRequest(key.pid, key.secretKey));
            await writer.close();
        }
        catch {
            // Ignore — the backend closes the stream immediately on receiving cancel
        }
    }
    /**
     * Listen for NOTIFY notifications on a channel.
     * Opens a dedicated session, sends LISTEN, and calls onNotification for each
     * NotificationResponse until the signal is aborted.
     */
    async listen(channel, onNotification, signal) {
        if (!this.transport || !this._connected) {
            throw new Error("Client is not connected. Call connect() first.");
        }
        const stream = await this.transport.createBidirectionalStream();
        const writer = stream.writable.getWriter();
        const reader = new MessageReader(stream.readable);
        const params = {
            user: this.options.user,
            password: this.options.password,
            token: this.options.token,
            database: this.options.database ?? this.options.user,
            applicationName: this.options.applicationName ?? "pg-webtransport",
        };
        try {
            await runStartup(writer, reader, params);
            // Send LISTEN command and wait for CommandComplete + ReadyForQuery
            await writer.write(encodeFrontendMessage({ type: "Query", query: `LISTEN ${channel}` }));
            let sawRfq = false;
            while (!sawRfq) {
                const msg = await reader.readMessage();
                if (msg.type === "ReadyForQuery")
                    sawRfq = true;
                else if (msg.type === "ErrorResponse")
                    throw new PgError(msg);
            }
            // Abort handler: cancel the reader to unblock the pending readMessage()
            const abortHandler = () => {
                reader.cancel();
                writer.close().catch(() => { });
            };
            signal.addEventListener("abort", abortHandler, { once: true });
            try {
                while (true) {
                    const msg = await reader.readMessage();
                    if (msg.type === "NotificationResponse") {
                        onNotification({ channel: msg.channel, payload: msg.payload });
                    }
                    else if (msg.type === "ErrorResponse") {
                        throw new PgError(msg);
                    }
                    // Ignore NoticeResponse and any other async messages
                }
            }
            catch (err) {
                if (signal.aborted)
                    return; // normal abort path
                throw err;
            }
            finally {
                signal.removeEventListener("abort", abortHandler);
            }
        }
        finally {
            reader.releaseLock();
            try {
                await writer.abort("closing");
            }
            catch { /* ignore */ }
        }
    }
    /**
     * Close the WebTransport session.
     */
    async close() {
        if (!this.transport)
            return;
        try {
            this.transport.close({ closeCode: 0, reason: "client close" });
            await this.transport.closed;
        }
        catch {
            // ignore errors during close
        }
        finally {
            this._connected = false;
            this.transport = null;
        }
    }
    /**
     * Open a bidirectional stream, run startup+auth, call fn, send Terminate, close stream.
     */
    async withSession(fn) {
        if (!this.transport || !this._connected) {
            throw new Error("Client is not connected. Call connect() first.");
        }
        const stream = await this.transport.createBidirectionalStream();
        const writer = stream.writable.getWriter();
        const reader = new MessageReader(stream.readable);
        const params = {
            user: this.options.user,
            password: this.options.password,
            token: this.options.token,
            database: this.options.database ?? this.options.user,
            applicationName: this.options.applicationName ?? "pg-webtransport",
        };
        try {
            this._activeCancelKey = await runStartup(writer, reader, params);
            const result = await fn(writer, reader);
            // Graceful terminate
            try {
                await writer.write(encodeFrontendMessage({ type: "Terminate" }));
                await writer.close();
            }
            catch {
                // stream may already be closing
            }
            return result;
        }
        catch (err) {
            try {
                await writer.abort("error");
            }
            catch {
                // ignore
            }
            throw err;
        }
        finally {
            this._activeCancelKey = undefined;
            reader.releaseLock();
        }
    }
}
//# sourceMappingURL=client.js.map