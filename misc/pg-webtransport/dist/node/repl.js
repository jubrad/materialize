#!/usr/bin/env node
// mzwt — psql-style REPL over WebTransport for Materialize.
//
// Requires Node.js >= 22 (WebTransport global, readline/promises).
//
// Usage:
//   mzwt [--url URL] [--user USER] [--password PW | --token JWT]
//        [--database DB] [--command SQL]
//
// Examples:
//   mzwt --url https://mz.example.com:6875/pgwire --user alice
//   mzwt -U alice -c "SELECT 1"
import * as rl from "node:readline/promises";
import * as fs from "node:fs/promises";
import * as crypto from "node:crypto";
import { Client, PgError } from "./client.js";
// ---------------------------------------------------------------------------
// WebTransport availability — use built-in (Node >=22) or polyfill.
// ---------------------------------------------------------------------------
async function ensureWebTransport() {
    if (typeof globalThis["WebTransport"] !== "undefined") {
        return;
    }
    try {
        const { WebTransport, quicheLoaded } = await import("@fails-components/webtransport");
        await quicheLoaded;
        globalThis["WebTransport"] = WebTransport;
    }
    catch {
        process.stderr.write("Error: WebTransport is not available.\n" +
            "       Install the polyfill: npm install @fails-components/webtransport @fails-components/webtransport-transport-http3-quiche\n");
        process.exit(1);
    }
}
function parseArgs(argv) {
    const args = {
        url: process.env["MZWT_URL"] ?? "https://localhost:6875/pgwire",
        user: process.env["MZWT_USER"] ?? process.env["USER"] ?? "materialize",
        noTlsVerify: false,
        help: false,
    };
    for (let i = 2; i < argv.length; i++) {
        const arg = argv[i];
        const next = argv[i + 1] ?? "";
        switch (arg) {
            case "--url":
                args.url = next;
                i++;
                break;
            case "--user":
            case "-U":
                args.user = next;
                i++;
                break;
            case "--password":
            case "-P":
                args.password = next;
                i++;
                break;
            case "--token":
                args.token = next;
                i++;
                break;
            case "--database":
            case "-d":
                args.database = next;
                i++;
                break;
            case "--command":
            case "-c":
                args.command = next;
                i++;
                break;
            case "--server-cert":
                args.serverCert = next;
                i++;
                break;
            case "--no-tls-verify":
                args.noTlsVerify = true;
                break;
            case "--help":
            case "-h":
                args.help = true;
                break;
            default:
                if (!arg.startsWith("-")) {
                    // Positional: treat as database name (like psql)
                    args.database = arg;
                }
                break;
        }
    }
    return args;
}
const HELP = `\
Usage: mzwt [OPTIONS] [DATABASE]

Connect to Materialize via WebTransport and run an interactive SQL REPL.

Options:
  --url URL              WebTransport endpoint (default: https://localhost:6875/pgwire)
                         Also read from \$MZWT_URL
  -U, --user USER        Database user (default: \$USER)
                         Also read from \$MZWT_USER
  -P, --password PASS    Password for authentication
  --token JWT            OIDC/SSO JWT token (alternative to password)
  -d, --database DB      Database name (default: same as user)
  -c, --command SQL      Execute SQL and exit (non-interactive)
  --server-cert FILE     Trust this PEM certificate (pins its SHA-256 hash)
  --no-tls-verify        Skip TLS certificate verification (dev only!)
  -h, --help             Show this help

In the REPL:
  Type SQL terminated with ; to execute.
  \\q, \\quit             Quit
  \\timing               Toggle query timing
  \\?                    Show REPL commands
`;
/**
 * Read a PEM certificate file and return its SHA-256 hash over the DER bytes.
 * Suitable for passing as serverCertificateHashes to WebTransport.
 */
async function certFileToHash(pemPath) {
    const pem = await fs.readFile(pemPath, "utf8");
    // Extract the first certificate block
    const match = /-----BEGIN CERTIFICATE-----\r?\n([\s\S]+?)\r?\n-----END CERTIFICATE-----/.exec(pem);
    if (!match)
        throw new Error(`No certificate found in ${pemPath}`);
    const der = Buffer.from(match[1].replace(/\s+/g, ""), "base64");
    const digest = crypto.createHash("sha256").update(der).digest();
    // Copy into a plain ArrayBuffer so the type satisfies WebTransportHash.value
    const ab = new ArrayBuffer(digest.length);
    new Uint8Array(ab).set(digest);
    return new Uint8Array(ab);
}
// ---------------------------------------------------------------------------
// Table rendering (psql-inspired)
// ---------------------------------------------------------------------------
function renderTable(result) {
    const { fields, rows, commandTag } = result;
    const lines = [];
    if (fields.length === 0) {
        if (commandTag)
            lines.push(commandTag);
        return lines.join("\n");
    }
    const headers = fields.map((f) => f.name);
    // Column widths: at least header width, at least value width
    const widths = headers.map((h) => h.length);
    for (const row of rows) {
        for (let i = 0; i < row.length; i++) {
            widths[i] = Math.max(widths[i], (row[i] ?? "(null)").length);
        }
    }
    const cell = (s, w) => " " + s.padEnd(w) + " ";
    const divider = widths.map((w) => "-".repeat(w + 2)).join("+");
    // Header
    lines.push(headers.map((h, i) => cell(h, widths[i])).join("|"));
    lines.push(divider);
    // Rows
    for (const row of rows) {
        lines.push(row.map((v, i) => cell(v ?? "(null)", widths[i])).join("|"));
    }
    // Row count
    lines.push("");
    const n = rows.length;
    lines.push(n === 1 ? "(1 row)" : `(${n} rows)`);
    return lines.join("\n");
}
// ---------------------------------------------------------------------------
// Streaming SUBSCRIBE renderer
// ---------------------------------------------------------------------------
function makeSubscribeRenderer(fields) {
    const headers = fields.map((f) => f.name);
    // Fixed column widths from header names (rows may exceed these — that's fine
    // for a streaming display where we can't know widths in advance).
    const widths = headers.map((h) => h.length);
    const cell = (s, w) => " " + s.padEnd(w) + " ";
    const divider = widths.map((w) => "-".repeat(w + 2)).join("+");
    const header = headers.map((h, i) => cell(h, widths[i])).join("|");
    // Return the header+divider as the first "row", then row formatter
    let first = true;
    return (row) => {
        const line = row.map((v, i) => cell(v ?? "(null)", widths[i] ?? 4)).join("|");
        if (first) {
            first = false;
            return `${header}\n${divider}\n${line}`;
        }
        return line;
    };
}
// ---------------------------------------------------------------------------
// REPL
// ---------------------------------------------------------------------------
async function runRepl(client) {
    const iface = rl.createInterface({
        input: process.stdin,
        output: process.stdout,
        terminal: true,
    });
    let buffer = "";
    let timing = false;
    process.stdout.write('Type SQL statements terminated by ";" or \\q to quit.\n\n');
    const prompt = () => (buffer.trimEnd() ? "mz-c> " : "mz> ");
    iface.on("close", () => {
        process.stdout.write("\n");
        process.exit(0);
    });
    // eslint-disable-next-line no-constant-condition
    while (true) {
        let line;
        try {
            line = await iface.question(prompt());
        }
        catch {
            break; // EOF / Ctrl-D
        }
        // --- Meta-commands (only when not mid-statement) ---
        if (buffer === "") {
            switch (line.trim()) {
                case "":
                    continue;
                case "\\q":
                case "\\quit":
                    iface.close();
                    return;
                case "\\?":
                case "\\help":
                    process.stdout.write("\n  \\q, \\quit      quit\n" +
                        "  \\timing        toggle query execution timing\n" +
                        "  \\?             this help\n\n");
                    continue;
                case "\\timing":
                    timing = !timing;
                    process.stdout.write(`Timing is ${timing ? "on" : "off"}.\n`);
                    continue;
            }
        }
        buffer += (buffer ? "\n" : "") + line;
        // Execute when the accumulated buffer ends with a semicolon
        if (!buffer.trimEnd().endsWith(";"))
            continue;
        const sql = buffer.trim();
        buffer = "";
        const t0 = performance.now();
        const isSubscribe = /^\s*subscribe\b/i.test(sql);
        const isListen = /^\s*listen\b/i.test(sql);
        try {
            if (!client.connected) {
                process.stderr.write("Reconnecting...\n");
                await client.connect();
            }
            if (isSubscribe) {
                // Streaming path: print rows as they arrive, Ctrl+C to cancel.
                const controller = new AbortController();
                let renderer = null;
                const sigintHandler = () => {
                    controller.abort();
                };
                process.once("SIGINT", sigintHandler);
                process.stdout.write("(Press Ctrl+C to cancel)\n");
                try {
                    await client.subscribe(sql, (fields, row) => {
                        if (!renderer)
                            renderer = makeSubscribeRenderer(fields);
                        process.stdout.write(renderer(row) + "\n");
                    }, controller.signal);
                }
                finally {
                    process.removeListener("SIGINT", sigintHandler);
                    if (controller.signal.aborted) {
                        process.stdout.write("\nSUBSCRIBE cancelled.\n");
                    }
                }
                const ms = performance.now() - t0;
                if (timing)
                    process.stdout.write(`Time: ${ms.toFixed(3)} ms\n`);
                continue;
            }
            if (isListen) {
                // Extract channel name: LISTEN <channel>;
                const m = /^\s*listen\s+("(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)\s*;?\s*$/i.exec(sql);
                if (!m) {
                    process.stderr.write(`ERROR:  Could not parse LISTEN channel from: ${sql}\n`);
                    continue;
                }
                // Strip surrounding quotes if present
                const rawChannel = m[1];
                const channel = rawChannel.startsWith('"')
                    ? rawChannel.slice(1, -1).replace(/""/g, '"')
                    : rawChannel.toLowerCase();
                const controller = new AbortController();
                const sigintHandler = () => { controller.abort(); };
                process.once("SIGINT", sigintHandler);
                process.stdout.write(`Listening on channel "${channel}". Press Ctrl+C to stop.\n`);
                try {
                    await client.listen(channel, (n) => {
                        process.stdout.write(`Notification on "${n.channel}": ${n.payload || "(empty)"}\n`);
                    }, controller.signal);
                }
                finally {
                    process.removeListener("SIGINT", sigintHandler);
                    if (controller.signal.aborted) {
                        process.stdout.write("\nLISTEN cancelled.\n");
                    }
                }
                const ms = performance.now() - t0;
                if (timing)
                    process.stdout.write(`Time: ${ms.toFixed(3)} ms\n`);
                continue;
            }
            // Regular query — SIGINT sends a CancelRequest to interrupt on the server.
            const sigintCancel = () => { void client.cancel().catch(() => { }); };
            process.once("SIGINT", sigintCancel);
            let result;
            try {
                result = await client.query(sql);
            }
            finally {
                process.removeListener("SIGINT", sigintCancel);
            }
            const ms = performance.now() - t0;
            process.stdout.write(renderTable(result) + "\n");
            if (timing)
                process.stdout.write(`Time: ${ms.toFixed(3)} ms\n`);
        }
        catch (err) {
            const ms = performance.now() - t0;
            // If the transport died mid-query, reconnect and retry once.
            if (!client.connected && !(err instanceof PgError)) {
                process.stderr.write("Connection lost. Reconnecting...\n");
                try {
                    await client.connect();
                    const result = await client.query(sql);
                    process.stdout.write(renderTable(result) + "\n");
                    if (timing)
                        process.stdout.write(`Time: ${ms.toFixed(3)} ms\n`);
                    continue;
                }
                catch (retryErr) {
                    process.stderr.write(`ERROR:  ${retryErr instanceof Error ? retryErr.message : String(retryErr)}\n`);
                    if (timing)
                        process.stdout.write(`Time: ${ms.toFixed(3)} ms\n`);
                    continue;
                }
            }
            if (err instanceof PgError) {
                process.stderr.write(`ERROR:  ${err.message}\n`);
                if (err.code)
                    process.stderr.write(`DETAIL: SQLSTATE ${err.code}\n`);
            }
            else {
                process.stderr.write(`ERROR:  ${err instanceof Error ? err.message : String(err)}\n`);
            }
            if (timing)
                process.stdout.write(`Time: ${ms.toFixed(3)} ms\n`);
        }
    }
    iface.close();
}
// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
async function main() {
    const args = parseArgs(process.argv);
    if (args.help) {
        process.stdout.write(HELP);
        process.exit(0);
    }
    await ensureWebTransport();
    let serverCertificateHashes;
    if (args.serverCert !== undefined) {
        const hash = await certFileToHash(args.serverCert);
        serverCertificateHashes = [{ algorithm: "sha-256", value: hash }];
    }
    const client = new Client({
        url: args.url,
        user: args.user,
        ...(args.password !== undefined && { password: args.password }),
        ...(args.token !== undefined && { token: args.token }),
        ...(args.database !== undefined && { database: args.database }),
        ...(serverCertificateHashes !== undefined && { serverCertificateHashes }),
    });
    process.stderr.write(`Connecting to ${args.url} as ${args.user}...\n`);
    try {
        await client.connect();
    }
    catch (err) {
        process.stderr.write(`Connection failed: ${err instanceof Error ? err.message : String(err)}\n`);
        process.exit(1);
    }
    process.stderr.write(`Connected.\n`);
    try {
        if (args.command !== undefined) {
            // Non-interactive: run one query, print result, exit.
            const result = await client.query(args.command);
            process.stdout.write(renderTable(result) + "\n");
        }
        else {
            await runRepl(client);
        }
    }
    finally {
        await client.close();
    }
}
main().catch((err) => {
    process.stderr.write(`Fatal: ${err instanceof Error ? err.message : String(err)}\n`);
    process.exit(1);
});
//# sourceMappingURL=repl.js.map