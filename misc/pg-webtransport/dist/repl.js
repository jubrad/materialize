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
import { Client, PgError } from "./client.js";
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
  --no-tls-verify        Skip TLS certificate verification (dev only!)
  -h, --help             Show this help

In the REPL:
  Type SQL terminated with ; to execute.
  \\q, \\quit             Quit
  \\timing               Toggle query timing
  \\?                    Show REPL commands
`;
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
        try {
            const result = await client.query(sql);
            const ms = performance.now() - t0;
            process.stdout.write(renderTable(result) + "\n");
            if (timing)
                process.stdout.write(`Time: ${ms.toFixed(3)} ms\n`);
        }
        catch (err) {
            const ms = performance.now() - t0;
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
    const client = new Client({
        url: args.url,
        user: args.user,
        ...(args.password !== undefined && { password: args.password }),
        ...(args.token !== undefined && { token: args.token }),
        ...(args.database !== undefined && { database: args.database }),
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