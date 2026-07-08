/**
 * Integration parity tests: proves pg-webtransport is wire-compatible with
 * the standard node-postgres (pg) driver against a live Materialize instance.
 *
 * The strategy is to run every test case through BOTH drivers and assert the
 * results are identical — not just that the WebTransport client succeeds, but
 * that it produces exactly the same field metadata, text values, NULL
 * semantics, error codes, and command tags as the reference driver.
 *
 * Environment variables:
 *   MZ_WT_URL          WebTransport endpoint  (default: https://127.0.0.1:6903/pgwire)
 *   MZ_WT_USER         database user          (default: materialize)
 *   MZ_WT_PASSWORD     password               (optional)
 *   MZ_WT_SERVER_CERT  path to server PEM for hash-pinning (optional)
 *   MZ_PG_URL          postgres connection URL (default: postgres://materialize@127.0.0.1:6900/materialize)
 */
import { test, before, after, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import pg from "pg";
import { Client as WtClient, PgError } from "./client.js";
// ---------------------------------------------------------------------------
// WebTransport polyfill (Node < 22 or when global is absent)
// ---------------------------------------------------------------------------
if (typeof globalThis["WebTransport"] === "undefined") {
    const { WebTransport, quicheLoaded } = await import("@fails-components/webtransport");
    await quicheLoaded;
    globalThis["WebTransport"] = WebTransport;
}
// ---------------------------------------------------------------------------
// Connection config from environment
// ---------------------------------------------------------------------------
const WT_URL = process.env["MZ_WT_URL"] ?? "https://127.0.0.1:6903/pgwire";
const WT_USER = process.env["MZ_WT_USER"] ?? "materialize";
const WT_PASSWORD = process.env["MZ_WT_PASSWORD"];
const WT_CERT = process.env["MZ_WT_SERVER_CERT"];
// Default to direct environmentd port (no TLS required). Use MZ_PG_URL to
// override, e.g. to point at balancerd's pgwire port with ?sslmode=require.
const PG_URL = process.env["MZ_PG_URL"] ?? "postgres://materialize@127.0.0.1:6875/materialize";
function certFileToHash(pemPath) {
    const pem = readFileSync(pemPath, "utf8");
    const match = /-----BEGIN CERTIFICATE-----\r?\n([\s\S]+?)\r?\n-----END CERTIFICATE-----/.exec(pem);
    if (!match)
        throw new Error(`No certificate found in ${pemPath}`);
    const der = Buffer.from(match[1].replace(/\s+/g, ""), "base64");
    const digest = createHash("sha256").update(der).digest();
    const ab = new ArrayBuffer(digest.length);
    new Uint8Array(ab).set(digest);
    return [{ algorithm: "sha-256", value: new Uint8Array(ab) }];
}
// ---------------------------------------------------------------------------
// Reference pg client — configured to return raw text values so that its
// output is directly comparable to the WebTransport client's text-format rows.
// ---------------------------------------------------------------------------
// Override ALL type parsers to return the raw text string. This gives us the
// exact PostgreSQL text-protocol representation, which is what the WebTransport
// client returns via columnAsString().
const rawTypes = {
    getTypeParser: () => (val) => val,
};
let pgClient;
let wtClient;
before(async () => {
    pgClient = new pg.Client({ connectionString: PG_URL, types: rawTypes });
    await pgClient.connect();
    wtClient = new WtClient({
        url: WT_URL,
        user: WT_USER,
        ...(WT_PASSWORD !== undefined && { password: WT_PASSWORD }),
        ...(WT_CERT !== undefined && { serverCertificateHashes: certFileToHash(WT_CERT) }),
    });
    await wtClient.connect();
});
after(async () => {
    await pgClient?.end().catch(() => { });
    await wtClient?.close().catch(() => { });
});
// ---------------------------------------------------------------------------
// Normalizers
// ---------------------------------------------------------------------------
function normalizeWt(r) {
    return {
        fields: r.fields.map((f) => ({ name: f.name, dataTypeOid: f.dataTypeOid })),
        rows: r.rows,
        commandTag: r.commandTag,
    };
}
function normalizePg(r) {
    // Reconstruct the full CommandComplete tag from pg's parsed fields.
    // pg splits e.g. "SELECT 3" into r.command="SELECT" + r.rowCount=3.
    let commandTag = r.command ?? "";
    if (r.command === "INSERT") {
        commandTag = `INSERT 0 ${r.rowCount ?? 0}`;
    }
    else if (r.rowCount !== null && r.rowCount !== undefined) {
        commandTag = `${r.command} ${r.rowCount}`;
    }
    return {
        fields: (r.fields ?? []).map((f) => ({ name: f.name, dataTypeOid: f.dataTypeID })),
        rows: r.rows.map((row) => row.map((v) => (v === undefined ? null : v))),
        commandTag,
    };
}
// ---------------------------------------------------------------------------
// Query runners
// ---------------------------------------------------------------------------
function clients() {
    if (!pgClient || !wtClient)
        throw new Error("clients not initialized — before() failed");
    return { pg: pgClient, wt: wtClient };
}
async function runBoth(sql) {
    const { pg: p, wt: w } = clients();
    const [pgResult, wtResult] = await Promise.all([
        p.query({ text: sql, rowMode: "array" }),
        w.query(sql),
    ]);
    return { pg: normalizePg(pgResult), wt: normalizeWt(wtResult) };
}
async function runBothExtended(sql, params) {
    const { pg: p, wt: w } = clients();
    const [pgResult, wtResult] = await Promise.all([
        p.query({ text: sql, rowMode: "array", values: params }),
        w.execute(sql, params),
    ]);
    return { pg: normalizePg(pgResult), wt: normalizeWt(wtResult) };
}
async function runBothExpectError(sql) {
    const { pg: p, wt: w } = clients();
    const [pgErr, wtErr] = await Promise.all([
        p.query(sql).then(() => null, (e) => e),
        w.query(sql).then(() => null, (e) => e),
    ]);
    if (!pgErr)
        throw new Error("pg did not error");
    if (!wtErr)
        throw new Error("wt did not error");
    const pgCode = pgErr.code;
    const wtCode = wtErr instanceof PgError ? wtErr.code : undefined;
    return { pg: { code: pgCode }, wt: { code: wtCode } };
}
// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------
describe("simple query protocol — values and types", () => {
    const cases = [
        // Integer literals
        { name: "int literal", sql: "SELECT 1 AS n" },
        { name: "negative int", sql: "SELECT -42 AS n" },
        { name: "int arithmetic", sql: "SELECT 1 + 2 AS n" },
        { name: "int8 cast", sql: "SELECT 9999999999::int8 AS n" },
        { name: "int4 cast", sql: "SELECT 42::int4 AS n" },
        { name: "int2 cast", sql: "SELECT 7::int2 AS n" },
        // Floats
        { name: "float literal", sql: "SELECT 1.5::float8 AS f" },
        { name: "float arithmetic", sql: "SELECT 0.1::float8 + 0.2::float8 AS f" },
        // Strings
        { name: "string literal", sql: "SELECT 'hello' AS s" },
        { name: "empty string", sql: "SELECT '' AS s" },
        { name: "string concat", sql: "SELECT 'foo' || 'bar' AS s" },
        { name: "string with spaces", sql: "SELECT 'hello world' AS s" },
        { name: "string with special chars", sql: "SELECT 'it''s a test' AS s" },
        // Booleans
        { name: "true", sql: "SELECT true AS b" },
        { name: "false", sql: "SELECT false AS b" },
        // NULL
        { name: "null literal", sql: "SELECT NULL AS n" },
        { name: "null in expression", sql: "SELECT NULL::text AS n" },
        { name: "coalesce non-null", sql: "SELECT COALESCE(NULL, 'fallback') AS s" },
        { name: "coalesce all null", sql: "SELECT COALESCE(NULL, NULL) AS n" },
        // Multiple columns
        { name: "multiple columns", sql: "SELECT 1 AS a, 'x' AS b, true AS c" },
        { name: "mixed nulls", sql: "SELECT 1 AS a, NULL AS b, 'z' AS c" },
        // Multiple rows
        {
            name: "multiple rows from VALUES",
            sql: "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(n, s)",
        },
        {
            name: "multiple rows with nulls",
            sql: "SELECT * FROM (VALUES (1, NULL), (NULL, 'b'), (3, 'c')) AS t(n, s)",
        },
        // Empty result
        { name: "empty result set", sql: "SELECT 1 AS n WHERE false" },
        { name: "empty result multiple cols", sql: "SELECT 1 AS a, 2 AS b WHERE 1 = 0" },
    ];
    for (const { name, sql } of cases) {
        test(name, async () => {
            const { pg: pgR, wt: wtR } = await runBoth(sql);
            assert.deepEqual(wtR.fields, pgR.fields, `field metadata mismatch for: ${sql}`);
            assert.deepEqual(wtR.rows, pgR.rows, `row data mismatch for: ${sql}`);
        });
    }
});
describe("command tags", () => {
    // The commandTag (e.g. "SELECT 3") should match between drivers.
    const cases = [
        { name: "SELECT 0 rows", sql: "SELECT 1 WHERE false" },
        { name: "SELECT 1 row", sql: "SELECT 1" },
        {
            name: "SELECT 3 rows",
            sql: "SELECT * FROM (VALUES (1),(2),(3)) AS t(n)",
        },
    ];
    for (const { name, sql } of cases) {
        test(name, async () => {
            const { pg: pgR, wt: wtR } = await runBoth(sql);
            assert.equal(wtR.commandTag, pgR.commandTag, `command tag mismatch for: ${sql}`);
        });
    }
});
describe("field metadata — OIDs", () => {
    // Type OIDs must match exactly so consumers can interpret values correctly.
    const cases = [
        { name: "int4 OID", sql: "SELECT 1::int4 AS n" },
        { name: "int8 OID", sql: "SELECT 1::int8 AS n" },
        { name: "float8 OID", sql: "SELECT 1.0::float8 AS n" },
        { name: "text OID", sql: "SELECT 'x'::text AS s" },
        { name: "bool OID", sql: "SELECT true AS b" },
        { name: "unknown OID from literal", sql: "SELECT 'x' AS s" },
    ];
    for (const { name, sql } of cases) {
        test(name, async () => {
            const { pg: pgR, wt: wtR } = await runBoth(sql);
            assert.deepEqual(wtR.fields.map((f) => f.dataTypeOid), pgR.fields.map((f) => f.dataTypeOid), `OID mismatch for: ${sql}`);
        });
    }
});
describe("extended query protocol — parameterized queries", () => {
    const cases = [
        {
            name: "single text param",
            sql: "SELECT $1::text AS s",
            params: ["hello"],
        },
        {
            name: "single int param",
            sql: "SELECT $1::int4 AS n",
            params: ["42"],
        },
        {
            name: "null param",
            sql: "SELECT $1::text AS s",
            params: [null],
        },
        {
            name: "multiple params",
            sql: "SELECT $1::text AS a, $2::int4 AS b, $3::bool AS c",
            params: ["hello", "7", "true"],
        },
        {
            name: "param in WHERE",
            sql: "SELECT n FROM (VALUES (1),(2),(3)) AS t(n) WHERE n > $1::int4",
            params: ["1"],
        },
        {
            name: "null param in filter",
            sql: "SELECT COALESCE($1::text, 'default') AS s",
            params: [null],
        },
    ];
    for (const { name, sql, params } of cases) {
        test(name, async () => {
            const { pg: pgR, wt: wtR } = await runBothExtended(sql, params);
            assert.deepEqual(wtR.fields, pgR.fields, `field metadata mismatch: ${sql}`);
            assert.deepEqual(wtR.rows, pgR.rows, `row data mismatch: ${sql}`);
        });
    }
});
describe("error semantics — SQLSTATE codes", () => {
    // Both drivers must surface the same SQLSTATE for the same errors.
    const cases = [
        {
            name: "syntax error (42601)",
            sql: "SELECT * FORM foo",
        },
        {
            name: "undefined table (42P01)",
            sql: "SELECT * FROM nonexistent_table_xyz_12345",
        },
        {
            name: "undefined column (42703)",
            sql: "SELECT nonexistent_col FROM (VALUES (1)) AS t(n)",
        },
        {
            name: "division by zero (22012)",
            sql: "SELECT 1 / 0",
        },
        {
            name: "invalid cast (22P02 or 42846)",
            sql: "SELECT 'not_a_number'::int4",
        },
    ];
    for (const { name, sql } of cases) {
        test(name, async () => {
            const { pg: pgE, wt: wtE } = await runBothExpectError(sql);
            assert.equal(wtE.code, pgE.code, `SQLSTATE mismatch for: ${sql} (pg=${pgE.code}, wt=${wtE.code})`);
        });
    }
});
describe("NULL edge cases", () => {
    test("null is distinct from empty string", async () => {
        const { pg: pgR, wt: wtR } = await runBoth("SELECT NULL AS a, '' AS b, 'null' AS c");
        assert.deepEqual(wtR.rows, pgR.rows);
        // Explicitly verify: first column must be null, others must not be
        assert.equal(wtR.rows[0][0], null);
        assert.equal(wtR.rows[0][1], "");
        assert.equal(wtR.rows[0][2], "null");
    });
    test("all-null row", async () => {
        const { pg: pgR, wt: wtR } = await runBoth("SELECT NULL::int4 AS a, NULL::text AS b, NULL::bool AS c");
        assert.deepEqual(wtR.rows, pgR.rows);
        assert.deepEqual(wtR.rows[0], [null, null, null]);
    });
    test("null mixed with values across multiple rows", async () => {
        const { pg: pgR, wt: wtR } = await runBoth("SELECT * FROM (VALUES (1, NULL), (NULL, 'x'), (3, 'y')) AS t(a, b)");
        assert.deepEqual(wtR.rows, pgR.rows);
    });
});
describe("query cancellation", () => {
    // SUBSCRIBE WITH (snapshot = false) never produces rows and never sends
    // CommandComplete/ReadyForQuery — it blocks until cancelled. This makes it
    // the most reliable blocking query for testing CancelRequest.
    test("CancelRequest interrupts a blocking query with SQLSTATE 57014", async () => {
        const { wt } = clients();
        let caught;
        const queryPromise = wt
            .query("SUBSCRIBE (SELECT 1) WITH (snapshot = false)")
            .then(() => { throw new Error("expected query to be cancelled, not succeed"); }, (e) => { caught = e; });
        // Give the query time to reach the server before sending cancel
        await new Promise((resolve) => setTimeout(resolve, 200));
        await wt.cancel();
        await queryPromise;
        assert.ok(caught instanceof PgError, `expected PgError, got: ${String(caught)}`);
        assert.equal(caught.code, "57014", `expected query_canceled (57014), got ${caught.code}`);
    });
});
describe("LISTEN / NOTIFY", () => {
    // Skips automatically on Materialize which does not support LISTEN.
    test("listen() registers and can be aborted cleanly", async () => {
        const { wt } = clients();
        const controller = new AbortController();
        // Abort after a short delay — no notifications will arrive; we just
        // verify that the LISTEN handshake succeeds and abort exits without error.
        setTimeout(() => controller.abort(), 200);
        try {
            await wt.listen("_wt_smoke_channel", () => { }, controller.signal);
        }
        catch (e) {
            // Skip if the server does not support LISTEN (e.g. Materialize)
            if (e instanceof PgError && e.code === "42601")
                return;
            throw e;
        }
        // If we get here without throwing, the LISTEN lifecycle is correct.
    });
    // This test requires PostgreSQL's pg_notify() which Materialize does not
    // support. It skips automatically when run against Materialize.
    test("receives NOTIFY sent via the pg driver", async () => {
        const { wt, pg: p } = clients();
        // Probe for pg_notify support; skip if unsupported (e.g. Materialize).
        try {
            await p.query("SELECT pg_notify('_wt_probe', '')");
        }
        catch {
            return; // skip — server does not support pg_notify
        }
        const received = [];
        const controller = new AbortController();
        const listenDone = wt.listen("_wt_notify_test", (n) => {
            received.push(n);
            controller.abort();
        }, controller.signal);
        // Let the LISTEN session fully register before sending the notification
        await new Promise((resolve) => setTimeout(resolve, 200));
        await p.query("SELECT pg_notify('_wt_notify_test', 'hello from pg')");
        await listenDone;
        assert.equal(received.length, 1);
        assert.equal(received[0]?.channel, "_wt_notify_test");
        assert.equal(received[0]?.payload, "hello from pg");
    });
});
//# sourceMappingURL=test-integration.js.map