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
export {};
//# sourceMappingURL=test-integration.d.ts.map