// Public API for @materialize/pg-webtransport
// Core codec
export { encodeFrontendMessage, decodeBackendMessage, MessageReader, columnAsString, ProtocolError, } from "./codec.js";
// Auth
export { scram256ClientFirst, scram256ClientFinal, scram256VerifyServerFinal, cleartextPasswordMessage, ScramError, } from "./auth.js";
// Client
export { Client, PgError, } from "./client.js";
// React hooks (tree-shaken away in non-React builds)
export { createMaterializeConnection, useQuery, useMutation, } from "./react.js"; // compiled from react.tsx
//# sourceMappingURL=index.js.map