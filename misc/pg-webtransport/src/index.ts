// Public API for @materialize/pg-webtransport

// Core codec
export {
  encodeFrontendMessage,
  decodeBackendMessage,
  MessageReader,
  columnAsString,
  ProtocolError,
  type FrontendMessage,
  type BackendMessage,
  type StartupMessage,
  type PasswordMessage,
  type SASLInitialResponse,
  type SASLResponse,
  type QueryMessage,
  type ParseMessage,
  type BindMessage,
  type DescribeMessage,
  type ExecuteMessage,
  type SyncMessage,
  type TerminateMessage,
  type AuthenticationOk,
  type AuthenticationCleartextPassword,
  type AuthenticationMD5Password,
  type AuthenticationSASL,
  type AuthenticationSASLContinue,
  type AuthenticationSASLFinal,
  type ParameterStatus,
  type BackendKeyData,
  type ReadyForQuery,
  type RowDescription,
  type RowDescriptionField,
  type DataRow,
  type CommandComplete,
  type ErrorResponse,
  type NoticeResponse,
  type ParseComplete,
  type BindComplete,
  type NoData,
  type PortalSuspended,
  type EmptyQueryResponse,
  type CloseComplete,
} from "./codec.js";

// Auth
export {
  scram256ClientFirst,
  scram256ClientFinal,
  scram256VerifyServerFinal,
  cleartextPasswordMessage,
  ScramError,
  type ScramClientState,
} from "./auth.js";

// Client
export {
  Client,
  PgError,
  type ClientOptions,
  type Field,
  type QueryResult,
} from "./client.js";

// React hooks (tree-shaken away in non-React builds)
export {
  createMaterializeConnection,
  useQuery,
  useMutation,
  type MaterializeConnection,
  type UseQueryOptions,
  type UseQueryResult,
  type UseMutationOptions,
  type UseMutationResult,
} from "./react.js"; // compiled from react.tsx
