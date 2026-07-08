declare function encodeString(s: string): Uint8Array;
declare function decodeString(buf: Uint8Array): string;
/** Concatenate multiple Uint8Arrays into one. */
declare function concat(...arrays: Uint8Array[]): Uint8Array;
export interface StartupMessage {
    type: "StartupMessage";
    user: string;
    database: string;
    /** Extra parameters (application_name etc.) */
    params?: Record<string, string>;
}
export interface PasswordMessage {
    type: "PasswordMessage";
    password: string;
}
export interface SASLInitialResponse {
    type: "SASLInitialResponse";
    mechanism: string;
    data: Uint8Array;
}
export interface SASLResponse {
    type: "SASLResponse";
    data: Uint8Array;
}
export interface QueryMessage {
    type: "Query";
    query: string;
}
export interface ParseMessage {
    type: "Parse";
    name: string;
    query: string;
    paramTypes: number[];
}
export interface BindMessage {
    type: "Bind";
    portal: string;
    statement: string;
    paramFormats: number[];
    params: (Uint8Array | null)[];
    resultFormats: number[];
}
export interface DescribeMessage {
    type: "Describe";
    which: "S" | "P";
    name: string;
}
export interface ExecuteMessage {
    type: "Execute";
    portal: string;
    maxRows: number;
}
export interface SyncMessage {
    type: "Sync";
}
export interface TerminateMessage {
    type: "Terminate";
}
export type FrontendMessage = StartupMessage | PasswordMessage | SASLInitialResponse | SASLResponse | QueryMessage | ParseMessage | BindMessage | DescribeMessage | ExecuteMessage | SyncMessage | TerminateMessage;
/** Encode a frontend message into a Uint8Array ready to send over the wire. */
export declare function encodeFrontendMessage(msg: FrontendMessage): Uint8Array;
export type AuthenticationOk = {
    type: "AuthenticationOk";
};
export type AuthenticationCleartextPassword = {
    type: "AuthenticationCleartextPassword";
};
export type AuthenticationMD5Password = {
    type: "AuthenticationMD5Password";
    salt: Uint8Array;
};
export type AuthenticationSASL = {
    type: "AuthenticationSASL";
    mechanisms: string[];
};
export type AuthenticationSASLContinue = {
    type: "AuthenticationSASLContinue";
    data: Uint8Array;
};
export type AuthenticationSASLFinal = {
    type: "AuthenticationSASLFinal";
    data: Uint8Array;
};
export type ParameterStatus = {
    type: "ParameterStatus";
    name: string;
    value: string;
};
export type BackendKeyData = {
    type: "BackendKeyData";
    pid: number;
    secretKey: number;
};
export type ReadyForQuery = {
    type: "ReadyForQuery";
    status: "I" | "T" | "E";
};
export type RowDescriptionField = {
    name: string;
    tableOid: number;
    colAttrNum: number;
    dataTypeOid: number;
    dataTypeSize: number;
    typeModifier: number;
    format: number;
};
export type RowDescription = {
    type: "RowDescription";
    fields: RowDescriptionField[];
};
export type DataRow = {
    type: "DataRow";
    columns: (Uint8Array | null)[];
};
export type CommandComplete = {
    type: "CommandComplete";
    tag: string;
};
export type ErrorResponse = {
    type: "ErrorResponse";
    fields: Record<string, string>;
    message: string;
    code?: string;
};
export type NoticeResponse = {
    type: "NoticeResponse";
    fields: Record<string, string>;
    message: string;
};
export type ParseComplete = {
    type: "ParseComplete";
};
export type BindComplete = {
    type: "BindComplete";
};
export type NoData = {
    type: "NoData";
};
export type PortalSuspended = {
    type: "PortalSuspended";
};
export type EmptyQueryResponse = {
    type: "EmptyQueryResponse";
};
export type CloseComplete = {
    type: "CloseComplete";
};
export type BackendMessage = AuthenticationOk | AuthenticationCleartextPassword | AuthenticationMD5Password | AuthenticationSASL | AuthenticationSASLContinue | AuthenticationSASLFinal | ParameterStatus | BackendKeyData | ReadyForQuery | RowDescription | DataRow | CommandComplete | ErrorResponse | NoticeResponse | ParseComplete | BindComplete | NoData | PortalSuspended | EmptyQueryResponse | CloseComplete;
export declare class ProtocolError extends Error {
    constructor(message: string);
}
/**
 * Decode a single backend message from a raw byte buffer.
 * The buffer must contain exactly one complete message (type byte + length + body).
 */
export declare function decodeBackendMessage(raw: Uint8Array): BackendMessage;
/**
 * MessageReader wraps a ReadableStream<Uint8Array> and provides readMessage()
 * which resolves with the next complete backend message.
 */
export declare class MessageReader {
    private readonly reader;
    private buf;
    private done;
    constructor(stream: ReadableStream<Uint8Array>);
    /** Append newly received bytes to the internal buffer. */
    private append;
    /** Pull bytes from the stream until we have at least `n` bytes buffered. */
    private ensureBytes;
    /** Consume `n` bytes from the front of the buffer and return them. */
    private consume;
    /**
     * Read and decode the next complete backend message from the stream.
     * Every backend message: 1-byte type + 4-byte length (includes itself).
     */
    readMessage(): Promise<BackendMessage>;
    /** Release the underlying stream reader lock. */
    releaseLock(): void;
}
export declare function columnAsString(col: Uint8Array | null): string | null;
export { encodeString, decodeString, concat };
//# sourceMappingURL=codec.d.ts.map