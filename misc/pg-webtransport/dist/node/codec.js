// pgwire message encoding/decoding for browser environments.
// Uses only Uint8Array and DataView — no Node.js APIs.
// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8");
function encodeString(s) {
    return encoder.encode(s);
}
function decodeString(buf) {
    return decoder.decode(buf);
}
/** Concatenate multiple Uint8Arrays into one. */
function concat(...arrays) {
    let total = 0;
    for (const a of arrays)
        total += a.byteLength;
    const out = new Uint8Array(total);
    let offset = 0;
    for (const a of arrays) {
        out.set(a, offset);
        offset += a.byteLength;
    }
    return out;
}
/** Write a 4-byte big-endian int32 into a new Uint8Array. */
function int32BE(value) {
    const buf = new Uint8Array(4);
    new DataView(buf.buffer).setInt32(0, value, false);
    return buf;
}
/** Write a 2-byte big-endian int16 into a new Uint8Array. */
function int16BE(value) {
    const buf = new Uint8Array(2);
    new DataView(buf.buffer).setInt16(0, value, false);
    return buf;
}
/** Null-terminated C string. */
function cstring(s) {
    const encoded = encodeString(s);
    const out = new Uint8Array(encoded.byteLength + 1);
    out.set(encoded);
    out[encoded.byteLength] = 0;
    return out;
}
/** Read a null-terminated string from a DataView starting at offset.
 *  Returns [string, nextOffset]. */
function readCString(view, offset) {
    let end = offset;
    while (end < view.byteLength && view.getUint8(end) !== 0)
        end++;
    const bytes = new Uint8Array(view.buffer, view.byteOffset + offset, end - offset);
    return [decodeString(bytes), end + 1]; // skip null byte
}
// ---------------------------------------------------------------------------
// Frontend message encoding
// ---------------------------------------------------------------------------
/** Encode a frontend message into a Uint8Array ready to send over the wire. */
export function encodeFrontendMessage(msg) {
    switch (msg.type) {
        case "StartupMessage":
            return encodeStartupMessage(msg);
        case "PasswordMessage":
            return encodePasswordMessage(msg);
        case "SASLInitialResponse":
            return encodeSASLInitialResponse(msg);
        case "SASLResponse":
            return encodeSASLResponse(msg);
        case "Query":
            return encodeQueryMessage(msg);
        case "Parse":
            return encodeParseMessage(msg);
        case "Bind":
            return encodeBindMessage(msg);
        case "Describe":
            return encodeDescribeMessage(msg);
        case "Execute":
            return encodeExecuteMessage(msg);
        case "Sync":
            return encodeFixed("S");
        case "Terminate":
            return encodeFixed("X");
    }
}
/** Build a standard frontend message: 1-byte type + 4-byte length (includes itself) + body. */
function buildMessage(typeChar, body) {
    const typeByte = new Uint8Array([typeChar.charCodeAt(0)]);
    const len = int32BE(body.byteLength + 4); // length field includes itself
    return concat(typeByte, len, body);
}
/** Messages with no body beyond the length (Sync, Terminate). */
function encodeFixed(typeChar) {
    return buildMessage(typeChar, new Uint8Array(0));
}
function encodeStartupMessage(msg) {
    // Protocol version 3.0 = 196608 (0x00030000)
    const version = int32BE(196608);
    const parts = [version];
    parts.push(cstring("user"), cstring(msg.user));
    parts.push(cstring("database"), cstring(msg.database));
    if (msg.params) {
        for (const [k, v] of Object.entries(msg.params)) {
            parts.push(cstring(k), cstring(v));
        }
    }
    parts.push(new Uint8Array([0])); // final null terminator
    const body = concat(...parts);
    const lenBuf = int32BE(body.byteLength + 4); // length includes itself
    return concat(lenBuf, body); // no type byte for startup
}
function encodePasswordMessage(msg) {
    return buildMessage("p", cstring(msg.password));
}
function encodeSASLInitialResponse(msg) {
    const mechanismBytes = cstring(msg.mechanism);
    const dataLen = int32BE(msg.data.byteLength);
    const body = concat(mechanismBytes, dataLen, msg.data);
    return buildMessage("p", body);
}
function encodeSASLResponse(msg) {
    return buildMessage("p", msg.data);
}
function encodeQueryMessage(msg) {
    return buildMessage("Q", cstring(msg.query));
}
function encodeParseMessage(msg) {
    const namePart = cstring(msg.name);
    const queryPart = cstring(msg.query);
    const numParams = int16BE(msg.paramTypes.length);
    const oids = new Uint8Array(msg.paramTypes.length * 4);
    const oidView = new DataView(oids.buffer);
    for (let i = 0; i < msg.paramTypes.length; i++) {
        oidView.setInt32(i * 4, msg.paramTypes[i] ?? 0, false);
    }
    return buildMessage("P", concat(namePart, queryPart, numParams, oids));
}
function encodeBindMessage(msg) {
    const portalPart = cstring(msg.portal);
    const stmtPart = cstring(msg.statement);
    const numPF = int16BE(msg.paramFormats.length);
    const pfParts = [];
    for (const f of msg.paramFormats)
        pfParts.push(int16BE(f));
    const numParams = int16BE(msg.params.length);
    const paramParts = [];
    for (const p of msg.params) {
        if (p === null) {
            paramParts.push(int32BE(-1));
        }
        else {
            paramParts.push(int32BE(p.byteLength), p);
        }
    }
    const numRF = int16BE(msg.resultFormats.length);
    const rfParts = [];
    for (const f of msg.resultFormats)
        rfParts.push(int16BE(f));
    const body = concat(portalPart, stmtPart, numPF, ...pfParts, numParams, ...paramParts, numRF, ...rfParts);
    return buildMessage("B", body);
}
function encodeDescribeMessage(msg) {
    const which = new Uint8Array([msg.which.charCodeAt(0)]);
    return buildMessage("D", concat(which, cstring(msg.name)));
}
function encodeExecuteMessage(msg) {
    return buildMessage("E", concat(cstring(msg.portal), int32BE(msg.maxRows)));
}
/**
 * Encode a CancelRequest startup-style message (16 bytes, no type byte).
 * Must be sent on a *separate* new stream/connection — never on a session stream.
 */
export function encodeCancelRequest(pid, secretKey) {
    const buf = new Uint8Array(16);
    const view = new DataView(buf.buffer);
    view.setInt32(0, 16, false); // length = 16
    view.setInt32(4, 80877102, false); // cancel request code
    view.setInt32(8, pid, false);
    view.setInt32(12, secretKey, false);
    return buf;
}
// ---------------------------------------------------------------------------
// Backend message decoding
// ---------------------------------------------------------------------------
export class ProtocolError extends Error {
    constructor(message) {
        super(message);
        this.name = "ProtocolError";
    }
}
/**
 * Decode a single backend message from a raw byte buffer.
 * The buffer must contain exactly one complete message (type byte + length + body).
 */
export function decodeBackendMessage(raw) {
    if (raw.byteLength < 5) {
        throw new ProtocolError("Message too short");
    }
    const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
    const typeByte = view.getUint8(0);
    const typeChar = String.fromCharCode(typeByte);
    // length field at offset 1 includes itself (4 bytes) but not the type byte
    const msgLength = view.getInt32(1, false);
    const bodyLength = msgLength - 4;
    if (raw.byteLength < 1 + msgLength) {
        throw new ProtocolError(`Incomplete message: expected ${1 + msgLength} bytes, got ${raw.byteLength}`);
    }
    // Body view starts at offset 5 (type byte + 4-byte length)
    const bodyView = new DataView(raw.buffer, raw.byteOffset + 5, bodyLength);
    const bodyBytes = new Uint8Array(raw.buffer, raw.byteOffset + 5, bodyLength);
    switch (typeChar) {
        case "R":
            return decodeAuthentication(bodyView, bodyBytes);
        case "S":
            return decodeParameterStatus(bodyView);
        case "K":
            return decodeBackendKeyData(bodyView);
        case "Z":
            return decodeReadyForQuery(bodyView);
        case "T":
            return decodeRowDescription(bodyView);
        case "D":
            return decodeDataRow(bodyView, bodyBytes);
        case "C":
            return decodeCommandComplete(bodyView);
        case "E":
            return decodeErrorOrNotice("ErrorResponse", bodyView);
        case "N":
            return decodeErrorOrNotice("NoticeResponse", bodyView);
        case "A":
            return decodeNotification(bodyView);
        case "1":
            return { type: "ParseComplete" };
        case "2":
            return { type: "BindComplete" };
        case "n":
            return { type: "NoData" };
        case "s":
            return { type: "PortalSuspended" };
        case "I":
            return { type: "EmptyQueryResponse" };
        case "3":
            return { type: "CloseComplete" };
        default:
            throw new ProtocolError(`Unknown backend message type: '${typeChar}' (0x${typeByte.toString(16)})`);
    }
}
function decodeAuthentication(view, bytes) {
    const authType = view.getInt32(0, false);
    switch (authType) {
        case 0:
            return { type: "AuthenticationOk" };
        case 3:
            return { type: "AuthenticationCleartextPassword" };
        case 5: {
            const salt = new Uint8Array(bytes.buffer, bytes.byteOffset + 4, 4);
            return { type: "AuthenticationMD5Password", salt: salt.slice() };
        }
        case 10: {
            // SASL — list of mechanism names, each null-terminated, list ends with empty string
            const mechanisms = [];
            let offset = 4;
            while (offset < view.byteLength) {
                const [name, next] = readCString(view, offset);
                if (name === "")
                    break;
                mechanisms.push(name);
                offset = next;
            }
            return { type: "AuthenticationSASL", mechanisms };
        }
        case 11: {
            const data = bytes.slice(4);
            return { type: "AuthenticationSASLContinue", data };
        }
        case 12: {
            const data = bytes.slice(4);
            return { type: "AuthenticationSASLFinal", data };
        }
        default:
            throw new ProtocolError(`Unknown authentication type: ${authType}`);
    }
}
function decodeParameterStatus(view) {
    const [name, offset] = readCString(view, 0);
    const [value] = readCString(view, offset);
    return { type: "ParameterStatus", name, value };
}
function decodeBackendKeyData(view) {
    return {
        type: "BackendKeyData",
        pid: view.getInt32(0, false),
        secretKey: view.getInt32(4, false),
    };
}
function decodeReadyForQuery(view) {
    const statusByte = view.getUint8(0);
    const status = String.fromCharCode(statusByte);
    return { type: "ReadyForQuery", status };
}
function decodeRowDescription(view) {
    const numFields = view.getInt16(0, false);
    const fields = [];
    let offset = 2;
    for (let i = 0; i < numFields; i++) {
        const [name, next] = readCString(view, offset);
        offset = next;
        const tableOid = view.getInt32(offset, false);
        offset += 4;
        const colAttrNum = view.getInt16(offset, false);
        offset += 2;
        const dataTypeOid = view.getInt32(offset, false);
        offset += 4;
        const dataTypeSize = view.getInt16(offset, false);
        offset += 2;
        const typeModifier = view.getInt32(offset, false);
        offset += 4;
        const format = view.getInt16(offset, false);
        offset += 2;
        fields.push({ name, tableOid, colAttrNum, dataTypeOid, dataTypeSize, typeModifier, format });
    }
    return { type: "RowDescription", fields };
}
function decodeDataRow(view, bytes) {
    const numCols = view.getInt16(0, false);
    const columns = [];
    let offset = 2;
    for (let i = 0; i < numCols; i++) {
        const len = view.getInt32(offset, false);
        offset += 4;
        if (len === -1) {
            columns.push(null);
        }
        else {
            columns.push(bytes.slice(offset, offset + len));
            offset += len;
        }
    }
    return { type: "DataRow", columns };
}
function decodeCommandComplete(view) {
    const [tag] = readCString(view, 0);
    return { type: "CommandComplete", tag };
}
function decodeErrorOrNotice(msgType, view) {
    const fields = {};
    let offset = 0;
    while (offset < view.byteLength) {
        const code = view.getUint8(offset);
        offset++;
        if (code === 0)
            break;
        const [value, next] = readCString(view, offset);
        fields[String.fromCharCode(code)] = value;
        offset = next;
    }
    const message = fields["M"] ?? "(unknown error)";
    if (msgType === "ErrorResponse") {
        const code = fields["C"];
        return code !== undefined
            ? { type: "ErrorResponse", fields, message, code }
            : { type: "ErrorResponse", fields, message };
    }
    return { type: "NoticeResponse", fields, message };
}
function decodeNotification(view) {
    const pid = view.getInt32(0, false);
    const [channel, offset] = readCString(view, 4);
    const [payload] = readCString(view, offset);
    return { type: "NotificationResponse", pid, channel, payload };
}
// ---------------------------------------------------------------------------
// MessageReader — incremental buffer management for a stream
// ---------------------------------------------------------------------------
/**
 * MessageReader wraps a ReadableStream<Uint8Array> and provides readMessage()
 * which resolves with the next complete backend message.
 */
export class MessageReader {
    reader;
    buf = new Uint8Array(0);
    done = false;
    constructor(stream) {
        this.reader = stream.getReader();
    }
    /** Append newly received bytes to the internal buffer. */
    append(chunk) {
        const next = new Uint8Array(this.buf.byteLength + chunk.byteLength);
        next.set(this.buf, 0);
        next.set(chunk, this.buf.byteLength);
        this.buf = next;
    }
    /** Pull bytes from the stream until we have at least `n` bytes buffered. */
    async ensureBytes(n) {
        while (this.buf.byteLength < n) {
            if (this.done) {
                throw new ProtocolError("Stream ended unexpectedly");
            }
            const { value, done } = await this.reader.read();
            if (done) {
                this.done = true;
                if (this.buf.byteLength < n) {
                    throw new ProtocolError("Stream ended unexpectedly");
                }
                return;
            }
            if (value !== undefined) {
                this.append(value);
            }
        }
    }
    /** Consume `n` bytes from the front of the buffer and return them. */
    consume(n) {
        const out = this.buf.slice(0, n);
        this.buf = this.buf.slice(n);
        return out;
    }
    /**
     * Read and decode the next complete backend message from the stream.
     * Every backend message: 1-byte type + 4-byte length (includes itself).
     */
    async readMessage() {
        await this.ensureBytes(5);
        const view = new DataView(this.buf.buffer, this.buf.byteOffset, 5);
        const msgLength = view.getInt32(1, false); // length field
        const totalLength = 1 + msgLength; // type byte + length field + body
        await this.ensureBytes(totalLength);
        const raw = this.consume(totalLength);
        return decodeBackendMessage(raw);
    }
    /** Release the underlying stream reader lock. */
    releaseLock() {
        this.reader.releaseLock();
    }
    /** Cancel the underlying reader, unblocking any pending readMessage() call. */
    cancel() {
        this.reader.cancel().catch(() => { });
    }
}
// ---------------------------------------------------------------------------
// Convenience: decode a DataRow column as a UTF-8 string (text format)
// ---------------------------------------------------------------------------
export function columnAsString(col) {
    if (col === null)
        return null;
    return decodeString(col);
}
export { encodeString, decodeString, concat };
//# sourceMappingURL=codec.js.map