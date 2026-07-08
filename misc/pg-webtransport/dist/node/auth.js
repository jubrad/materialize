// SCRAM-SHA-256 authentication and cleartext password support.
// Uses only Web Crypto API (crypto.subtle) — no Node.js.
// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8");
function encode(s) {
    return encoder.encode(s);
}
function decodeUtf8(b) {
    return decoder.decode(b);
}
function encodeBase64(data) {
    let binary = "";
    for (let i = 0; i < data.byteLength; i++) {
        binary += String.fromCharCode(data[i] ?? 0);
    }
    return btoa(binary);
}
function decodeBase64(s) {
    const binary = atob(s);
    const out = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
        out[i] = binary.charCodeAt(i);
    }
    return out;
}
/** XOR two Uint8Arrays of the same length. */
function xor(a, b) {
    if (a.byteLength !== b.byteLength) {
        throw new Error(`xor: length mismatch ${a.byteLength} vs ${b.byteLength}`);
    }
    const out = new Uint8Array(a.byteLength);
    for (let i = 0; i < a.byteLength; i++) {
        out[i] = (a[i] ?? 0) ^ (b[i] ?? 0);
    }
    return out;
}
/** Generate `n` cryptographically random bytes. */
function randomBytes(n) {
    const buf = new Uint8Array(n);
    crypto.getRandomValues(buf);
    return buf;
}
/** Generate a base64-encoded nonce of `byteLength` random bytes. */
function generateNonce(byteLength = 24) {
    return encodeBase64(randomBytes(byteLength));
}
// ---------------------------------------------------------------------------
// Web Crypto helpers
// ---------------------------------------------------------------------------
async function hmacSha256(keyData, data) {
    // Cast to Uint8Array<ArrayBuffer>: in practice new Uint8Array() always uses
    // ArrayBuffer (not SharedArrayBuffer), but @types/node@25 requires the
    // stricter type to satisfy BufferSource.
    const key = await crypto.subtle.importKey("raw", keyData, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
    const sig = await crypto.subtle.sign("HMAC", key, data);
    return new Uint8Array(sig);
}
async function sha256(data) {
    const hash = await crypto.subtle.digest("SHA-256", data);
    return new Uint8Array(hash);
}
/**
 * PBKDF2-HMAC-SHA256: derive a key from a password and salt.
 */
async function pbkdf2HmacSha256(password, salt, iterations) {
    const keyMaterial = await crypto.subtle.importKey("raw", password, "PBKDF2", false, ["deriveBits"]);
    const derivedBits = await crypto.subtle.deriveBits({
        name: "PBKDF2",
        hash: "SHA-256",
        salt: salt,
        iterations,
    }, keyMaterial, 256);
    return new Uint8Array(derivedBits);
}
/**
 * Build the SASLInitialResponse message (client-first-message).
 * Returns both the message to send and internal state needed for step 2.
 */
export function scram256ClientFirst(username) {
    const clientNonce = generateNonce();
    // Normalize username per RFC 5802 (saslprep-lite: escape ',' and '=')
    const safeUsername = username.replace(/=/g, "=3D").replace(/,/g, "=2C");
    const clientFirstMessageBare = `n=${safeUsername},r=${clientNonce}`;
    // GS2 header: no channel binding, no authzid
    const clientFirstMessage = `n,,${clientFirstMessageBare}`;
    const data = encode(clientFirstMessage);
    return {
        message: {
            type: "SASLInitialResponse",
            mechanism: "SCRAM-SHA-256",
            data,
        },
        state: {
            clientFirstMessageBare,
            clientNonce,
        },
    };
}
export class ScramError extends Error {
    constructor(message) {
        super(message);
        this.name = "ScramError";
    }
}
/**
 * Parse the server-first-message from AuthenticationSASLContinue.
 */
function parseServerFirst(data) {
    const serverFirstMessage = decodeUtf8(data);
    const parts = {};
    for (const part of serverFirstMessage.split(",")) {
        const eqIdx = part.indexOf("=");
        if (eqIdx === -1)
            continue;
        const key = part.slice(0, eqIdx);
        const val = part.slice(eqIdx + 1);
        parts[key] = val;
    }
    const serverNonce = parts["r"];
    const saltB64 = parts["s"];
    const iterStr = parts["i"];
    if (!serverNonce || !saltB64 || !iterStr) {
        throw new ScramError(`Invalid server-first-message: missing r/s/i in "${serverFirstMessage}"`);
    }
    const iterations = parseInt(iterStr, 10);
    if (isNaN(iterations) || iterations < 1) {
        throw new ScramError(`Invalid iteration count: ${iterStr}`);
    }
    return {
        serverNonce,
        salt: decodeBase64(saltB64),
        iterations,
        serverFirstMessage,
    };
}
/**
 * Build the SASLResponse message (client-final-message).
 * Also returns the server signature for verification.
 */
export async function scram256ClientFinal(msg, password, state) {
    const { serverNonce, salt, iterations, serverFirstMessage } = parseServerFirst(msg.data);
    // Verify the nonce starts with our client nonce
    if (!serverNonce.startsWith(state.clientNonce)) {
        throw new ScramError("Server nonce does not contain client nonce");
    }
    const passwordBytes = encode(password);
    // SaltedPassword = Hi(password, salt, iterations)
    const saltedPassword = await pbkdf2HmacSha256(passwordBytes, salt, iterations);
    // ClientKey = HMAC(SaltedPassword, "Client Key")
    const clientKey = await hmacSha256(saltedPassword, encode("Client Key"));
    // StoredKey = H(ClientKey)
    const storedKey = await sha256(clientKey);
    // client-final-message-without-proof
    // c= is base64 of the GS2 header "n,,"
    const gs2Header = encode("n,,");
    const channelBinding = encodeBase64(gs2Header);
    const clientFinalWithoutProof = `c=${channelBinding},r=${serverNonce}`;
    // AuthMessage = client-first-bare + "," + server-first + "," + client-final-without-proof
    const authMessage = `${state.clientFirstMessageBare},${serverFirstMessage},${clientFinalWithoutProof}`;
    // ClientSignature = HMAC(StoredKey, AuthMessage)
    const clientSignature = await hmacSha256(storedKey, encode(authMessage));
    // ClientProof = ClientKey XOR ClientSignature
    const clientProof = xor(clientKey, clientSignature);
    const clientFinalMessage = `${clientFinalWithoutProof},p=${encodeBase64(clientProof)}`;
    // ServerKey = HMAC(SaltedPassword, "Server Key")
    const serverKey = await hmacSha256(saltedPassword, encode("Server Key"));
    // ServerSignature = HMAC(ServerKey, AuthMessage)
    const serverSignature = await hmacSha256(serverKey, encode(authMessage));
    return {
        message: {
            type: "SASLResponse",
            data: encode(clientFinalMessage),
        },
        serverSignature,
    };
}
/**
 * Verify the server-final-message from AuthenticationSASLFinal.
 * Throws ScramError if the server signature does not match.
 */
export function scram256VerifyServerFinal(data, expectedServerSignature) {
    const serverFinalMessage = decodeUtf8(data);
    const parts = {};
    for (const part of serverFinalMessage.split(",")) {
        const eqIdx = part.indexOf("=");
        if (eqIdx === -1)
            continue;
        parts[part.slice(0, eqIdx)] = part.slice(eqIdx + 1);
    }
    if (parts["e"] !== undefined) {
        throw new ScramError(`Server reported SCRAM error: ${parts["e"]}`);
    }
    const vB64 = parts["v"];
    if (!vB64) {
        throw new ScramError("Server final message missing verifier");
    }
    const serverSig = decodeBase64(vB64);
    if (serverSig.byteLength !== expectedServerSignature.byteLength) {
        throw new ScramError("Server signature length mismatch");
    }
    // Constant-time comparison
    let diff = 0;
    for (let i = 0; i < serverSig.byteLength; i++) {
        diff |= (serverSig[i] ?? 0) ^ (expectedServerSignature[i] ?? 0);
    }
    if (diff !== 0) {
        throw new ScramError("Server signature verification failed");
    }
}
// ---------------------------------------------------------------------------
// Cleartext password helper
// ---------------------------------------------------------------------------
/** Build a cleartext PasswordMessage. */
export function cleartextPasswordMessage(password) {
    return { type: "PasswordMessage", password };
}
//# sourceMappingURL=auth.js.map