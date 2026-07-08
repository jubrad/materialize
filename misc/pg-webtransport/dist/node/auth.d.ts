import { type SASLInitialResponse, type SASLResponse, type AuthenticationSASLContinue } from "./codec.js";
export interface ScramClientState {
    clientFirstMessageBare: string;
    clientNonce: string;
}
/**
 * Build the SASLInitialResponse message (client-first-message).
 * Returns both the message to send and internal state needed for step 2.
 */
export declare function scram256ClientFirst(username: string): {
    message: SASLInitialResponse;
    state: ScramClientState;
};
export declare class ScramError extends Error {
    constructor(message: string);
}
/**
 * Build the SASLResponse message (client-final-message).
 * Also returns the server signature for verification.
 */
export declare function scram256ClientFinal(msg: AuthenticationSASLContinue, password: string, state: ScramClientState): Promise<{
    message: SASLResponse;
    serverSignature: Uint8Array;
}>;
/**
 * Verify the server-final-message from AuthenticationSASLFinal.
 * Throws ScramError if the server signature does not match.
 */
export declare function scram256VerifyServerFinal(data: Uint8Array, expectedServerSignature: Uint8Array): void;
/** Build a cleartext PasswordMessage. */
export declare function cleartextPasswordMessage(password: string): import("./codec.js").PasswordMessage;
//# sourceMappingURL=auth.d.ts.map