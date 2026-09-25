/** The specification draft revision this SDK targets (AWP-VER-009). */
export const SPEC_REVISION = "0.1-draft.9";

/** The wire protocol version negotiated in `initialize`, as `MAJOR.MINOR` only (AWP-VER-008). */
export const PROTOCOL_VERSION = "0.1";

/** Protocol versions this SDK offers in `initialize`, highest first (AWP-VER-002). */
export const SUPPORTED_PROTOCOL_VERSIONS: readonly string[] = [PROTOCOL_VERSION];

/** WebSocket subprotocol offered on every AWP connection (AWP-TRN-013, AWP-SEC-005). */
export const AWP_SUBPROTOCOL = "awp";
