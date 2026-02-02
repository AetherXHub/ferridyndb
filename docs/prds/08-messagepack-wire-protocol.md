# PRD: MessagePack Wire Protocol

**Priority:** 8
**Status:** Approved
**Proposal:** [docs/proposals/accepted/messagepack-wire-protocol.md](../proposals/accepted/messagepack-wire-protocol.md)

## Summary

Replace the JSON-over-newlines wire protocol in ferridyn-server with MessagePack-over-length-prefix. Reduces payload size ~30-40%, eliminates redundant JSON round-trip (documents are already stored as MessagePack), and uses a robust binary framing format.

## Scope

### In Scope

- Length-prefixed framing: 4-byte big-endian u32 length + MessagePack payload
- Server-side: replace JSON read/write with MessagePack
- Client-side: replace JSON read/write with MessagePack
- Maximum message size enforcement (64 MiB)
- Integration test updates
- Console client updates (console uses FerridynClient which talks to server)

### Out of Scope

- Protocol negotiation / handshake (hard cut-over, pre-1.0)
- Mixed JSON/MessagePack mode
- Backward compatibility with old protocol version

## Decisions

| Question | Decision | Rationale |
|----------|----------|-----------|
| Migration approach? | Hard cut-over | Pre-1.0, no external consumers; server and client ship together |
| Max message size? | 64 MiB | Prevents memory exhaustion from malformed length prefix |
| Client implementation? | Option A initially (serde_json::Value via rmp-serde) | Minimal diff; rmp-serde handles Value natively |
| Error responses format? | Same as data (MessagePack) | Simpler; no format mixing |

## Implementation

### Single Phase (Low Complexity)

**Deliverables:**

**Server (`server.rs`):**
- Remove `BufReader` + `read_line` loop
- Read 4-byte length prefix with `AsyncReadExt::read_exact`
- Validate length <= 64 MiB
- Read N bytes, deserialize with `rmp_serde::from_slice`
- Serialize response with `rmp_serde::to_vec`, write 4-byte length + payload + flush

**Client (`client.rs`):**
- Serialize requests with `rmp_serde::to_vec`
- Write 4-byte length prefix + payload
- Read 4-byte length prefix + payload
- Deserialize with `rmp_serde::from_slice`

**Protocol (`protocol.rs`):**
- No structural changes to `Request`/`Response` enums (already use serde traits)
- Update module doc comments

**Cargo.toml:**
- Add `rmp-serde = "1"` to ferridyn-server dependencies

**Files:**
| File | Change |
|------|--------|
| `ferridyn-server/Cargo.toml` | Add `rmp-serde` dependency |
| `ferridyn-server/src/server.rs` | Replace JSON framing with length-prefix + MessagePack |
| `ferridyn-server/src/client.rs` | Replace JSON framing with length-prefix + MessagePack |
| `ferridyn-server/src/protocol.rs` | Doc comment update only |
| `ferridyn-server/tests/integration.rs` | Update test helpers for new framing |
| `ferridyn-console/` | No changes needed (uses FerridynClient) |

**Tests:**
- All existing integration tests pass with new protocol
- `test_oversized_message_rejected` (send length > 64 MiB, verify error)
- `test_zero_length_message` (edge case)
- `test_truncated_message` (connection drops mid-payload)

## Acceptance Criteria

1. All existing server integration tests pass with MessagePack protocol
2. Console continues to work (uses updated FerridynClient)
3. Messages > 64 MiB are rejected with clear error
4. No `serde_json` serialization in the wire path (except within `serde_json::Value` which is the document format)
5. `cargo clippy --workspace -- -D warnings` clean

## Dependencies

- None (standalone infrastructure change)

## Dependents

- All future protocol additions automatically use MessagePack framing
