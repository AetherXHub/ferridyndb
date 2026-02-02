# Proposal: MessagePack Wire Protocol for ferridyn-server

**Status:** Draft
**Author:** —
**Date:** 2026-02-01

## Motivation

The ferridyn-server communicates with clients over a Unix socket using JSON-over-newlines. Meanwhile, ferridyn-core already stores documents on disk as MessagePack (via `rmp-serde`). Switching the wire protocol to MessagePack would:

- **Reduce payload size** — MessagePack is typically 30-40% smaller than equivalent JSON, which matters for large document transfers over the socket.
- **Unify serialization formats** — The core engine already pays the cost of MessagePack serialization for storage. Using the same format on the wire avoids a redundant JSON round-trip on every request/response.
- **Reuse an existing dependency** — `rmp-serde` is already in the workspace (ferridyn-core). Adding it to ferridyn-server introduces no new transitive dependencies.

## Current Protocol

The server uses a newline-delimited JSON protocol:

- **Framing:** Each message is a single JSON object terminated by `\n`.
- **Server side** (`server.rs`): `read_line()` to receive, `serde_json::from_str()` to parse, `serde_json::to_vec()` + `\n` to respond.
- **Client side** (`client.rs`): `serde_json::to_vec()` + `\n` to send, `read_line()` + `serde_json::from_str()` to receive.
- **Types** (`protocol.rs`): `Request` and `Response` enums derive serde `Serialize`/`Deserialize` — they are not tied to JSON specifically.

Newline framing works because JSON text does not contain bare `0x0A` bytes (newlines inside strings are escaped as `\n`).

## Proposed Protocol

Replace JSON-over-newlines with MessagePack-over-length-prefix.

### Framing

Each message (request or response) is preceded by a 4-byte big-endian unsigned integer indicating the length of the MessagePack payload that follows:

```
+-------------------+-----------------------------+
| length (4 bytes)  | MessagePack payload (N bytes)|
+-------------------+-----------------------------+
```

- **Length field:** `u32` big-endian, value = N (byte count of the payload). Maximum message size: ~4 GiB, far beyond any practical need.
- **Payload:** MessagePack-encoded `Request` or `Response`.

Newline framing is not viable for MessagePack because the binary encoding can contain arbitrary byte values including `0x0A`.

### Reading a message

```
1. Read exactly 4 bytes → decode as u32 big-endian → N
2. Read exactly N bytes → deserialize with rmp_serde::from_slice()
```

### Writing a message

```
1. Serialize with rmp_serde::to_vec() → payload
2. Write payload.len() as u32 big-endian (4 bytes)
3. Write payload
4. Flush
```

## Changes Required

### `crates/ferridyn-server/Cargo.toml`

Add `rmp-serde = "1"` to `[dependencies]`.

### `crates/ferridyn-server/src/protocol.rs`

No structural changes to `Request`, `Response`, or helper types — they already use serde traits. Update the module doc comment to reflect the new framing.

### `crates/ferridyn-server/src/server.rs`

Replace `handle_connection`:

- Remove `BufReader` + `read_line` loop.
- Use `AsyncReadExt::read_exact` to read the 4-byte length prefix, then `read_exact` for the payload.
- Replace `serde_json::from_str` with `rmp_serde::from_slice`.
- Replace `serde_json::to_vec` with `rmp_serde::to_vec`, prepend the 4-byte length, write + flush.

### `crates/ferridyn-server/src/client.rs`

Replace `send_request`:

- Serialize requests with `rmp_serde::to_vec` instead of `serde_json::to_vec`.
- Write 4-byte length prefix + payload.
- Read 4-byte length prefix + payload, deserialize with `rmp_serde::from_slice`.
- The client currently builds requests as `serde_json::json!()` macro calls and parses responses by hand-walking `serde_json::Value` fields. Two options:
  - **Option A:** Keep building `serde_json::Value` objects, serialize them with `rmp_serde::to_vec` (rmp-serde handles `serde_json::Value` natively). Response parsing stays the same against `serde_json::Value`, just deserialized from MessagePack instead of JSON.
  - **Option B:** Construct typed `Request` variants directly and deserialize into typed `Response` variants. Cleaner, but a larger diff.

Option A is the minimal change. Option B is better long-term.

### `crates/ferridyn-server/tests/integration.rs`

Update test helpers to use the new framing.

### `crates/ferridyn-console/` (if applicable)

If the console connects via the server, its socket communication would also need updating. Currently the console operates directly against an embedded `FerridynDB`, so this likely does not apply.

## Trade-offs

| Dimension | JSON (current) | MessagePack (proposed) |
|---|---|---|
| **Wire size** | Larger — keys and values are UTF-8 text | ~30-40% smaller for typical documents |
| **CPU cost** | JSON parse/serialize is well-optimized | MessagePack is generally faster to serialize/deserialize |
| **Debuggability** | Human-readable; can use `socat`, `jq`, `echo` to interact with the socket | Binary; requires tooling (e.g., `msgpack-cli`, custom script) to inspect traffic |
| **Framing complexity** | Newline delimiter — trivial | Length-prefix — slightly more code, but standard and robust |
| **Ecosystem** | Every language has JSON | Most languages have MessagePack libraries, but coverage is narrower |
| **Consistency** | Different from on-disk format (MessagePack) | Same format end-to-end |

### When JSON is better

- During development/debugging, being able to `echo '{"op":"list_tables"}' | socat - UNIX-CONNECT:/path` is convenient.
- If third-party clients in scripting languages need to talk to the server, JSON has zero-dependency support everywhere.

### When MessagePack is better

- The primary consumers are programmatic clients (e.g., ferridyn-memory plugin) where human readability of the wire format is irrelevant.
- Large document payloads benefit from smaller wire size.
- Avoiding the JSON encode/decode step when documents are already stored as MessagePack internally.

## Migration Path

### Option 1: Hard cut-over (recommended for now)

Since ferridyn-server is pre-1.0 with no external consumers beyond the first-party client, switch the protocol in a single change. Both server and client ship together.

### Option 2: Negotiated protocol

Add a handshake where the client sends a magic byte on connect indicating the desired encoding (`0x01` = JSON, `0x02` = MessagePack). The server reads this byte and dispatches accordingly. This is more complex but allows a gradual transition or mixed clients.

Option 1 is appropriate given the project's current stage.

## Open Questions

1. **Maximum message size** — Should there be an enforced cap below 4 GiB to prevent accidental memory exhaustion from a malformed length prefix? A reasonable default might be 64 MiB.
2. **Error responses** — Should error responses remain human-readable (JSON) for easier debugging, while only data responses use MessagePack? This adds complexity and is probably not worth it.
