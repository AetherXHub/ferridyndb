---
name: setup
description: Build and configure the DynaMite memory system — binaries, server, MCP tools, and hooks. Run this once to bootstrap everything.
allowed-tools: Bash, Read, Write, Edit, Glob, Grep
user-invocable: true
---

# DynaMite Memory Setup

You are setting up the DynaMite memory system. Follow each step in order. If a step fails, report the error and stop.

## Step 1: Build release binaries

Run from the project root:

```bash
cargo build --release -p dynamite-server -p dynamite-memory
```

Confirm the binaries exist:
- `target/release/dynamite-server`
- `target/release/dynamite-memory`
- `target/release/dynamite-memory-cli`

## Step 2: Create data directory

```bash
mkdir -p ~/.local/share/dynamite
```

## Step 3: Start the DynaMite server

Check if the server is already running by testing the socket:

```bash
ls -la ~/.local/share/dynamite/server.sock 2>/dev/null
```

If the socket exists, test if the server is responsive by running:

```bash
target/release/dynamite-memory-cli discover
```

If the CLI hangs or the socket doesn't exist, start the server:

```bash
rm -f ~/.local/share/dynamite/server.sock
nohup target/release/dynamite-server \
  --db ~/.local/share/dynamite/memory.db \
  --socket ~/.local/share/dynamite/server.sock \
  > /dev/null 2>&1 &
```

Wait 1 second then verify the socket was created.

## Step 4: Verify the CLI

Run:

```bash
target/release/dynamite-memory-cli discover
```

This should return `[]` (empty array) or a list of categories. If it hangs, the server failed to start — check stderr.

## Step 5: Test round-trip

Store a test memory, recall it, then clean up:

```bash
target/release/dynamite-memory-cli remember --category _setup-test --key check --content "Setup verification"
target/release/dynamite-memory-cli recall --category _setup-test
target/release/dynamite-memory-cli forget --category _setup-test --key check
```

All three commands should succeed.

## Step 6: Report status

Tell the user:

1. **Server**: running at `~/.local/share/dynamite/server.sock`, database at `~/.local/share/dynamite/memory.db`
2. **MCP tools**: `remember`, `recall`, `discover`, `forget` — available via the dynamite-memory MCP server
3. **Hooks**: UserPromptSubmit (auto-retrieval) and PreCompact (auto-save) configured
4. **CLI**: `target/release/dynamite-memory-cli` for direct access

If an `ANTHROPIC_API_KEY` is set, the hooks will use Claude Haiku for intelligent memory selection and extraction. Otherwise they fall back to fetching all memories.
