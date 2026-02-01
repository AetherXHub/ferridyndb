# DynaMite Memory Plugin for Claude Code

Self-contained Claude Code plugin that provides:

1. **MCP server** — `remember`, `recall`, `discover`, `forget` tools available directly in Claude
2. **Auto-retrieval hook** (UserPromptSubmit) — automatically injects relevant memories into context before Claude processes each prompt
3. **Auto-save hook** (PreCompact) — extracts and persists important learnings before conversation compaction
4. **Setup skill** — `/dynamite-memory:setup` bootstraps the entire system

## Setup

Run `/dynamite-memory:setup` in Claude Code. It will:

1. Build the release binaries
2. Start the DynaMite server
3. Verify the CLI and MCP tools work
4. Test a round-trip memory store/recall/forget

## Architecture

```
dynamite-server (background, owns DB file)
    ^ Unix socket (~/.local/share/dynamite/server.sock)
    |
    +-- dynamite-memory (MCP server, provides tools to Claude)
    +-- dynamite-memory-cli (used by hooks for read/write)
```

## Hooks

- **memory-retrieval.mjs** (UserPromptSubmit): Discovers stored memories, selects relevant ones (using Claude Haiku if ANTHROPIC_API_KEY is set, or fetches all as fallback), injects as `additionalContext`
- **memory-commit.mjs** (PreCompact): Reads recent transcript, uses Claude Haiku to extract key learnings, stores them as memories

## Configuration

- `DYNAMITE_MEMORY_CLI` — override CLI binary path
- `DYNAMITE_MEMORY_SOCKET` — override server socket path
- `ANTHROPIC_API_KEY` — enables intelligent memory selection/extraction via Claude Haiku

## Plugin structure

```
plugin/
  .claude-plugin/plugin.json   — plugin metadata
  .mcp.json                    — MCP server declaration
  hooks/hooks.json             — hook configuration
  scripts/
    config.mjs                 — shared utilities
    memory-retrieval.mjs       — auto-retrieval hook
    memory-commit.mjs          — auto-save hook
  skills/setup/SKILL.md        — /dynamite-memory:setup
```
