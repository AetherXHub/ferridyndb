// Shared utilities for DynaMite memory hooks.
// Zero npm dependencies — Node.js built-ins only.

import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// Plugin root is one directory up from scripts/.
const PLUGIN_ROOT = resolve(import.meta.dirname, "..");

// CLI binary: env var override, or resolve relative to plugin root.
// The plugin sits at <project>/plugin/, so the binary is at <project>/target/release/.
const CLI_BIN =
  process.env.DYNAMITE_MEMORY_CLI ||
  resolve(PLUGIN_ROOT, "..", "target", "release", "dynamite-memory-cli");

// ---------------------------------------------------------------------------
// CLI runner
// ---------------------------------------------------------------------------

/**
 * Run dynamite-memory-cli with the given arguments.
 * Returns parsed JSON from stdout.
 */
export function runCli(args, { timeout = 10_000 } = {}) {
  return new Promise((resolve, reject) => {
    execFile(CLI_BIN, args, { timeout }, (err, stdout, stderr) => {
      if (err) {
        reject(new Error(`CLI error: ${err.message}\nstderr: ${stderr}`));
        return;
      }
      try {
        resolve(JSON.parse(stdout));
      } catch {
        // Some commands produce no JSON output (e.g. remember/forget).
        resolve(stdout.trim());
      }
    });
  });
}

// ---------------------------------------------------------------------------
// LLM helper — Anthropic API with claude CLI fallback
// ---------------------------------------------------------------------------

/**
 * Call a small Claude model for lightweight inference.
 * Tries ANTHROPIC_API_KEY first, then falls back to `claude -p`.
 * Returns the text response, or null if both methods fail.
 */
export async function callHaiku(systemPrompt, userMessage) {
  // Method 1: Direct API call
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (apiKey) {
    try {
      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
          "x-api-key": apiKey,
          "anthropic-version": "2023-06-01",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          model: "claude-haiku-4-20250414",
          max_tokens: 2048,
          system: systemPrompt,
          messages: [{ role: "user", content: userMessage }],
        }),
      });
      if (res.ok) {
        const data = await res.json();
        const text = data.content?.[0]?.text;
        if (text) return text;
      }
    } catch {
      // Fall through to next method.
    }
  }

  // Method 2: claude CLI
  try {
    const text = await new Promise((resolve, reject) => {
      execFile(
        "claude",
        ["-p", "--model", "haiku", "--no-input"],
        { timeout: 30_000 },
        (err, stdout) => {
          if (err) reject(err);
          else resolve(stdout.trim());
        },
      );
    });
    if (text) return text;
  } catch {
    // Fall through.
  }

  return null;
}

// ---------------------------------------------------------------------------
// JSON extraction from LLM responses
// ---------------------------------------------------------------------------

/**
 * Extract a JSON array or object from text that may contain markdown fences.
 */
export function parseJsonFromText(text) {
  if (!text) return null;

  // Try direct parse first.
  try {
    return JSON.parse(text);
  } catch {
    // continue
  }

  // Try extracting from markdown code fence.
  const fenceMatch = text.match(/```(?:json)?\s*\n?([\s\S]*?)\n?```/);
  if (fenceMatch) {
    try {
      return JSON.parse(fenceMatch[1]);
    } catch {
      // continue
    }
  }

  // Try finding array or object boundaries.
  const jsonMatch = text.match(/[\[{][\s\S]*[}\]]/);
  if (jsonMatch) {
    try {
      return JSON.parse(jsonMatch[0]);
    } catch {
      // give up
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Stdin reader
// ---------------------------------------------------------------------------

/**
 * Read all of stdin and parse as JSON.
 */
export async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) {
    chunks.push(chunk);
  }
  const raw = Buffer.concat(chunks).toString("utf-8");
  return JSON.parse(raw);
}

// ---------------------------------------------------------------------------
// Transcript reader
// ---------------------------------------------------------------------------

/**
 * Read the last N entries from a JSONL transcript file.
 */
export async function readTranscriptTail(transcriptPath, maxEntries = 50) {
  try {
    const content = await readFile(transcriptPath, "utf-8");
    const lines = content.trim().split("\n").filter(Boolean);
    const tail = lines.slice(-maxEntries);
    return tail.map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return { raw: line };
      }
    });
  } catch {
    return [];
  }
}
