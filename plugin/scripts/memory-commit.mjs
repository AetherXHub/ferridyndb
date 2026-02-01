#!/usr/bin/env node
// PreCompact hook — extract and persist key memories before context compaction.

import {
  runCli,
  callHaiku,
  parseJsonFromText,
  readStdin,
  readTranscriptTail,
} from "./config.mjs";

// ---------------------------------------------------------------------------
// Prompts
// ---------------------------------------------------------------------------

const COMMIT_PROMPT = `You are a memory extraction assistant. Given a conversation transcript, extract the most important learnings, decisions, and facts that should be remembered for future sessions.

Existing memory categories: {categories}

Return a JSON array of memory entries to store:
[
  {
    "category": "category-name",
    "key": "hierarchical#key#name",
    "content": "The memory content to store"
  }
]

Guidelines:
- Use existing categories when appropriate, or create new descriptive ones.
- Keys should use '#' as a hierarchy separator (e.g., "rust#ownership#rules").
- Content should be concise but self-contained.
- Focus on: architecture decisions, user preferences, bug fixes, learned patterns, project-specific knowledge.
- Skip: trivial exchanges, greetings, status updates.
- Return an empty array [] if nothing worth remembering.
- Maximum 10 entries.`;

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const input = await readStdin();
  const transcriptPath = input.transcript_path;

  if (!transcriptPath) {
    process.exit(0);
  }

  // Step 1: Read recent transcript entries.
  const entries = await readTranscriptTail(transcriptPath, 50);
  if (entries.length === 0) {
    process.exit(0);
  }

  // Step 2: Get existing categories for context.
  let categories = [];
  try {
    const cats = await runCli(["discover"]);
    if (Array.isArray(cats)) {
      categories = cats.map((c) => (typeof c === "string" ? c : String(c)));
    }
  } catch {
    // No existing categories — that's fine.
  }

  // Step 3: Extract conversation text from transcript.
  const conversationText = entries
    .map((entry) => {
      if (entry.role && entry.content) {
        const text =
          typeof entry.content === "string"
            ? entry.content
            : JSON.stringify(entry.content);
        return `[${entry.role}]: ${text.slice(0, 500)}`;
      }
      if (entry.type === "message" && entry.message) {
        const role = entry.message.role || "unknown";
        const content = entry.message.content;
        const text =
          typeof content === "string" ? content : JSON.stringify(content);
        return `[${role}]: ${text.slice(0, 500)}`;
      }
      return null;
    })
    .filter(Boolean)
    .join("\n");

  if (!conversationText.trim()) {
    process.exit(0);
  }

  // Step 4: Use LLM to extract memories.
  const systemPrompt = COMMIT_PROMPT.replace(
    "{categories}",
    categories.length > 0 ? categories.join(", ") : "(none yet)",
  );

  const llmResponse = await callHaiku(
    systemPrompt,
    `Conversation transcript:\n${conversationText}`,
  );
  const memories = parseJsonFromText(llmResponse);

  if (!Array.isArray(memories) || memories.length === 0) {
    process.stderr.write("memory-commit: no memories extracted\n");
    process.exit(0);
  }

  // Step 5: Store each extracted memory.
  let stored = 0;
  for (const mem of memories.slice(0, 10)) {
    if (!mem.category || !mem.key || !mem.content) continue;
    try {
      const args = [
        "remember",
        "--category",
        mem.category,
        "--key",
        mem.key,
        "--content",
        mem.content,
      ];
      if (mem.metadata) {
        args.push("--metadata", mem.metadata);
      }
      await runCli(args);
      stored++;
    } catch (err) {
      process.stderr.write(
        `memory-commit: failed to store ${mem.category}#${mem.key}: ${err.message}\n`,
      );
    }
  }

  process.stderr.write(`memory-commit: stored ${stored} memories\n`);
}

main().catch((err) => {
  process.stderr.write(`memory-commit error: ${err.message}\n`);
  // Exit 0 so we don't block compaction.
  process.exit(0);
});
