#!/usr/bin/env node
// UserPromptSubmit hook — inject relevant memories into Claude's context.

import { runCli, callHaiku, parseJsonFromText, readStdin } from "./config.mjs";

// ---------------------------------------------------------------------------
// Prompts
// ---------------------------------------------------------------------------

const RETRIEVAL_PROMPT = `You are a memory retrieval assistant. Given a user prompt and a memory index, select which memory categories and optional prefixes are most relevant to the prompt.

Return a JSON array of objects: [{"category": "...", "prefix": "..."}]
- "prefix" is optional — omit it to fetch all entries in the category.
- Return an empty array [] if no memories are relevant.
- Be selective: only return categories that are clearly related to the prompt.
- Maximum 5 entries.`;

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const input = await readStdin();
  const prompt = input.prompt;

  if (!prompt) {
    // No prompt text — nothing to do.
    process.exit(0);
  }

  // Step 1: Discover all categories.
  let categories;
  try {
    categories = await runCli(["discover"]);
  } catch {
    // CLI not available or no memories — exit silently.
    process.exit(0);
  }

  if (!Array.isArray(categories) || categories.length === 0) {
    // No memories stored yet.
    process.exit(0);
  }

  // Step 2: Build memory index (categories + their prefixes).
  const index = {};
  for (const cat of categories) {
    const catName = typeof cat === "string" ? cat : String(cat);
    try {
      const prefixes = await runCli(["discover", "--category", catName]);
      index[catName] = Array.isArray(prefixes) ? prefixes : [];
    } catch {
      index[catName] = [];
    }
  }

  const indexText = Object.entries(index)
    .map(
      ([cat, pfxs]) =>
        `- ${cat}: [${pfxs.map((p) => (typeof p === "string" ? p : String(p))).join(", ")}]`,
    )
    .join("\n");

  // Step 3: Select relevant memories.
  let selections;

  // Try LLM-based selection first.
  const llmResponse = await callHaiku(
    RETRIEVAL_PROMPT,
    `Memory index:\n${indexText}\n\nUser prompt:\n${prompt}`,
  );
  selections = parseJsonFromText(llmResponse);

  if (!Array.isArray(selections) || selections.length === 0) {
    // Fallback: fetch from all categories (limited to 5 per category).
    selections = categories
      .slice(0, 5)
      .map((c) => ({ category: typeof c === "string" ? c : String(c) }));
  }

  // Step 4: Fetch selected memories.
  const memories = [];
  for (const sel of selections.slice(0, 5)) {
    try {
      const args = ["recall", "--category", sel.category, "--limit", "10"];
      if (sel.prefix) {
        args.push("--prefix", sel.prefix);
      }
      const items = await runCli(args);
      if (Array.isArray(items) && items.length > 0) {
        memories.push({ category: sel.category, prefix: sel.prefix, items });
      }
    } catch {
      // Skip failures.
    }
  }

  if (memories.length === 0) {
    // No relevant memories found.
    process.exit(0);
  }

  // Step 5: Format and output.
  const contextParts = memories.map(({ category, prefix, items }) => {
    const header = prefix ? `${category} (${prefix})` : category;
    const entries = items
      .map((item) => {
        const key = item.key || "?";
        const content = item.content || JSON.stringify(item);
        return `  - [${key}]: ${content}`;
      })
      .join("\n");
    return `## ${header}\n${entries}`;
  });

  const context = `# Recalled Memories\n\n${contextParts.join("\n\n")}`;

  const output = JSON.stringify({
    hookSpecificOutput: {
      hookEventName: "UserPromptSubmit",
      additionalContext: context,
    },
  });

  process.stdout.write(output);
}

main().catch((err) => {
  process.stderr.write(`memory-retrieval error: ${err.message}\n`);
  // Exit 0 so we don't block the prompt.
  process.exit(0);
});
