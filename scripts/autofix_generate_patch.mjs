#!/usr/bin/env node
/**
 * AutoFix: generates patches/latest.patch by:
 *  - running project checks (lint/typecheck/test/build/dist if present),
 *  - asking OpenAI for a unified diff patch when a check fails,
 *  - applying patch, re-running checks,
 *  - when all pass -> writes git diff to patches/latest.patch and resets working tree.
 *
 * Requirements: Node 20+, git, npm dependencies installed (npm ci already done).
 */

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";

function argValue(name, fallback = "") {
  const idx = process.argv.indexOf(name);
  if (idx === -1) return fallback;
  const v = process.argv[idx + 1];
  return v ?? fallback;
}

const LOGS_DIR = argValue("--logs-dir", "_ci_logs");
const RUN_ID = argValue("--run-id", "");
const EXPECT_SHA = argValue("--sha", "");
const TRIGGER_WORKFLOW = argValue("--trigger-workflow", "");

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_MODEL = process.env.OPENAI_MODEL || "";

function run(cmd, args, opts = {}) {
  return new Promise((resolve) => {
    const p = spawn(cmd, args, {
      shell: false,
      windowsHide: true,
      ...opts,
    });

    let out = "";
    let err = "";

    p.stdout?.on("data", (d) => (out += d.toString()));
    p.stderr?.on("data", (d) => (err += d.toString()));

    p.on("close", (code) => {
      resolve({ code: code ?? 1, out, err });
    });
  });
}

async function mustRun(cmd, args, opts = {}) {
  const r = await run(cmd, args, opts);
  if (r.code !== 0) {
    const e = new Error(`Command failed: ${cmd} ${args.join(" ")}`);
    e.out = r.out;
    e.err = r.err;
    e.code = r.code;
    throw e;
  }
  return r;
}

async function readTextFileSafe(fp, maxBytes = 200_000) {
  try {
    const st = await fsp.stat(fp);
    if (!st.isFile()) return "";
    const size = Math.min(st.size, maxBytes);
    const fd = await fsp.open(fp, "r");
    const buf = Buffer.alloc(size);
    await fd.read(buf, 0, size, 0);
    await fd.close();
    return buf.toString("utf8");
  } catch {
    return "";
  }
}

async function collectLogs(dir, maxChars = 30_000) {
  let acc = "";
  async function walk(d) {
    let entries = [];
    try {
      entries = await fsp.readdir(d, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entries) {
      const p = path.join(d, e.name);
      if (e.isDirectory()) await walk(p);
      else if (e.isFile()) {
        // only text-ish
        if (!/\.(log|txt|md)$/i.test(e.name)) continue;
        const t = await readTextFileSafe(p, 120_000);
        if (!t.trim()) continue;
        acc += `\n\n===== ${p} =====\n${t}`;
        if (acc.length > maxChars) {
          acc = acc.slice(-maxChars);
          return;
        }
      }
    }
  }
  await walk(dir);
  return acc.trim();
}

async function getHeadSha() {
  const r = await mustRun("git", ["rev-parse", "HEAD"]);
  return r.out.trim();
}

async function ensureShaMatches(expected) {
  if (!expected) return;
  const actual = await getHeadSha();
  if (actual !== expected) {
    throw new Error(`HEAD SHA mismatch. expected=${expected} actual=${actual}`);
  }
}

async function getScriptsList() {
  const raw = await fsp.readFile("package.json", "utf8");
  const pkg = JSON.parse(raw);
  const scripts = pkg.scripts || {};

  const steps = [];

  if (scripts.lint) steps.push({ name: "lint", cmd: "npm", args: ["run", "lint"] });
  if (scripts.typecheck) steps.push({ name: "typecheck", cmd: "npm", args: ["run", "typecheck"] });

  // tests are optional; if they exist, run them
  if (scripts.test) steps.push({ name: "test", cmd: "npm", args: ["run", "test"] });

  // build/dist: prefer dist, fallback to build
  if (scripts.dist) steps.push({ name: "dist", cmd: "npm", args: ["run", "dist"] });
  else if (scripts.build) steps.push({ name: "build", cmd: "npm", args: ["run", "build"] });

  return steps;
}

async function runChecksOnce() {
  const steps = await getScriptsList();
  if (steps.length === 0) {
    return { ok: true, failedStep: "", output: "No scripts to run (package.json scripts empty)" };
  }

  for (const s of steps) {
    const r = await run(s.cmd, s.args, { env: process.env });
    const combined = `${r.out}\n${r.err}`.trim();
    if (r.code !== 0) {
      const tail = combined.length > 14_000 ? combined.slice(-14_000) : combined;
      return { ok: false, failedStep: s.name, output: tail || `${s.name} failed with code ${r.code}` };
    }
  }

  return { ok: true, failedStep: "", output: "All checks passed" };
}

function extractTouchedFilesFromPatch(patchText) {
  const files = [];
  const re = /^\+\+\+\s+b\/(.+)$/gm;
  let m;
  while ((m = re.exec(patchText)) !== null) {
    files.push(m[1].trim());
  }
  return files;
}

function patchTouchesForbiddenPaths(patchText) {
  const files = extractTouchedFilesFromPatch(patchText);
  const forbidden = [
    "patches/latest.patch",
    ".github/workflows/",
    "scripts/autofix_generate_patch.mjs",
  ];
  return files.some((f) => forbidden.some((x) => f.startsWith(x)));
}

async function openaiPatch(prompt) {
  if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY is missing");
  if (!OPENAI_MODEL) throw new Error("OPENAI_MODEL is missing");

  const body = {
    model: OPENAI_MODEL,
    input: prompt,
    temperature: 0.2,
    max_output_tokens: 1800,
  };

  const res = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const json = await res.json();
  if (!res.ok) {
    throw new Error(`OpenAI error: ${res.status} ${JSON.stringify(json).slice(0, 500)}`);
  }

  // Try common places for text
  if (typeof json.output_text === "string" && json.output_text.trim()) return json.output_text.trim();

  let text = "";
  if (Array.isArray(json.output)) {
    for (const item of json.output) {
      if (!item) continue;
      const content = item.content;
      if (Array.isArray(content)) {
        for (const c of content) {
          const t = c?.text;
          if (typeof t === "string") text += t;
        }
      }
    }
  }

  return text.trim();
}

async function applyPatchText(patchText) {
  const tmp = "._autofix_model.patch";
  await fsp.writeFile(tmp, patchText, "utf8");
  const r = await run("git", ["apply", "--whitespace=nowarn", tmp]);
  await fsp.unlink(tmp).catch(() => {});
  return { ok: r.code === 0, out: r.out, err: r.err };
}

async function getRelevantFilesSnippet(errorText, maxFiles = 6, maxChars = 18_000) {
  // naive: grab paths like src/... or ...\file.ts from error output
  const candidates = new Set();

  const re1 = /([A-Za-z0-9_.-]+\/)+[A-Za-z0-9_.-]+\.(ts|tsx|js|jsx|css|scss|json)/g;
  const re2 = /([A-Za-z0-9_.-]+\\)+[A-Za-z0-9_.-]+\.(ts|tsx|js|jsx|css|scss|json)/g;

  for (const re of [re1, re2]) {
    let m;
    while ((m = re.exec(errorText)) !== null) {
      const p = m[0].replace(/\\/g, "/");
      if (p.startsWith("node_modules/")) continue;
      candidates.add(p);
      if (candidates.size >= maxFiles) break;
    }
  }

  let acc = "";
  for (const fp of Array.from(candidates).slice(0, maxFiles)) {
    const abs = path.resolve(fp);
    if (!fs.existsSync(abs)) continue;
    const t = await readTextFileSafe(abs, 50_000);
    if (!t.trim()) continue;
    acc += `\n\n===== FILE: ${fp} =====\n${t}`;
    if (acc.length > maxChars) {
      acc = acc.slice(0, maxChars);
      break;
    }
  }
  return acc.trim();
}

async function writeLatestPatchFromDiff() {
  const diff = await mustRun("git", ["diff"]);
  const patchText = diff.out.trim();
  await fsp.mkdir("patches", { recursive: true });
  await fsp.writeFile("patches/latest.patch", patchText ? patchText + "\n" : "", "utf8");
  return patchText.length;
}

async function hardReset() {
  await mustRun("git", ["reset", "--hard"]);
  await mustRun("git", ["clean", "-fd"]);
}

function buildPrompt(params) {
  const {
    headSha,
    runId,
    failedStep,
    failureOutput,
    logsTail,
    filesSnippet,
    applyError,
    existingDiff,
  } = params;

  return [
    `You are a senior maintainer. Produce ONLY a unified diff patch (git apply compatible). No commentary.`,
    `Repo: Ozonator`,
    `Target commit (HEAD): ${headSha}`,
    runId ? `CI run id: ${runId}` : "",
    `Goal: fix the failure so checks pass.`,
    `Constraints:`,
    `- Patch must be a unified diff starting with 'diff --git'.`,
    `- Keep changes minimal.`,
    `- Do NOT modify: patches/latest.patch, .github/workflows/*, scripts/autofix_generate_patch.mjs`,
    `Failure context:`,
    `- Failed step: ${failedStep}`,
    `- Output tail:\n${failureOutput}`,
    logsTail ? `\nCI logs tail:\n${logsTail}` : "",
    filesSnippet ? `\nRelevant files:\n${filesSnippet}` : "",
    existingDiff ? `\nCurrent uncommitted diff (do NOT revert; build on top of this):\n${existingDiff}` : "",
    applyError ? `\nPrevious patch apply error:\n${applyError}` : "",
    `\nReturn ONLY the patch.`,
  ].filter(Boolean).join("\n");
}


function truncateMiddle(s, maxChars = 18_000) {
  const t = (s || "").toString();
  if (t.length <= maxChars) return t;
  const head = Math.floor(maxChars * 0.55);
  const tail = maxChars - head - 40;
  return `${t.slice(0, head)}\n\n... [truncated ${t.length - head - tail} chars] ...\n\n${t.slice(-tail)}`;
}

async function getCurrentDiff(maxChars = 18_000) {
  const r = await run("git", ["diff"]);
  return truncateMiddle((r.out || "").trim(), maxChars);
}

async function getFilesContextForPatch(patchText, maxFiles = 6, maxChars = 18_000) {
  const files = extractTouchedFilesFromPatch(patchText)
    .filter((f) => f && !f.startsWith("node_modules/") && fs.existsSync(f))
    .slice(0, maxFiles);

  let acc = "";
  for (const fp of files) {
    const t = await readTextFileSafe(fp, 60_000);
    if (!t.trim()) continue;
    acc += `\n\n===== FILE: ${fp} (current) =====\n${t}`;
    if (acc.length > maxChars) {
      acc = acc.slice(0, maxChars);
      break;
    }
  }
  return acc.trim();
}

function shouldRebaseLatestPatch({ triggerWorkflow, logsTail, latestPatchText }) {
  if (!latestPatchText || !latestPatchText.trim()) return false;
  const hay = `${triggerWorkflow || ""}\n${logsTail || ""}`.toLowerCase();
  if (hay.includes("apply patch and open pr")) return true;
  if (hay.includes("patch does not apply")) return true;
  if (hay.includes("patch failed")) return true;
  if (hay.includes("git apply")) return true;
  return false;
}

function buildRebasePrompt(params) {
  const {
    headSha,
    runId,
    originalPatch,
    applyError,
    logsTail,
    filesSnippet,
  } = params;

  return [
    `You are a senior maintainer. Produce ONLY a unified diff patch (git apply compatible). No commentary.`,
    `Repo: Ozonator`,
    `Target commit (HEAD): ${headSha}`,
    runId ? `CI run id: ${runId}` : "",
    `Goal: update/rebase the PROVIDED patch so it applies cleanly to this HEAD, preserving its intent.`,
    `Constraints:`,
    `- Patch must be a unified diff starting with 'diff --git'.`,
    `- Keep changes minimal; preserve the original intent.`,
    `- Do NOT modify: patches/latest.patch, .github/workflows/*, scripts/autofix_generate_patch.mjs`,
    `Original patch (may be truncated):\n${originalPatch}`,
    applyError ? `\nCurrent git apply error:\n${applyError}` : "",
    logsTail ? `\nCI logs tail:\n${logsTail}` : "",
    filesSnippet ? `\nCurrent file snapshots:\n${filesSnippet}` : "",
    `\nReturn ONLY the patch.`,
  ].filter(Boolean).join("\n");
}

async function main() {
  await ensureShaMatches(EXPECT_SHA);

  const headSha = await getHeadSha();
  const logsTail = await collectLogs(LOGS_DIR, 30_000);

  const latestPatchPath = path.join("patches", "latest.patch");
  const latestPatchText = await readTextFileSafe(latestPatchPath, 200_000);
  const wantsRebase = shouldRebaseLatestPatch({
    triggerWorkflow: TRIGGER_WORKFLOW,
    logsTail,
    latestPatchText,
  });
  let rebasePending = wantsRebase && latestPatchText.trim().length > 0;
  let rebaseApplied = false;

  let applyError = "";
  for (let attempt = 1; attempt <= 3; attempt++) {
    if (rebasePending && !rebaseApplied) {
      // The failing workflow likely indicates patches/latest.patch no longer applies.
      // Try to rebase it onto the current HEAD, preserving intent.
      if (!latestPatchText.includes("diff --git")) {
        applyError = "patches/latest.patch is not a unified diff (missing 'diff --git').";
      } else if (patchTouchesForbiddenPaths(latestPatchText)) {
        throw new Error("patches/latest.patch touches forbidden paths (workflows/scripts/patches).");
      }

      const appliedOrig = await applyPatchText(latestPatchText);
      if (!appliedOrig.ok) {
        applyError = (appliedOrig.err || appliedOrig.out || "git apply failed").slice(-6_000);
        const filesSnippetForRebase = await getFilesContextForPatch(latestPatchText);
        const prompt = buildRebasePrompt({
          headSha,
          runId: RUN_ID,
          originalPatch: truncateMiddle(latestPatchText.trimEnd(), 40_000),
          applyError,
          logsTail,
          filesSnippet: filesSnippetForRebase,
        });

        const rebasedPatch = await openaiPatch(prompt);

        if (!rebasedPatch.includes("diff --git")) {
          applyError = "Model output is not a unified diff (missing 'diff --git').";
          continue;
        }

        if (patchTouchesForbiddenPaths(rebasedPatch)) {
          applyError = "Patch touches forbidden paths (.github/workflows/, scripts/autofix..., or patches/latest.patch).";
          continue;
        }

        const appliedRebased = await applyPatchText(rebasedPatch);
        if (!appliedRebased.ok) {
          applyError = (appliedRebased.err || appliedRebased.out || "git apply failed").slice(-6_000);
          continue;
        }
      }

      rebaseApplied = true;
      rebasePending = false;
      applyError = "";
    }
    const check = await runChecksOnce();
    if (check.ok) {
      const n = await writeLatestPatchFromDiff();
      // We purposely do NOT keep code changes in this workflow: latest.patch is the deliverable.
      await hardReset();
      if (n === 0) {
        console.log("✅ Checks already pass. No patch needed (patches/latest.patch will be empty).");
      } else {
        console.log(`✅ Checks pass after fixes. Wrote patches/latest.patch (${n} chars).`);
      }
      return;
    }

    const filesSnippet = await getRelevantFilesSnippet(check.output);
    const existingDiff = await getCurrentDiff(18_000);
    const prompt = buildPrompt({
      headSha,
      runId: RUN_ID,
      failedStep: check.failedStep,
      failureOutput: check.output,
      logsTail,
      filesSnippet,
      applyError,
      existingDiff,
    });

    const patchText = await openaiPatch(prompt);

    if (!patchText.includes("diff --git")) {
      applyError = "Model output is not a unified diff (missing 'diff --git').";
      continue;
    }

    if (patchTouchesForbiddenPaths(patchText)) {
      applyError = "Patch touches forbidden paths (.github/workflows/, scripts/autofix..., or patches/latest.patch).";
      continue;
    }

    const applied = await applyPatchText(patchText);
    if (!applied.ok) {
      applyError = (applied.err || applied.out || "git apply failed").slice(-6_000);
      continue;
    }

    applyError = ""; // applied ok, next loop will re-run checks
  }

  throw new Error("AutoFix failed after 3 attempts (could not produce an applicable patch that makes checks pass).");
}

main().catch((e) => {
  console.error("❌ AutoFix error:", e?.message || e);
  process.exit(1);
});
