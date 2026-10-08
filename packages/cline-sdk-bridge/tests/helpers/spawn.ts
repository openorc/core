/** Child-process driver for stdio framing tests: spawns `node
 * tests/spawn-harness.mjs` (built bridge core + scripted fake) or the real
 * `dist/cli.js` entrypoint, writes raw stdin bytes, and captures stdout
 * frames and stderr verbatim for assertions. */

import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { JsonObject } from "../../src/types.js";

export interface SpawnedBridge {
  child: ChildProcessWithoutNullStreams;
  /** Complete JSON objects parsed from stdout lines, in arrival order. */
  readonly stdoutFrames: JsonObject[];
  /** Complete stdout lines, in arrival order (before parsing). */
  readonly stdoutLines: string[];
  /** Everything the child wrote to stderr, verbatim. */
  readonly stderr: string;
  /** Resolve when the child exits; resolves with its exit code. */
  readonly exited: Promise<number | null>;
  /** Current exit status (undefined while running). */
  readonly exitStatus: number | null | undefined;
  write(text: string): void;
  end(): Promise<void>;
  cleanup(): Promise<void>;
}

export interface HarnessScript {
  outcomes?: Record<string, Array<Record<string, unknown>>>;
  events?: Array<Record<string, unknown>>;
}

export async function spawnHarness(script: HarnessScript): Promise<SpawnedBridge> {
  const dir = await mkdtemp(join(tmpdir(), "cline-sdk-bridge-test-"));
  const scriptPath = join(dir, "script.json");
  const recordPath = join(dir, "record.jsonl");
  await writeFile(scriptPath, JSON.stringify(script), "utf8");
  const child = spawn(process.execPath, [harnessScriptPath()], {
    cwd: packageRoot(),
    env: {
      ...process.env,
      OPENORC_BRIDGE_FAKE_SCRIPT: scriptPath,
      OPENORC_BRIDGE_FAKE_RECORD: recordPath,
    },
    stdio: ["pipe", "pipe", "pipe"],
  });
  return trackChild(child, dir, recordPath);
}

/** Spawn the REAL built entrypoint (imports the pinned SDK, no attach). */
export async function spawnRealEntrypoint(): Promise<SpawnedBridge> {
  const dir = await mkdtemp(join(tmpdir(), "cline-sdk-bridge-entry-"));
  const child = spawn(process.execPath, [join(packageRoot(), "dist", "cli.js")], {
    cwd: packageRoot(),
    env: { ...process.env },
    stdio: ["pipe", "pipe", "pipe"],
  });
  return trackChild(child, dir, null);
}

/** Absolute path of the package root: bridge tests always run via `npm test`
 * from the package directory (matching the CI job's working directory). */
function packageRoot(): string {
  return process.cwd();
}

/** Absolute path of the subprocess harness script. */
function harnessScriptPath(): string {
  return join(process.cwd(), "tests", "spawn-harness.mjs");
}

function trackChild(child: ChildProcessWithoutNullStreams, tempDir: string, recordPath: string | null): SpawnedBridge {
  const stdoutFrames: JsonObject[] = [];
  const stdoutLines: string[] = [];
  let stderrText = "";
  let buffer = "";
  let exitCode: number | null | undefined;
  let exitedResolve: ((code: number | null) => void) | undefined;
  const exited = new Promise<number | null>((resolve) => {
    exitedResolve = resolve;
  });

  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    buffer += chunk;
    let newlineAt = buffer.indexOf("\n");
    while (newlineAt !== -1) {
      const line = buffer.slice(0, newlineAt).replace(/\r$/, "");
      buffer = buffer.slice(newlineAt + 1);
      if (line.length > 0) {
        stdoutLines.push(line);
        stdoutFrames.push(JSON.parse(line) as JsonObject);
      }
      newlineAt = buffer.indexOf("\n");
    }
  });
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk: string) => {
    stderrText += chunk;
  });
  child.on("exit", (code) => {
    exitCode = code;
    exitedResolve?.(code);
  });

  void recordPath;
  return {
    child,
    stdoutFrames,
    stdoutLines,
    get stderr(): string {
      return stderrText;
    },
    exited,
    get exitStatus(): number | null | undefined {
      return exitCode;
    },
    write: (text: string) => {
      child.stdin.write(text);
    },
    end: async () => {
      await new Promise<void>((resolve) => {
        child.stdin.end(resolve);
      });
    },
    cleanup: async () => {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill("SIGKILL");
      }
      await rm(tempDir, { recursive: true, force: true });
    },
  };
}

/** Poll until the probe returns a defined value. */
export async function until<T>(probe: () => T | undefined, timeoutMs = 5000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = probe();
    if (value !== undefined) {
      return value;
    }
    if (Date.now() > deadline) {
      throw new Error("condition not met within timeout");
    }
    await new Promise((resolve) => {
      setTimeout(resolve, 10);
    });
  }
}