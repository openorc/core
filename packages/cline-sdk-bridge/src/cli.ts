#!/usr/bin/env node
/** D3 stdio entrypoint: long-lived JSON-RPC 2.0 bridge over stdin/stdout.
 *
 * Console output is silenced before any SDK import so stdout carries JSON-RPC
 * protocol frames only — no banners, prompts, progress text, or SDK
 * `console.log` output. The bridge writes nothing to stderr itself; uncaught
 * crashes surface through the process exit status, which D4 supervises.
 * End-of-input performs a best-effort local `dispose` (bounded below) and
 * exits 0; that releases bridge/client resources only and never deletes or
 * replaces an external Task session.
 */

const silence = (): void => undefined;
console.log = silence;
console.info = silence;
console.warn = silence;
console.error = silence;
console.debug = silence;
console.trace = silence;

const EXIT_DISPOSE_BUDGET_MS = 5000;

async function main(): Promise<void> {
  const [{ BridgeCore }, { createRealClientFactory }] = await Promise.all([
    import("./bridge.js"),
    import("./sdk-client.js"),
  ]);
  const core = new BridgeCore({
    clientFactory: createRealClientFactory(),
    writeFrame: (frame) => {
      process.stdout.write(`${JSON.stringify(frame)}\n`);
    },
  });
  process.stdin.on("data", (chunk) => {
    core.handleData(chunk);
  });
  process.stdin.on("end", () => {
    core.handleEnd();
    const exit = (): void => {
      process.exit(0);
    };
    void Promise.race([
      core.disposeForExit(),
      new Promise<void>((resolve) => {
        setTimeout(resolve, EXIT_DISPOSE_BUDGET_MS);
      }),
    ]).then(exit, exit);
  });
}

void main();