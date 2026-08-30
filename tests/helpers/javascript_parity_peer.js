import { createXO, closeXO } from "../../js/xo.js";

const config = JSON.parse(process.argv[2]);
let resolveReady;
let rejectReady;
const ready = new Promise((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
const timeout = setTimeout(() => rejectReady(new Error("JavaScript peer did not become ready")), 5000);
const xo = createXO({
  url: config.url,
  namespace: config.namespace,
  token: config.token,
  prefixes: [[]],
  writable: true,
  reconnect: false,
  onState(change) {
    if (change.state === "ready") { clearTimeout(timeout); resolveReady(); }
    if (change.state === "disconnected") rejectReady(change.error ?? new Error("disconnected"));
  },
});

function plain(value) {
  if (value instanceof Uint8Array) return { bytes: [...value] };
  if (Array.isArray(value)) return value.map(plain);
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, plain(item)]));
  return value;
}

async function main() {
  await ready;
  const before = {
    revision: xo.revision,
    parent: xo.shared.value,
    counter: xo.shared.counter.value,
    clearable: xo.shared.clearable.value,
    keys: [...xo.shared],
    bytes: plain(xo.shared.pythonBytes.value),
    tuple: plain(xo.shared.pythonTuple.value),
  };

  await xo.transaction([
    { kind: "set", path: "shared.counter", value: 2 },
    { kind: "set", path: "shared.fromJs", value: { language: "javascript", bytes: new Uint8Array([4, 5, 6]) } },
    { kind: "clear", path: "shared.clearable" },
    { kind: "delete", path: "shared.deletable" },
  ]);
  await xo.shared.restored.restore({
    $value: "js-restored",
    $children: [["child", { $value: 9, $children: [] }]],
  });

  process.stdout.write(`${JSON.stringify({ before, afterRevision: xo.revision })}\n`);
  process.stdin.resume();
  await new Promise((resolve) => process.stdin.once("data", resolve));
  const deadline = Date.now() + 3000;
  while ((xo.shared.pythonAfterJs.value?.status !== "seen" || xo.shared.counter.value !== 3) && Date.now() < deadline) {
    await Bun.sleep(5);
  }
  if (xo.shared.pythonAfterJs.value?.status !== "seen" || xo.shared.counter.value !== 3 || xo.revision !== 6) {
    throw new Error(`Python post-write did not converge in JavaScript at revision ${xo.revision}`);
  }
  process.stdout.write(`${JSON.stringify({ finalRevision: xo.revision, pythonAfterJs: xo.shared.pythonAfterJs.value, counter: xo.shared.counter.value })}\n`);
  await new Promise((resolve) => process.stdin.once("end", resolve));
  closeXO(xo);
}

main().catch((error) => {
  closeXO(xo);
  console.error(error?.stack ?? String(error));
  process.exitCode = 1;
});
