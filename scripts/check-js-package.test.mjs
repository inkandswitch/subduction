// Fixture tests for package selection and all four Node entrypoints. Real Wasm
// initialization is covered by the packed-package checks in the same workflow.
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

const checker = fileURLToPath(new URL("./check-js-package.mjs", import.meta.url));
const entries = ["node.mjs", "node.cjs", "debug-node.mjs", "debug-node.cjs"];
const packages = ["@automerge/sedimentree", "@automerge/subduction", "@automerge/automerge-subduction"];

function fixture(t, name, brokenEntry) {
  const root = mkdtempSync(join(tmpdir(), "subduction-package-check-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  mkdirSync(join(root, "dist"));
  const calls = join(root, "calls.log");
  writeFileSync(join(root, "package.json"), JSON.stringify({
    name,
    version: "0.0.0",
    type: "module",
    main: "./dist/node.cjs",
    module: "./dist/node.mjs",
    types: "./dist/index.d.ts",
    exports: {
      ".": { import: "./dist/node.mjs", require: "./dist/node.cjs" },
      "./debug": { import: "./dist/debug-node.mjs", require: "./dist/debug-node.cjs" },
    },
  }));
  writeFileSync(join(root, "README.md"), "Test fixture\n");
  writeFileSync(join(root, "dist/index.d.ts"), "export {};\n");
  for (const entry of entries) {
    const esm = entry.endsWith(".mjs");
    const prelude = esm
      ? 'import { appendFileSync } from "node:fs";'
      : 'const { appendFileSync } = require("node:fs");';
    const record = `function record() { appendFileSync(${JSON.stringify(calls)}, ${JSON.stringify(entry + "\n")}); }`;
    let api, exports;
    if (name === "@automerge/sedimentree") {
      api = `
        function setSubductionLogLevel(level) {
          if (level !== "error") throw new Error("invalid level");
          record();
        }
        function set_subduction_logger(callback) {
          if (typeof callback !== "function") throw new Error("invalid logger");
        }
        function clear_subduction_logger() {}
      `;
      exports = ["setSubductionLogLevel", "set_subduction_logger", "clear_subduction_logger"];
      if (entry === brokenEntry) exports = exports.filter(name => name !== "setSubductionLogLevel");
    } else {
      api = `
        function createMemorySigner() { return { peerId: () => "33".repeat(32) }; }
        function createMemoryStorage() { return {}; }
        class Subduction {
          async storeCommit(tree, commit) { record(); this.head = commit.head; }
          async getHeads() { return ${entry === brokenEntry ? "[]" : "[this.head]"}; }
          async close() {}
          free() {}
        }
      `;
      exports = ["Subduction", "createMemorySigner", "createMemoryStorage"];
    }
    const expose = esm
      ? `export { ${exports.join(", ")} };`
      : `module.exports = { ${exports.join(", ")} };`;
    writeFileSync(join(root, "dist", entry), `${prelude}\n${record}\n${api}\n${expose}\n`);
  }
  return { root, calls };
}

function check(root, name) {
  return spawnSync(process.execPath, [checker, root, name], { encoding: "utf8", timeout: 10_000 });
}

for (const name of packages) {
  test(`${name}: checks release/debug in ESM and CJS`, t => {
    const { root, calls } = fixture(t, name);
    const result = check(root, name);
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stderr);
    assert.deepEqual(readFileSync(calls, "utf8").trim().split("\n").sort(), [...entries].sort());
  });
  for (const entry of entries) {
    test(`${name}: rejects a broken ${entry}`, t => {
      const { root } = fixture(t, name, entry);
      const result = check(root, name);
      assert.ifError(result.error);
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, /package smoke test failed/);
    });
  }
}

test("unknown packages require an explicit smoke-test policy", t => {
  const name = "@automerge/unknown";
  const { root } = fixture(t, name);
  const result = check(root, name);
  assert.ifError(result.error);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /No smoke test defined/);
});
