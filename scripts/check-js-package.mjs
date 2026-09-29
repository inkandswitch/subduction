// Check the extracted npm tarball, not the working tree's dist/ or node_modules/.
// Usage: node scripts/check-js-package.mjs /path/to/extracted/package
import assert from "node:assert/strict";
import { readFileSync, statSync } from "node:fs";
import { resolve, sep } from "node:path";
import { spawnSync } from "node:child_process";

assert(process.argv[2], "Expected the extracted package directory");
const root = resolve(process.argv[2]);
const pkg = JSON.parse(readFileSync(resolve(root, "package.json"), "utf8"));
assert.equal(pkg.name, "@automerge/subduction");

function checkTarget(target) {
  if (typeof target === "string") {
    assert(target.startsWith("./dist/"), `Unexpected export target: ${target}`);
    const path = resolve(root, target);
    assert(path.startsWith(`${root}${sep}`), `Export escapes package: ${target}`);
    assert(statSync(path).isFile(), `Missing export: ${target}`);
  } else {
    assert(target && typeof target === "object", "Invalid export target");
    for (const value of Object.values(target)) checkTarget(value);
  }
}

for (const field of ["main", "module", "types", "exports"]) {
  checkTarget(pkg[field]);
}
assert(statSync(resolve(root, "README.md")).isFile());

// Exercise actual Wasm initialization and a small API operation through Node's
// package resolution in both module systems, including the debug variant.
const check = `
  function check(api) {
    assert.equal(typeof api.Fragment.fromCheckpointPrefixes, "function");
    const bytes = Uint8Array.from({ length: 12 }, (_, i) => i);
    const checkpoint = api.Checkpoint.fromBytes(bytes);
    assert.deepEqual(checkpoint.toBytes(), bytes);
    checkpoint.free();
  }
`;
for (const mode of ["module", "commonjs"]) {
  const prelude = mode === "module"
    ? 'import assert from "node:assert/strict";'
    : 'const assert = require("node:assert/strict");';
  const calls = [pkg.name, `${pkg.name}/debug`].map((name) =>
    mode === "module"
      ? `check(await import(${JSON.stringify(name)}));`
      : `check(require(${JSON.stringify(name)}));`
  ).join("\n");
  const result = spawnSync(process.execPath, ["--input-type", mode, "--eval", `${prelude}\n${check}\n${calls}`], {
    cwd: root,
    stdio: "inherit",
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, `${mode} package smoke test failed`);
}
console.log(`Verified ${pkg.name}@${pkg.version}: export files, Node ESM/CJS and debug builds`);
