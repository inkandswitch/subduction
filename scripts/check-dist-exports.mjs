// Check that every entry point in package.json exists on disk before publishing.
// Usage (from a package directory): node ../scripts/check-dist-exports.mjs
//
// `pnpm build` writes pkg/, pkg-node/ and pkg-slim/, but the published package
// only ships dist/, which `bodge <crate>` produces. Publishing without running
// bodge first uploads a package with no code in it.
import { readFileSync, statSync } from "node:fs";
import { resolve } from "node:path";

const root = process.cwd();
const pkg = JSON.parse(readFileSync(resolve(root, "package.json"), "utf8"));
const missing = new Set();

function check(target) {
  if (typeof target === "string") {
    try {
      if (!statSync(resolve(root, target)).isFile()) missing.add(target);
    } catch {
      missing.add(target);
    }
  } else if (target && typeof target === "object") {
    for (const value of Object.values(target)) check(value);
  }
}

for (const field of ["main", "module", "types", "exports"]) {
  check(pkg[field]);
}

if (missing.size > 0) {
  console.error(`${pkg.name}: ${missing.size} entry point(s) missing:`);
  for (const path of missing) console.error(`  ${path}`);
  console.error("Run `bodge <crate>` in `nix develop` to build dist/ before publishing.");
  process.exit(1);
}

console.log(`${pkg.name}: all entry points present.`);
