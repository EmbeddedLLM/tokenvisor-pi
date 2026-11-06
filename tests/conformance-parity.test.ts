import { describe, expect, test } from "bun:test";
import { spawnSync } from "child_process";
import { copyFileSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";

const producerRoot = join(import.meta.dir, "..");
const fixtureNames = ["agent-metadata-v1.schema.json", "agent-metadata-v1.json"];

describe("producer/backend conformance parity check", () => {
  test("ignores formatting changes but detects contract drift", () => {
    const root = mkdtempSync(join(tmpdir(), "tokenvisor-conformance-parity-"));
    try {
      for (const name of fixtureNames) copyFileSync(join(producerRoot, "conformance", name), join(root, name));
      const script = join(producerRoot, "scripts", "check-conformance-parity.mjs");

      for (const name of fixtureNames) {
        const parsed = JSON.parse(readFileSync(join(root, name), "utf8"));
        writeFileSync(join(root, name), `${JSON.stringify(parsed, null, 4)}\n`);
      }
      const matches = spawnSync(process.execPath, [script, root], { encoding: "utf8" });
      expect(matches.status).toBe(0);

      for (const [name, mutate] of [
        ["agent-metadata-v1.json", (document: any) => { document.version = 2; }],
        ["agent-metadata-v1.schema.json", (schema: any) => { schema.title = "different contract"; }],
      ] as const) {
        const parsed = JSON.parse(readFileSync(join(root, name), "utf8"));
        mutate(parsed);
        writeFileSync(join(root, name), `${JSON.stringify(parsed, null, 2)}\n`);
        const drift = spawnSync(process.execPath, [script, root], { encoding: "utf8" });
        expect(drift.status).toBe(1);
        expect(drift.stderr).toContain(name);
        copyFileSync(join(producerRoot, "conformance", name), join(root, name));
      }
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
