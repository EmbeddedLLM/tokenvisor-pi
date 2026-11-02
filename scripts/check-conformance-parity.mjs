import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { fileURLToPath } from "node:url";

const producerRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const backendRoot = process.argv[2];

if (!backendRoot) {
  console.error("Usage: bun run check:backend-conformance -- <backend-audit-fixtures-directory>");
  process.exit(2);
}

const files = ["agent-metadata-v1.schema.json", "agent-metadata-v1.json"];
const differences = files.filter((name) => {
  const producer = JSON.parse(readFileSync(resolve(producerRoot, "conformance", name), "utf8"));
  const backend = JSON.parse(readFileSync(resolve(backendRoot, name), "utf8"));
  return !isDeepStrictEqual(producer, backend);
});

if (differences.length) {
  console.error(`Producer/backend conformance files differ: ${differences.join(", ")}`);
  process.exit(1);
}

console.log(`Conformance files match: ${files.join(", ")}`);
