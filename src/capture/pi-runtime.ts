import { existsSync, readFileSync, realpathSync } from "fs";
import { dirname, join, resolve } from "path";

/** Locate the host's pinned Pi AI package from its executable or extension path. */
export function findPiAiPackageRoot(...startPaths: Array<string | undefined>): string | null {
  const candidates = new Set<string>();
  for (const start of startPaths) {
    if (!start) continue;
    let current: string;
    try {
      current = dirname(realpathSync(start));
    } catch {
      continue;
    }
    while (true) {
      const packageJson = join(current, "package.json");
      if (existsSync(packageJson)) {
        try {
          const packageInfo = JSON.parse(readFileSync(packageJson, "utf8")) as { name?: string };
          if (packageInfo.name === "@earendil-works/pi-coding-agent") {
            candidates.add(resolve(current, "node_modules", "@earendil-works", "pi-ai"));
          }
        } catch {
          // Continue walking when an ancestor is not a readable package.
        }
      }
      const parent = dirname(current);
      if (parent === current) break;
      current = parent;
    }
  }
  return [...candidates].find((candidate) => existsSync(join(candidate, "package.json"))) ?? null;
}
