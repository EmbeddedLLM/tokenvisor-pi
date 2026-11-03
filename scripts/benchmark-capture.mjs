import { spawnSync } from "node:child_process";

// Run the synthetic fixtures in one Bun process; no native harness or server is needed.
const result = spawnSync("bun", ["test", "tests/pi-foreground-launch.test.ts", "tests/omp-blocking-binding.test.ts"], {
  cwd: new URL("..", import.meta.url),
  env: {
    ...process.env,
    TOKENVISOR_PI_FOREGROUND_BENCHMARK: "1",
    TOKENVISOR_OMP_CAPTURE_BENCHMARK: "1",
  },
  stdio: "inherit",
});
if (result.error) console.error(result.error.message);
process.exit(result.status ?? 1);
