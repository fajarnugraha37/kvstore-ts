import { beforeAll } from "bun:test";
import { existsSync, readdirSync, unlinkSync, rmSync, statSync } from "node:fs";
import { join } from "node:path";

// Wipe the ./data directory contents before running tests to ensure test isolation.
// No-op setup: per-test temp dirs are used to ensure isolation.
beforeAll(() => {});
