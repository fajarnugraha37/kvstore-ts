import { beforeAll } from "bun:test";

// Wipe the ./data directory contents before running tests to ensure test isolation.
// No-op setup: per-test temp dirs are used to ensure isolation.
beforeAll(() => {});
