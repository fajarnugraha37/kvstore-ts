const os = require("os");
const path = require("path");
const fs = require("fs");

module.exports = function makeTempDir(prefix = "kvstore-test") {
  const suffix = process.env.TEST_RUN_SUFFIX || process.env.TEST_IMPL || "";
  const name = suffix ? `${prefix}-${suffix}-${Date.now()}-${Math.random().toString(36).slice(2)}` : `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const dir = path.join(os.tmpdir(), name);
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch (e) {}
  fs.mkdirSync(dir, { recursive: true });
  return dir;
};
