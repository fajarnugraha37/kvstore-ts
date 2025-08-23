const os = require("os");
const path = require("path");
const fs = require("fs");

module.exports = function makeTempDir(prefix = "kvstore-test") {
  const dir = path.join(
    os.tmpdir(),
    `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2)}`
  );
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch (e) {}
  fs.mkdirSync(dir, { recursive: true });
  return dir;
};
