import fs from "node:fs";
import { promisify } from "node:util";

const fsync = promisify(fs.fsync).bind(fs);
const fdatasync = promisify(fs.fdatasync).bind(fs);
const write = promisify(fs.write).bind(fs);
const read = promisify(fs.read).bind(fs);
const close = promisify(fs.close).bind(fs);

export const fasync = {
  fsync,
  fdatasync,
  write,
  read,
  close,
};
