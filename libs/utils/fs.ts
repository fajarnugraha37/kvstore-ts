import fs from "node:fs";
import { promisify } from "node:util";

const fsync = promisify(fs.fsync).bind(fs);
const fdatasync = promisify(fs.fdatasync).bind(fs);
const write = promisify(fs.write).bind(fs);
const read = promisify(fs.read).bind(fs);
const close = promisify(fs.close).bind(fs);

/**
 * Write multiple buffers to a file descriptor.
 * Some Node versions expose fs.writev (callback-style). Prefer promisified
 * writev when available to perform a single syscall for multiple buffers.
 * @param fd The file descriptor to write to.
 * @param buffers The buffers to write.
 * @param position The position to write at.
 * @returns A promise that resolves when the write is complete.
 */
const writevCb: ((fd: number, buffers: Array<Buffer | Uint8Array>, position?: number | null, cb?: Function) => void) | null =
  (fs as any).writev ? (fs as any).writev.bind(fs) : null;

/**
 * Write multiple buffers to a file descriptor.
 */
const writev = writevCb
  ? promisify(writevCb).bind(fs)
  : async function (
      fd: number,
      buffers: Array<Buffer | Uint8Array>,
      position: number | null = null
    ) {
    // Fallback: concat buffers and use write.
    const buf = Buffer.concat(buffers.map((b) => Buffer.from(b)));
    // write(fd, buffer, offset, length, position)
    return await write(fd, buf, 0, buf.length, position);
  };

// Whether native fs.writev is available and being used (helps benchmarking)
export const writevNative = !!writevCb;

export const fasync = {
  fsync,
  fdatasync,
  write,
  writev,
  read,
  close,
};
