import { mkdir, writeFile, rename, open } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fasync } from "./fs";

/**
 * Get the absolute path for a given relative path.
 * @param p The relative path.
 * @returns The absolute path.
 */
export const dir = (...p: string[]) => resolve(process.cwd(), ...p);
/**
 * Ensure that the directory exists.
 * @param p The path to the directory.
 */
export const ensureDir = async (p: string) =>
  await mkdir(p, { recursive: true });

/**
 * Synchronizes the file system state for a given path.
 * @param path The path to synchronize.
 */
export async function fsyncPath(path: string) {
  const dir = await open(path, "r");
  try {
    await dir.sync();
  } finally {
    await dir.close();
  }
}

/**
 * Get the file descriptor for a given file path.
 * @param path The path to the file.
 * @param flags The file opening flags (default: "r").
 * @returns The file descriptor.
 */
export const fileDescriptor = async (path: string, flags: string = "r") =>
  (await open(path, flags)).fd;

/**
 * Atomically write data to a file.
 * @param path The path to the file.
 * @param data The data to write.
 */
export async function atomicWriteFile(
  path: string,
  data: Uint8Array | string
): Promise<void> {
  const tmp = path + ".tmp";
  await writeFile(tmp, data);
  const fd = await fileDescriptor(tmp);
  await fasync.fsync(fd);
  await rename(tmp, path);
  const dir = dirname(path);
  const dirFd = await open(dir, "r");
  await dirFd.sync();
  await dirFd.close();
}
