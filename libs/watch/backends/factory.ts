import { FileBackend } from "./file_backend";
import { HttpForwarder } from "./http_forwarder";
import { WatchManager, type WatchBackend } from "../watch_manager";

export function createWatchBackend(
  spec: any,
  engineDir?: string
): WatchBackend {
  if (!spec) return new WatchManager();
  if (typeof spec === "string") {
    if (spec === "file") return new FileBackend(engineDir || ".");
    if (spec.startsWith("http")) return new HttpForwarder(spec);
  }
  if (typeof spec === "object") {
    if (spec.type === "file")
      return new FileBackend(spec.dir || engineDir || ".", spec.opts);
    if (spec.type === "http")
      return new HttpForwarder(spec.endpoint, spec.opts);
  }
  // fallback to in-memory
  return new WatchManager();
}
