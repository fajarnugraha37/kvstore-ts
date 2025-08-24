#!/usr/bin/env bun
// Parse mitata output file and append a small summary under '## Benchmarks' in README.md
import fs from "fs";
import path from "path";

async function main() {
  const arg = process.argv[2];
  if (!arg) {
    console.error("Usage: append_bench_readme.ts <mitata_output_file>");
    process.exit(2);
  }
  const outPath = path.resolve(arg);
  if (!fs.existsSync(outPath)) {
    console.error("Output file not found:", outPath);
    process.exit(2);
  }
  console.log("Reading mitata output from:", outPath);
  let txt = await fs.promises.readFile(outPath, "utf8");
  // strip ANSI color/formatting sequences (mitata outputs colored text)
  txt = txt.replace(/\x1b\[[0-9;]*m/g, "");

  // match lines like: "open-close                    52.32 ms/iter"
  const re = /^\s*(\S[\S ]*?\S)\s+(\d+(?:\.\d+)?)\s+ms\/iter/gm;
  const matches: Array<{ name: string; avgMs: string; avgMemMb?: string }> = [];
  let m: RegExpExecArray | null;
  while ((m = re.exec(txt))) {
    if (!m[1] || !m[2]) continue;
    const name = m[1].trim();
    const avgMs = m[2];
    // attempt to capture a following memory line that contains 'mb' within next 300 chars
    const rest = txt.slice(m.index, Math.min(txt.length, m.index + 300));
    const memMatch = /\((?:[^\)]*?)\)\s*(\d+(?:\.\d+)?)\s*mb/m.exec(rest);
    matches.push({ name, avgMs, avgMemMb: memMatch ? memMatch[1] : undefined });
  }

  if (matches.length === 0) {
    console.error("No bench lines parsed from mitata output.");
    process.exit(1);
  }

  // Parse RUN_META for cpu percent and duration if present
  const metaMatch = /RUN_META\s+cpu_percent=(\d+(?:\.\d+)?)\s+duration_ms=(\d+)/.exec(txt);
  const cpuPercent = metaMatch ? metaMatch[1] : undefined;

  const readmePath = path.resolve("README.md");
  let readme = "";
  if (fs.existsSync(readmePath)) readme = await fs.promises.readFile(readmePath, "utf8");

  const header = "## Benchmarks";
  if (!readme.includes(header)) {
    readme += "\n\n" + header + "\n\n";
  }

  // create a Markdown table header
  const tableHeader = "| name | avg_ms | avg_mem_mb | cpu_percent | ts |\n| --- | ---: | ---: | ---: | --- |\n";
  let table = tableHeader;
  const now = new Date().toISOString();
  for (const r of matches) {
    table += `| ${r.name} | ${r.avgMs} | ${r.avgMemMb || ""} | ${cpuPercent || ""} | ${now} |\n`;
  }

  readme += table + "\n";
  await fs.promises.writeFile(readmePath, readme, "utf8");
  console.log(`Appended ${matches.length} bench rows to README.md`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
