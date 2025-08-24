/**
 * Tries to execute a function and returns a tuple indicating success or failure.
 * @param fn The function to execute, which can be a promise or a function returning a promise.
 * @returns A promise resolving to a tuple: [result, undefined] on success, or [undefined, error] on failure.
 */
export const tryFunc = async <T>(
  fn: (() => Promise<T>) | Promise<T>
): Promise<[T, undefined] | [undefined, unknown]> => {
  try {
    if (fn instanceof Promise) return [await fn, undefined];
    else return [await fn(), undefined];
  } catch (error) {
    console.error("Error occurred:", error);
    return [undefined, error];
  }
};

/**
 * Convert a glob pattern to a regular expression.
 * Very small glob -> RegExp: supports '*' and '?' only, on UTF-8 decoding of keys.
 * @param globUtf8 The glob pattern to convert.
 * @returns The converted regular expression.
 */
export function globToRegexBytes(globUtf8: string): RegExp {
  const esc = (s: string) => s.replace(/[.+^${}()|\[\]\\]/g, "\\$&");
  const pat =
    "^" + esc(globUtf8).replace(/\*/g, ".*").replace(/\?/g, ".") + "$";
  return new RegExp(pat);
}
