
/**
 * Convert a glob pattern to a regular expression.
 * Very small glob -> RegExp: supports '*' and '?' only, on UTF-8 decoding of keys.
 * @param globUtf8 The glob pattern to convert.
 * @returns The converted regular expression.
 */
export function globToRegexBytes(globUtf8: string): RegExp {
  const esc = (s: string) => s.replace(/[.+^${}()|\[\]\\]/g, '\\$&');
  const pat = '^' + esc(globUtf8).replace(/\*/g, '.*').replace(/\?/g, '.') + '$';
  return new RegExp(pat);
}
