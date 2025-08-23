export type MemValue = { value: Buffer | null; rev?: number };

export class Node {
  key: Buffer;
  val: MemValue;
  left: Node | null = null;
  right: Node | null = null;
  height = 1;
  constructor(key: Buffer, val: MemValue) {
    this.key = Buffer.from(key);
    this.val = val;
  }
}
