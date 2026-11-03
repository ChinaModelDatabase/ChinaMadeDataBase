const TABLE = new Int32Array(256);

(function initTable(): void {
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let j = 0; j < 8; j++) {
      if (crc & 1) {
        crc = (crc >>> 1) ^ 0x82f63b78;
      } else {
        crc = crc >>> 1;
      }
    }
    TABLE[i] = crc;
  }
})();

/** `crc32c` 的对象式别名：测试与上层代码用的是 `CRC32C.calculate(...)`。 */
export const CRC32C = {
  calculate: (data: Buffer): number => crc32c(data),
};

export function crc32c(data: Buffer): number {
  let crc = 0xffffffff;
  for (let i = 0; i < data.length; i++) {
    crc = TABLE[(crc ^ data[i]) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}
