package mdb.driver;

public final class CRC32C {

    private static final int[] TABLE = new int[256];

    static {
        final int POLYNOMIAL = 0x82F63B78;
        for (int i = 0; i < 256; i++) {
            int crc = i;
            for (int j = 0; j < 8; j++) {
                if ((crc & 1) != 0) {
                    crc = (crc >>> 1) ^ POLYNOMIAL;
                } else {
                    crc = crc >>> 1;
                }
            }
            TABLE[i] = crc;
        }
    }

    private CRC32C() {
    }

    public static int compute(byte[] data) {
        return compute(data, 0, data.length);
    }

    public static int compute(byte[] data, int offset, int length) {
        int crc = 0xFFFFFFFF;
        for (int i = offset; i < offset + length; i++) {
            crc = TABLE[(crc ^ data[i]) & 0xFF] ^ (crc >>> 8);
        }
        return crc ^ 0xFFFFFFFF;
    }

    public static byte[] computeBytes(byte[] data) {
        int crc = compute(data);
        byte[] result = new byte[4];
        result[0] = (byte) (crc);
        result[1] = (byte) (crc >>> 8);
        result[2] = (byte) (crc >>> 16);
        result[3] = (byte) (crc >>> 24);
        return result;
    }

    public static byte[] computeBytes(byte[] data, int offset, int length) {
        int crc = compute(data, offset, length);
        byte[] result = new byte[4];
        result[0] = (byte) (crc);
        result[1] = (byte) (crc >>> 8);
        result[2] = (byte) (crc >>> 16);
        result[3] = (byte) (crc >>> 24);
        return result;
    }
}
