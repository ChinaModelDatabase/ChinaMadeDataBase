package mdb.driver;

import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;

public final class ObjectIds {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final AtomicInteger COUNTER = new AtomicInteger(RANDOM.nextInt());

    private ObjectIds() {
    }

    public static byte[] generate() {
        byte[] id = new byte[12];

        int timestamp = (int) (System.currentTimeMillis() / 1000);
        id[0] = (byte) (timestamp >>> 24);
        id[1] = (byte) (timestamp >>> 16);
        id[2] = (byte) (timestamp >>> 8);
        id[3] = (byte) (timestamp);

        byte[] random = new byte[5];
        RANDOM.nextBytes(random);
        System.arraycopy(random, 0, id, 4, 5);

        int counter = COUNTER.getAndIncrement() & 0xFFFFFF;
        id[9] = (byte) (counter >>> 16);
        id[10] = (byte) (counter >>> 8);
        id[11] = (byte) (counter);

        return id;
    }

    public static String toHexString(byte[] id) {
        StringBuilder sb = new StringBuilder(24);
        for (byte b : id) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }

    public static byte[] fromHexString(String hex) {
        if (hex.length() != 24) {
            throw new IllegalArgumentException("ObjectId hex string must be 24 characters, got: " + hex.length());
        }
        byte[] id = new byte[12];
        for (int i = 0; i < 12; i++) {
            id[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return id;
    }

    public static int getTimestamp(byte[] id) {
        return ((id[0] & 0xFF) << 24) | ((id[1] & 0xFF) << 16) | ((id[2] & 0xFF) << 8) | (id[3] & 0xFF);
    }
}
