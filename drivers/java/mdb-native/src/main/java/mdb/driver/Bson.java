package mdb.driver;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class Bson {

    public static final byte TYPE_DOUBLE = 0x01;
    public static final byte TYPE_STRING = 0x02;
    public static final byte TYPE_DOCUMENT = 0x03;
    public static final byte TYPE_ARRAY = 0x04;
    public static final byte TYPE_BINARY = 0x05;
    public static final byte TYPE_OBJECT_ID = 0x07;
    public static final byte TYPE_BOOLEAN = 0x08;
    public static final byte TYPE_DATETIME = 0x09;
    public static final byte TYPE_NULL = 0x0A;
    public static final byte TYPE_INT32 = 0x10;
    public static final byte TYPE_INT64 = 0x12;

    private Bson() {
    }

    public static byte[] encode(Map<String, Object> doc) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            writeDocument(out, doc);
        } catch (IOException e) {
            throw new RuntimeException("BSON encode error", e);
        }
        return out.toByteArray();
    }

    public static Map<String, Object> decode(byte[] bytes) {
        if (bytes == null || bytes.length < 5) {
            throw new IllegalArgumentException("Invalid BSON document: too short");
        }
        int[] offset = {0};
        return readDocument(bytes, offset);
    }

    static void writeDocument(ByteArrayOutputStream out, Map<String, Object> doc) throws IOException {
        ByteArrayOutputStream docOut = new ByteArrayOutputStream();
        if (doc != null) {
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                writeElement(docOut, entry.getKey(), entry.getValue());
            }
        }
        docOut.write(0x00);

        byte[] body = docOut.toByteArray();
        writeInt32(out, body.length + 4);
        out.write(body);
    }

    static void writeArray(ByteArrayOutputStream out, List<Object> arr) throws IOException {
        ByteArrayOutputStream arrOut = new ByteArrayOutputStream();
        for (int i = 0; i < arr.size(); i++) {
            writeElement(arrOut, String.valueOf(i), arr.get(i));
        }
        arrOut.write(0x00);

        byte[] body = arrOut.toByteArray();
        writeInt32(out, body.length + 4);
        out.write(body);
    }

    static void writeElement(ByteArrayOutputStream out, String name, Object value) throws IOException {
        byte type = getType(value);
        out.write(type);
        writeCString(out, name);
        writeValue(out, type, value);
    }

    static byte getType(Object value) {
        if (value == null) return TYPE_NULL;
        if (value instanceof Double || value instanceof double[]) return TYPE_DOUBLE;
        if (value instanceof String) return TYPE_STRING;
        if (value instanceof Map) return TYPE_DOCUMENT;
        if (value instanceof List) return TYPE_ARRAY;
        if (value instanceof byte[]) return TYPE_BINARY;
        if (value instanceof Boolean) return TYPE_BOOLEAN;
        if (value instanceof Date) return TYPE_DATETIME;
        if (value instanceof ObjectIdHolder) return TYPE_OBJECT_ID;
        if (value instanceof Integer) return TYPE_INT32;
        if (value instanceof Long) return TYPE_INT64;
        throw new IllegalArgumentException("Unsupported BSON type: " + value.getClass().getName());
    }

    static void writeValue(ByteArrayOutputStream out, byte type, Object value) throws IOException {
        switch (type) {
            case TYPE_DOUBLE:
                writeDouble(out, ((Number) value).doubleValue());
                break;
            case TYPE_STRING:
                writeString(out, (String) value);
                break;
            case TYPE_DOCUMENT:
                writeDocument(out, (Map<String, Object>) value);
                break;
            case TYPE_ARRAY:
                writeArray(out, (List<Object>) value);
                break;
            case TYPE_BINARY:
                byte[] bin = (byte[]) value;
                writeInt32(out, bin.length);
                out.write(0x00);
                out.write(bin);
                break;
            case TYPE_OBJECT_ID:
                out.write(((ObjectIdHolder) value).id);
                break;
            case TYPE_BOOLEAN:
                out.write(((Boolean) value) ? 1 : 0);
                break;
            case TYPE_DATETIME:
                writeInt64(out, ((Date) value).getTime());
                break;
            case TYPE_NULL:
                break;
            case TYPE_INT32:
                writeInt32(out, ((Number) value).intValue());
                break;
            case TYPE_INT64:
                writeInt64(out, ((Number) value).longValue());
                break;
            default:
                throw new IllegalArgumentException("Unknown BSON type: 0x" + Integer.toHexString(type & 0xFF));
        }
    }

    static Object readValue(byte[] data, int[] offset, byte expectedType) {
        byte type = data[offset[0]++];
        if (expectedType != 0 && type != expectedType) {
            throw new RuntimeException("Expected type 0x" + Integer.toHexString(expectedType & 0xFF)
                    + " but got 0x" + Integer.toHexString(type & 0xFF) + " at offset " + (offset[0] - 1));
        }
        return readValueForType(data, offset, type);
    }

    static Object readValueForType(byte[] data, int[] offset, byte type) {
        switch (type) {
            case TYPE_DOUBLE:
                return readDouble(data, offset);
            case TYPE_STRING:
                return readString(data, offset);
            case TYPE_DOCUMENT:
                return readDocument(data, offset);
            case TYPE_ARRAY:
                return readArray(data, offset);
            case TYPE_BINARY:
                return readBinary(data, offset);
            case TYPE_OBJECT_ID:
                return readObjectId(data, offset);
            case TYPE_BOOLEAN:
                return data[offset[0]++] != 0;
            case TYPE_DATETIME:
                return new Date(readInt64(data, offset));
            case TYPE_NULL:
                return null;
            case TYPE_INT32:
                return readInt32(data, offset);
            case TYPE_INT64:
                return readInt64(data, offset);
            default:
                throw new RuntimeException("Unknown BSON type: 0x" + Integer.toHexString(type & 0xFF) + " at offset " + (offset[0] - 1));
        }
    }

    static Map<String, Object> readDocument(byte[] data, int[] offset) {
        int size = readInt32(data, offset);
        int end = offset[0] + size - 4;
        Map<String, Object> doc = new LinkedHashMap<>();
        while (offset[0] < end) {
            if (data[offset[0]] == 0x00) {
                offset[0]++;
                break;
            }
            byte type = data[offset[0]++];
            String name = readCString(data, offset);
            Object value = readValueForType(data, offset, type);
            doc.put(name, value);
        }
        offset[0] = end;
        return doc;
    }

    static List<Object> readArray(byte[] data, int[] offset) {
        Map<String, Object> map = readDocument(data, offset);
        List<Object> list = new ArrayList<>(map.size());
        for (int i = 0; i < map.size(); i++) {
            list.add(map.get(String.valueOf(i)));
        }
        return list;
    }

    static byte[] readBinary(byte[] data, int[] offset) {
        int len = readInt32(data, offset);
        byte subtype = data[offset[0]++];
        byte[] bin = new byte[len];
        System.arraycopy(data, offset[0], bin, 0, len);
        offset[0] += len;
        return bin;
    }

    static ObjectIdHolder readObjectId(byte[] data, int[] offset) {
        byte[] id = new byte[12];
        System.arraycopy(data, offset[0], id, 0, 12);
        offset[0] += 12;
        return new ObjectIdHolder(id);
    }

    static String readString(byte[] data, int[] offset) {
        int len = readInt32(data, offset);
        String s = new String(data, offset[0], len - 1, StandardCharsets.UTF_8);
        offset[0] += len;
        return s;
    }

    static String readCString(byte[] data, int[] offset) {
        int start = offset[0];
        while (data[offset[0]] != 0x00) {
            offset[0]++;
        }
        String s = new String(data, start, offset[0] - start, StandardCharsets.UTF_8);
        offset[0]++;
        return s;
    }

    static void writeString(ByteArrayOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeInt32(out, bytes.length + 1);
        out.write(bytes);
        out.write(0x00);
    }

    static void writeCString(ByteArrayOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        out.write(bytes);
        out.write(0x00);
    }

    static void writeInt32(ByteArrayOutputStream out, int v) throws IOException {
        out.write(v);
        out.write(v >>> 8);
        out.write(v >>> 16);
        out.write(v >>> 24);
    }

    static void writeInt64(ByteArrayOutputStream out, long v) throws IOException {
        out.write((int) v);
        out.write((int) (v >>> 8));
        out.write((int) (v >>> 16));
        out.write((int) (v >>> 24));
        out.write((int) (v >>> 32));
        out.write((int) (v >>> 40));
        out.write((int) (v >>> 48));
        out.write((int) (v >>> 56));
    }

    static void writeDouble(ByteArrayOutputStream out, double v) throws IOException {
        writeInt64(out, Double.doubleToLongBits(v));
    }

    static int readInt32(byte[] data, int[] offset) {
        int v = (data[offset[0]] & 0xFF)
                | ((data[offset[0] + 1] & 0xFF) << 8)
                | ((data[offset[0] + 2] & 0xFF) << 16)
                | ((data[offset[0] + 3] & 0xFF) << 24);
        offset[0] += 4;
        return v;
    }

    static long readInt64(byte[] data, int[] offset) {
        long v = (data[offset[0]] & 0xFFL)
                | ((data[offset[0] + 1] & 0xFFL) << 8)
                | ((data[offset[0] + 2] & 0xFFL) << 16)
                | ((data[offset[0] + 3] & 0xFFL) << 24)
                | ((data[offset[0] + 4] & 0xFFL) << 32)
                | ((data[offset[0] + 5] & 0xFFL) << 40)
                | ((data[offset[0] + 6] & 0xFFL) << 48)
                | ((data[offset[0] + 7] & 0xFFL) << 56);
        offset[0] += 8;
        return v;
    }

    static double readDouble(byte[] data, int[] offset) {
        return Double.longBitsToDouble(readInt64(data, offset));
    }

    static void writeInt32ToArray(byte[] arr, int[] offset, int v) {
        arr[offset[0]] = (byte) v;
        arr[offset[0] + 1] = (byte) (v >>> 8);
        arr[offset[0] + 2] = (byte) (v >>> 16);
        arr[offset[0] + 3] = (byte) (v >>> 24);
        offset[0] += 4;
    }

    public static byte[] generateObjectId() {
        return ObjectIds.generate();
    }

    public static ObjectIdHolder oid(byte[] id) {
        return new ObjectIdHolder(id);
    }

    public static ObjectIdHolder oid() {
        return new ObjectIdHolder(ObjectIds.generate());
    }

    public static class ObjectIdHolder {
        public final byte[] id;

        public ObjectIdHolder(byte[] id) {
            this.id = id.clone();
        }

        public String toHexString() {
            return ObjectIds.toHexString(id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return Arrays.equals(id, ((ObjectIdHolder) o).id);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(id);
        }

        @Override
        public String toString() {
            return "ObjectId(\"" + toHexString() + "\")";
        }
    }
}
