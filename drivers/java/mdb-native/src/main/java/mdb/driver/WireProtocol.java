package mdb.driver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class WireProtocol {

    public static final int OP_MSG = 2013;
    public static final int FLAG_CHECKSUM_PRESENT = 1 << 0;
    public static final int FLAG_MORE_TO_COME = 1 << 1;
    public static final int FLAG_EXHAUST_ALLOWED = 1 << 16;

    private WireProtocol() {
    }

    public static byte[] buildRequest(int requestId, Map<String, Object> command,
                                       List<Map<String, Object>> documents, String docSequenceName) {
        try {
            ByteArrayOutputStream payload = new ByteArrayOutputStream();
            int flagBits = 0;
            Bson.writeInt32(payload, flagBits);

            byte[] cmdBytes = Bson.encode(command);
            payload.write(0);
            Bson.writeInt32(payload, cmdBytes.length + 4);
            payload.write(cmdBytes);

            if (documents != null && !documents.isEmpty()) {
                ByteArrayOutputStream sectionBody = new ByteArrayOutputStream();
                byte[] identifierBytes = (docSequenceName + "\0").getBytes(StandardCharsets.UTF_8);
                sectionBody.write(identifierBytes);
                for (Map<String, Object> doc : documents) {
                    byte[] docBytes = Bson.encode(doc);
                    sectionBody.write(docBytes);
                }

                byte[] sectionBodyBytes = sectionBody.toByteArray();
                payload.write(1);
                Bson.writeInt32(payload, sectionBodyBytes.length + 4);
                payload.write(sectionBodyBytes);
            }

            int messageLength = 16 + payload.size();
            ByteArrayOutputStream msg = new ByteArrayOutputStream(messageLength);
            Bson.writeInt32(msg, messageLength);
            Bson.writeInt32(msg, requestId);
            Bson.writeInt32(msg, 0);
            Bson.writeInt32(msg, OP_MSG);
            msg.write(payload.toByteArray());

            return msg.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to build OP_MSG", e);
        }
    }

    public static byte[] buildRequestSimple(int requestId, Map<String, Object> command) {
        return buildRequest(requestId, command, null, null);
    }

    public static OpMsg parseResponse(byte[] data) {
        if (data == null || data.length < 16) {
            throw new IllegalArgumentException("Response too short: " + (data == null ? 0 : data.length));
        }

        int[] offset = {0};
        int messageLength = Bson.readInt32(data, offset);
        if (messageLength != data.length) {
            throw new IllegalArgumentException("Message length mismatch: header=" + messageLength + " actual=" + data.length);
        }

        OpMsg opMsg = new OpMsg();
        opMsg.requestId = Bson.readInt32(data, offset);
        opMsg.responseTo = Bson.readInt32(data, offset);
        int opCode = Bson.readInt32(data, offset);

        if (opCode != OP_MSG) {
            throw new IllegalArgumentException("Expected OP_MSG (2013), got: " + opCode);
        }

        int flagBits = Bson.readInt32(data, offset);
        opMsg.moreToCome = (flagBits & FLAG_MORE_TO_COME) != 0;
        boolean checksumPresent = (flagBits & FLAG_CHECKSUM_PRESENT) != 0;

        int end = data.length;
        if (checksumPresent) {
            end -= 4;
        }

        Map<String, Object> kind0Doc = null;
        List<Map<String, Object>> docSequence = new ArrayList<>();

        while (offset[0] < end) {
            int kind = data[offset[0]++] & 0xFF;
            int sectionSize = Bson.readInt32(data, offset);
            int sectionEnd = offset[0] + sectionSize - 4;

            if (kind == 0) {
                kind0Doc = Bson.readDocument(data, offset);
                offset[0] = sectionEnd;
            } else if (kind == 1) {
                String identifier = Bson.readCString(data, offset);
                opMsg.docSequenceName = identifier;
                while (offset[0] < sectionEnd) {
                    Map<String, Object> doc = Bson.readDocument(data, offset);
                    docSequence.add(doc);
                }
                offset[0] = sectionEnd;
            } else {
                offset[0] = sectionEnd;
            }
        }

        opMsg.body = kind0Doc;
        opMsg.documentSequence = docSequence;

        if (checksumPresent) {
            opMsg.checksum = Bson.readInt32(data, offset);
        }

        return opMsg;
    }

    public static int crc32c(byte[] data) {
        return CRC32C.compute(data);
    }

    public static class OpMsg {
        public int requestId;
        public int responseTo;
        public Map<String, Object> body;
        public List<Map<String, Object>> documentSequence;
        public String docSequenceName;
        public boolean moreToCome;
        public int checksum;

        public List<Map<String, Object>> getDocuments() {
            return documentSequence != null ? documentSequence : new ArrayList<>();
        }

        @Override
        public String toString() {
            return "OpMsg{requestId=" + requestId + ", responseTo=" + responseTo
                    + ", moreToCome=" + moreToCome + ", body=" + body + '}';
        }
    }
}
