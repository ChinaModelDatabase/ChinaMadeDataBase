package mdb.driver;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MdbConnection {

    private final String host;
    private final int port;
    private Socket socket;
    private OutputStream out;
    private InputStream in;
    private final AtomicInteger requestIdCounter = new AtomicInteger(1);
    private volatile boolean connected = false;

    public MdbConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        out = new BufferedOutputStream(socket.getOutputStream());
        in = new BufferedInputStream(socket.getInputStream());
        connected = true;
    }

    public void close() {
        connected = false;
        try {
            if (out != null) out.close();
        } catch (IOException ignored) {
        }
        try {
            if (in != null) in.close();
        } catch (IOException ignored) {
        }
        try {
            if (socket != null) socket.close();
        } catch (IOException ignored) {
        }
    }

    public boolean isConnected() {
        return connected && socket != null && !socket.isClosed();
    }

    public WireProtocol.OpMsg sendCommand(Map<String, Object> command) throws IOException {
        return sendCommand(command, null, null);
    }

    public WireProtocol.OpMsg sendCommand(Map<String, Object> command, List<Map<String, Object>> docs, String seqName) throws IOException {
        int reqId = requestIdCounter.getAndIncrement();
        byte[] msg = WireProtocol.buildRequest(reqId, command, docs, seqName);
        writeMessage(msg);
        byte[] response = readMessage();
        return WireProtocol.parseResponse(response);
    }

    public int nextRequestId() {
        return requestIdCounter.getAndIncrement();
    }

    private void writeMessage(byte[] data) throws IOException {
        synchronized (out) {
            out.write(data);
            out.flush();
        }
    }

    private byte[] readMessage() throws IOException {
        byte[] header = readBytes(16);
        int[] offset = {0};
        int messageLength = Bson.readInt32(header, offset);

        if (messageLength < 16) {
            throw new IOException("Invalid message length: " + messageLength);
        }

        byte[] message = new byte[messageLength];
        System.arraycopy(header, 0, message, 0, 16);
        readBytesInto(message, 16, messageLength - 16);
        return message;
    }

    private byte[] readBytes(int count) throws IOException {
        byte[] buf = new byte[count];
        int read = 0;
        while (read < count) {
            int n = in.read(buf, read, count - read);
            if (n < 0) {
                throw new IOException("Connection closed, read " + read + " of " + count + " bytes");
            }
            read += n;
        }
        return buf;
    }

    private void readBytesInto(byte[] buf, int offset, int count) throws IOException {
        int read = 0;
        while (read < count) {
            int n = in.read(buf, offset + read, count - read);
            if (n < 0) {
                throw new IOException("Connection closed, read " + read + " of " + count + " bytes");
            }
            read += n;
        }
    }
}
