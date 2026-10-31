package mdb.driver;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class MdbClient implements AutoCloseable {

    private final MdbConnection connection;
    private String username;
    private String password;
    private String authDb;

    public MdbClient(String host, int port) {
        this.connection = new MdbConnection(host, port);
    }

    public MdbClient(String host, int port, String username, String password, String authDb) {
        this.connection = new MdbConnection(host, port);
        this.username = username;
        this.password = password;
        this.authDb = authDb;
    }

    public void connect() throws IOException {
        connection.connect();
        if (username != null && password != null) {
            authenticate(username, password, authDb != null ? authDb : "admin");
        }
    }

    public void close() {
        connection.close();
    }

    public MdbDatabase getDatabase(String name) {
        return new MdbDatabase(connection, name);
    }

    public List<String> listDatabases() throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("listDatabases", 1);
        WireProtocol.OpMsg response = connection.sendCommand(cmd);
        Map<String, Object> body = response.body;
        List<Map<String, Object>> databases = (List<Map<String, Object>>) body.get("databases");
        List<String> names = new ArrayList<>();
        if (databases != null) {
            for (Map<String, Object> db : databases) {
                names.add((String) db.get("name"));
            }
        }
        return names;
    }

    public Map<String, Object> getServerStatus() throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("serverStatus", 1);
        WireProtocol.OpMsg response = connection.sendCommand(cmd);
        return response.body;
    }

    public boolean ping() throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("ping", 1);
        WireProtocol.OpMsg response = connection.sendCommand(cmd);
        Object ok = response.body.get("ok");
        return ok != null && ((Number) ok).doubleValue() == 1.0;
    }

    public void authenticate(String username, String password, String authDb) throws IOException {
        Map<String, Object> saslStart = new LinkedHashMap<>();
        saslStart.put("saslStart", 1);
        saslStart.put("mechanism", "SCRAM-SHA-256");
        byte[] nonce = new byte[24];
        new Random().nextBytes(nonce);
        String nonceBase64 = Base64.getEncoder().encodeToString(nonce);
        String clientFirstBare = "n=" + username + ",r=" + nonceBase64;
        String clientFirstMessage = "n,," + clientFirstBare;
        saslStart.put("payload", Base64.getEncoder().encodeToString(clientFirstMessage.getBytes()));
        saslStart.put("$db", authDb);

        WireProtocol.OpMsg response = connection.sendCommand(saslStart);
        Map<String, Object> body = response.body;

        Object done = body.get("done");
        if (done != null && (Boolean) done) {
            return;
        }

        String payload = (String) body.get("payload");
        byte[] serverFirstMessageBytes = Base64.getDecoder().decode(payload);
        String serverFirstMessage = new String(serverFirstMessageBytes);

        String[] serverParts = serverFirstMessage.split(",");
        String serverNonce = null;
        String salt = null;
        int iterations = 0;
        for (String part : serverParts) {
            if (part.startsWith("r=")) serverNonce = part.substring(2);
            else if (part.startsWith("s=")) salt = part.substring(2);
            else if (part.startsWith("i=")) iterations = Integer.parseInt(part.substring(2));
        }

        byte[] saltBytes = Base64.getDecoder().decode(salt);
        byte[] saltedPassword = hi(password, saltBytes, iterations);
        byte[] clientKey = hmac(saltedPassword, "Client Key".getBytes());
        byte[] storedKey = sha256(clientKey);

        String clientFinalWithoutProof = "c=biws,r=" + serverNonce;
        String authMessage = clientFirstBare + "," + serverFirstMessage + "," + clientFinalWithoutProof;

        byte[] clientSignature = hmac(storedKey, authMessage.getBytes());
        byte[] clientProof = new byte[clientKey.length];
        for (int i = 0; i < clientKey.length; i++) {
            clientProof[i] = (byte) (clientKey[i] ^ clientSignature[i]);
        }

        String clientFinal = clientFinalWithoutProof + ",p=" + Base64.getEncoder().encodeToString(clientProof);

        Map<String, Object> saslContinue = new LinkedHashMap<>();
        saslContinue.put("saslContinue", 1);
        saslContinue.put("conversationId", body.get("conversationId"));
        saslContinue.put("payload", Base64.getEncoder().encodeToString(clientFinal.getBytes()));
        saslContinue.put("$db", authDb);

        response = connection.sendCommand(saslContinue);
        body = response.body;

        if (body.get("done") == null || !(Boolean) body.get("done")) {
            Map<String, Object> finalContinue = new LinkedHashMap<>();
            finalContinue.put("saslContinue", 1);
            finalContinue.put("conversationId", body.get("conversationId"));
            finalContinue.put("payload", "");
            finalContinue.put("$db", authDb);
            connection.sendCommand(finalContinue);
        }
    }

    private byte[] hi(String password, byte[] salt, int iterations) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] passwordBytes = password.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            md.update(passwordBytes);
            byte[] u = md.digest();
            md.reset();

            byte[] salted = new byte[salt.length + 4];
            System.arraycopy(salt, 0, salted, 0, salt.length);
            salted[salt.length] = 0;
            salted[salt.length + 1] = 0;
            salted[salt.length + 2] = 0;
            salted[salt.length + 3] = 1;

            md.update(u);
            byte[] ui = md.digest(salted);
            byte[] result = ui.clone();

            for (int i = 1; i < iterations; i++) {
                md.reset();
                ui = md.digest(ui);
                for (int j = 0; j < result.length; j++) {
                    result[j] ^= ui[j];
                }
            }
            return result;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] hmac(byte[] key, byte[] data) {
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] sha256(byte[] data) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            return md.digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public MdbConnection getConnection() {
        return connection;
    }
}
