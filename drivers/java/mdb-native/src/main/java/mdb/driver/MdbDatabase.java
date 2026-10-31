package mdb.driver;

import java.io.IOException;
import java.util.*;

public class MdbDatabase {

    private final MdbConnection connection;
    private final String name;

    public MdbDatabase(MdbConnection connection, String name) {
        this.connection = connection;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public MdbCollection getCollection(String collName) {
        return new MdbCollection(connection, name, collName);
    }

    public List<String> listCollections() throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("listCollections", 1);
        Map<String, Object> filter = new LinkedHashMap<>();
        filter.put("name", new LinkedHashMap<String, Object>() {{ put("$ne", "system.buckets"); }});
        cmd.put("filter", filter);
        WireProtocol.OpMsg response = runCommandRaw(cmd);
        List<Map<String, Object>> firstBatch = response.getDocuments();
        if (firstBatch.isEmpty()) {
            Object cursor = response.body.get("cursor");
            if (cursor instanceof Map) {
                firstBatch = (List<Map<String, Object>>) ((Map<?, ?>) cursor).get("firstBatch");
            }
        }
        List<String> names = new ArrayList<>();
        if (firstBatch != null) {
            for (Map<String, Object> coll : firstBatch) {
                names.add((String) coll.get("name"));
            }
        }
        return names;
    }

    public void createCollection(String collName) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("create", collName);
        runCommand(cmd);
    }

    public void dropCollection(String collName) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("drop", collName);
        runCommand(cmd);
    }

    public Map<String, Object> runCommand(Map<String, Object> cmd) throws IOException {
        WireProtocol.OpMsg response = runCommandRaw(cmd);
        return response.body;
    }

    WireProtocol.OpMsg runCommandRaw(Map<String, Object> cmd) throws IOException {
        Map<String, Object> cmdWithDb = new LinkedHashMap<>(cmd);
        cmdWithDb.put("$db", name);
        return connection.sendCommand(cmdWithDb);
    }

    public Map<String, Object> getStats() throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("dbStats", 1);
        return runCommand(cmd);
    }

    public void createUser(String username, String password) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("createUser", username);
        cmd.put("pwd", password);
        cmd.put("roles", Arrays.asList("readWrite"));
        runCommand(cmd);
    }
}
