#ifndef MDB_HPP
#define MDB_HPP

#include "mdb.h"
#include <string>
#include <vector>
#include <optional>
#include <memory>
#include <stdexcept>

namespace mdb {

class Error : public std::runtime_error {
public:
    Error(const bson_error_t& err)
        : std::runtime_error(std::string(err.message))
        , _code(err.code) {}
    int32_t code() const { return _code; }
private:
    int32_t _code;
};

class Database;

class Client {
public:
    Client(const std::string& host, uint16_t port = 27017);
    explicit Client(const std::string& uri);
    Client(const std::string& host, uint16_t port,
           const std::string& username, const std::string& password,
           const std::string& auth_db = "admin");
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    Client(Client&& other) noexcept;
    Client& operator=(Client&& other) noexcept;

    bool connect();
    bool is_connected() const;
    void close();

    Database database(const std::string& name);
    std::vector<std::string> list_databases();
    bson_t server_status();

private:
    mdb_client_t* _client;
};

class Collection;

class Database {
public:
    Collection collection(const std::string& name);
    std::vector<std::string> list_collections();
    void create_collection(const std::string& name);
    void drop_collection(const std::string& name);
    bson_t command(const bson_t& cmd);
    bson_t stats();

private:
    friend class Client;
    mdb_database_t* _db;
    explicit Database(mdb_database_t* db);
public:
    ~Database();
    Database(Database&& other) noexcept;
    Database& operator=(Database&& other) noexcept;
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;
};

class Collection {
public:
    class Cursor;

    std::optional<bson_t> find_one(const bson_t& filter = bson_t());
    Cursor find(const bson_t& filter = bson_t());
    bson_t insert_one(const bson_t& doc);
    bson_t insert_many(const std::vector<bson_t>& docs);
    bson_t update_one(const bson_t& filter, const bson_t& update);
    bson_t update_many(const bson_t& filter, const bson_t& update);
    bson_t delete_one(const bson_t& filter);
    bson_t delete_many(const bson_t& filter);
    bson_t replace_one(const bson_t& filter, const bson_t& replacement);
    Cursor aggregate(const bson_t& pipeline);
    bson_t distinct(const std::string& key, const bson_t& filter = bson_t());
    int64_t count_documents(const bson_t& filter = bson_t());
    void create_index(const bson_t& keys);
    void drop_index(const std::string& name);
    bson_t list_indexes();
    bson_t stats();
    bson_t validate();
    void compact();

private:
    friend class Database;
    mdb_collection_t* _coll;
    explicit Collection(mdb_collection_t* coll);
public:
    ~Collection();
    Collection(Collection&& other) noexcept;
    Collection& operator=(Collection&& other) noexcept;
    Collection(const Collection&) = delete;
    Collection& operator=(const Collection&) = delete;
};

class Collection::Cursor {
public:
    bool next();
    bson_t current() const;
    void close();
    ~Cursor();

    Cursor(Cursor&& other) noexcept;
    Cursor& operator=(Cursor&& other) noexcept;
    Cursor(const Cursor&) = delete;
    Cursor& operator=(const Cursor&) = delete;

private:
    friend class Collection;
    mdb_cursor_t* _cursor;
    explicit Cursor(mdb_cursor_t* cursor);
};

} // namespace mdb

#endif /* MDB_HPP */
