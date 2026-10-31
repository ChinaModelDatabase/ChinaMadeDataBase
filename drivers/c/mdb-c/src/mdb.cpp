#include "mdb/mdb.hpp"
#include <utility>

namespace mdb {

/* ============ Client ============ */

Client::Client(const std::string& host, uint16_t port) {
    _client = mdb_client_new(host.c_str(), port);
    if (!_client) throw std::runtime_error("Failed to create MDB client");
}

Client::Client(const std::string& uri) {
    _client = mdb_client_new_from_uri(uri.c_str());
    if (!_client) throw std::runtime_error("Failed to create MDB client from URI");
}

Client::Client(const std::string& host, uint16_t port,
               const std::string& username, const std::string& password,
               const std::string& auth_db) {
    _client = mdb_client_new_with_auth(host.c_str(), port,
                                        username.c_str(), password.c_str(),
                                        auth_db.c_str());
    if (!_client) throw std::runtime_error("Failed to create MDB client with auth");
}

Client::~Client() {
    if (_client) {
        mdb_client_destroy(_client);
    }
}

Client::Client(Client&& other) noexcept : _client(other._client) {
    other._client = nullptr;
}

Client& Client::operator=(Client&& other) noexcept {
    if (this != &other) {
        if (_client) mdb_client_destroy(_client);
        _client = other._client;
        other._client = nullptr;
    }
    return *this;
}

bool Client::connect() {
    return mdb_client_connect(_client);
}

bool Client::is_connected() const {
    return mdb_client_is_connected(_client);
}

void Client::close() {
    mdb_client_close(_client);
}

Database Client::database(const std::string& name) {
    mdb_database_t* db = mdb_client_get_database(_client, name.c_str());
    if (!db) throw std::runtime_error("Failed to get database: " + name);
    return Database(db);
}

std::vector<std::string> Client::list_databases() {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_client_list_databases(_client, &reply, &error)) {
        throw Error(error);
    }

    std::vector<std::string> result;
    bson_iter_t iter;
    bson_iter_t databases;

    if (bson_iter_init_find(&iter, reply, "databases") &&
        bson_iter_recurse(&iter, &databases)) {
        while (bson_iter_next(&databases)) {
            bson_iter_t name_iter;
            if (bson_iter_recurse(&databases, &name_iter) &&
                bson_iter_find(&name_iter, "name")) {
                uint32_t len;
                const char* str = bson_iter_utf8(&name_iter, &len);
                result.emplace_back(str, len);
            }
        }
    }

    bson_destroy(reply);
    return result;
}

bson_t Client::server_status() {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_client_server_status(_client, &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

/* ============ Database ============ */

Database::Database(mdb_database_t* db) : _db(db) {}

Database::~Database() {
    if (_db) mdb_database_destroy(_db);
}

Database::Database(Database&& other) noexcept : _db(other._db) {
    other._db = nullptr;
}

Database& Database::operator=(Database&& other) noexcept {
    if (this != &other) {
        if (_db) mdb_database_destroy(_db);
        _db = other._db;
        other._db = nullptr;
    }
    return *this;
}

Collection Database::collection(const std::string& name) {
    mdb_collection_t* coll = mdb_database_get_collection(_db, name.c_str());
    if (!coll) throw std::runtime_error("Failed to get collection: " + name);
    return Collection(coll);
}

std::vector<std::string> Database::list_collections() {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_database_list_collections(_db, &reply, &error)) {
        throw Error(error);
    }

    std::vector<std::string> result;
    bson_iter_t iter;
    bson_iter_t cursor_batch;

    if (bson_iter_init_find(&iter, reply, "cursor") &&
        bson_iter_recurse(&iter, &cursor_batch) &&
        bson_iter_find(&cursor_batch, "firstBatch")) {
        bson_iter_t batch;
        if (bson_iter_recurse(&cursor_batch, &batch)) {
            while (bson_iter_next(&batch)) {
                bson_iter_t name_iter;
                if (bson_iter_recurse(&batch, &name_iter) &&
                    bson_iter_find(&name_iter, "name")) {
                    uint32_t len;
                    const char* str = bson_iter_utf8(&name_iter, &len);
                    result.emplace_back(str, len);
                }
            }
        }
    }

    bson_destroy(reply);
    return result;
}

void Database::create_collection(const std::string& name) {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_database_create_collection(_db, name.c_str(), &reply, &error)) {
        throw Error(error);
    }
    bson_destroy(reply);
}

void Database::drop_collection(const std::string& name) {
    bson_error_t error;
    if (!mdb_database_drop_collection(_db, name.c_str(), &error)) {
        throw Error(error);
    }
}

bson_t Database::command(const bson_t& cmd) {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_database_command_simple(_db, &cmd, &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

bson_t Database::stats() {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_database_stats(_db, &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

/* ============ Collection ============ */

Collection::Collection(mdb_collection_t* coll) : _coll(coll) {}

Collection::~Collection() {
    if (_coll) mdb_collection_destroy(_coll);
}

Collection::Collection(Collection&& other) noexcept : _coll(other._coll) {
    other._coll = nullptr;
}

Collection& Collection::operator=(Collection&& other) noexcept {
    if (this != &other) {
        if (_coll) mdb_collection_destroy(_coll);
        _coll = other._coll;
        other._coll = nullptr;
    }
    return *this;
}

std::optional<bson_t> Collection::find_one(const bson_t& filter) {
    bson_error_t error;
    bson_t* result = NULL;

    bson_t f = (bson_is_empty(&filter)) ? bson_t() : filter;

    if (!mdb_collection_find_one(_coll,
                                  bson_is_empty(&f) ? NULL : (bson_t*)&f,
                                  NULL, &result, &error)) {
        if (result) bson_destroy(result);
        return std::nullopt;
    }

    bson_t out;
    bson_copy_to(result, &out);
    bson_destroy(result);
    return out;
}

Collection::Cursor Collection::find(const bson_t& filter) {
    mdb_cursor_t* cursor = mdb_collection_find(_coll,
                                                 bson_is_empty(&filter) ? NULL : (bson_t*)&filter,
                                                 NULL);
    return Cursor(cursor);
}

bson_t Collection::insert_one(const bson_t& doc) {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_collection_insert_one(_coll, (bson_t*)&doc, &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

bson_t Collection::insert_many(const std::vector<bson_t>& docs) {
    if (docs.empty()) throw std::runtime_error("Empty document list");

    std::vector<const bson_t*> ptrs(docs.size());
    for (size_t i = 0; i < docs.size(); i++) {
        ptrs[i] = &docs[i];
    }

    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_collection_insert_many(_coll, ptrs.data(), ptrs.size(), &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

bson_t Collection::update_one(const bson_t& filter, const bson_t& update) {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_collection_update_one(_coll, (bson_t*)&filter, (bson_t*)&update, &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

bson_t Collection::update_many(const bson_t& filter, const bson_t& update) {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_collection_update_many(_coll, (bson_t*)&filter, (bson_t*)&update, &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

bson_t Collection::delete_one(const bson_t& filter) {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_collection_delete_one(_coll, (bson_t*)&filter, &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

bson_t Collection::delete_many(const bson_t& filter) {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_collection_delete_many(_coll, (bson_t*)&filter, &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

bson_t Collection::replace_one(const bson_t& filter, const bson_t& replacement) {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_collection_replace_one(_coll, (bson_t*)&filter, (bson_t*)&replacement, &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

Collection::Cursor Collection::aggregate(const bson_t& pipeline) {
    mdb_cursor_t* cursor = mdb_collection_aggregate(_coll, (bson_t*)&pipeline, NULL);
    return Cursor(cursor);
}

bson_t Collection::distinct(const std::string& key, const bson_t& filter) {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_collection_distinct(_coll, key.c_str(),
                                  bson_is_empty(&filter) ? NULL : (bson_t*)&filter,
                                  &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

int64_t Collection::count_documents(const bson_t& filter) {
    bson_error_t error;
    int64_t count = mdb_collection_count_documents(_coll,
                                                    bson_is_empty(&filter) ? NULL : (bson_t*)&filter,
                                                    NULL, &error);
    if (count < 0) throw Error(error);
    return count;
}

void Collection::create_index(const bson_t& keys) {
    bson_error_t error;
    if (!mdb_collection_create_index(_coll, (bson_t*)&keys, NULL, &error)) {
        throw Error(error);
    }
}

void Collection::drop_index(const std::string& name) {
    bson_error_t error;
    if (!mdb_collection_drop_index(_coll, name.c_str(), &error)) {
        throw Error(error);
    }
}

bson_t Collection::list_indexes() {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_collection_list_indexes(_coll, &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

bson_t Collection::stats() {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_collection_stats(_coll, &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

bson_t Collection::validate() {
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_collection_validate(_coll, &reply, &error)) {
        throw Error(error);
    }
    bson_t result;
    bson_copy_to(reply, &result);
    bson_destroy(reply);
    return result;
}

void Collection::compact() {
    bson_t cmd;
    bson_init(&cmd);
    BSON_APPEND_INT32(&cmd, "compact", 1);
    bson_error_t error;
    bson_t* reply = NULL;
    if (!mdb_collection_command_simple(_coll, &cmd, &reply, &error)) {
        bson_destroy(&cmd);
        throw Error(error);
    }
    bson_destroy(&cmd);
    bson_destroy(reply);
}

/* ============ Collection::Cursor ============ */

Collection::Cursor::Cursor(mdb_cursor_t* cursor) : _cursor(cursor) {}

Collection::Cursor::~Cursor() {
    if (_cursor) mdb_cursor_destroy(_cursor);
}

Collection::Cursor::Cursor(Cursor&& other) noexcept : _cursor(other._cursor) {
    other._cursor = nullptr;
}

Collection::Cursor& Collection::Cursor::operator=(Cursor&& other) noexcept {
    if (this != &other) {
        if (_cursor) mdb_cursor_destroy(_cursor);
        _cursor = other._cursor;
        other._cursor = nullptr;
    }
    return *this;
}

bool Collection::Cursor::next() {
    const bson_t* doc = NULL;
    return mdb_cursor_next(_cursor, &doc);
}

bson_t Collection::Cursor::current() const {
    const bson_t* doc = NULL;
    mdb_cursor_next(const_cast<mdb_cursor_t*>(_cursor), &doc);
    bson_t result;
    bson_init(&result);
    if (doc) {
        bson_copy_to(doc, &result);
    }
    return result;
}

void Collection::Cursor::close() {
    if (_cursor) {
        mdb_cursor_destroy(_cursor);
        _cursor = NULL;
    }
}

} // namespace mdb
