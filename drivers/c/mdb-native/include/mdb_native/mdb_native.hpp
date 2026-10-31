#ifndef MDB_NATIVE_HPP
#define MDB_NATIVE_HPP

#include <mdb_native/bson.h>
#include <mdb_native/wire.h>
#include <mdb_native/connection.h>
#include <mdb_native/client.h>
#include <mdb_native/database.h>
#include <mdb_native/collection.h>
#include <mdb_native/cursor.h>

#include <string>
#include <memory>
#include <functional>
#include <iostream>
#include <vector>
#include <stdexcept>
#include <utility>

namespace mdb {

class Document {
public:
    Document() : bson_(mdb_bson_new(), mdb_bson_destroy) {}

    explicit Document(mdb_bson_t *raw) : bson_(raw, mdb_bson_destroy) {}

    Document(const Document &other) {
        uint32_t len = 0;
        uint8_t *bytes = mdb_bson_to_bytes(other.bson_.get(), &len);
        if (bytes) {
            bson_.reset(mdb_bson_from_bytes(bytes, len));
            free(bytes);
        }
    }

    Document &operator=(const Document &other) {
        if (this != &other) {
            Document tmp(other);
            swap(tmp);
        }
        return *this;
    }

    Document(Document &&) noexcept = default;
    Document &operator=(Document &&) noexcept = default;

    void swap(Document &other) noexcept {
        bson_.swap(other.bson_);
    }

    Document &append_int32(const char *name, int32_t value) {
        mdb_bson_append_int32(bson_.get(), name, value);
        return *this;
    }

    Document &append_int64(const char *name, int64_t value) {
        mdb_bson_append_int64(bson_.get(), name, value);
        return *this;
    }

    Document &append_double(const char *name, double value) {
        mdb_bson_append_double(bson_.get(), name, value);
        return *this;
    }

    Document &append_utf8(const char *name, const char *value, uint32_t len) {
        mdb_bson_append_utf8(bson_.get(), name, value, len);
        return *this;
    }

    Document &append_bool(const char *name, bool value) {
        mdb_bson_append_bool(bson_.get(), name, value);
        return *this;
    }

    Document &append_null(const char *name) {
        mdb_bson_append_null(bson_.get(), name);
        return *this;
    }

    Document &append_object_id(const char *name, const mdb_object_id_t &oid) {
        mdb_bson_append_object_id(bson_.get(), name, &oid);
        return *this;
    }

    Document &append_datetime(const char *name, int64_t ms) {
        mdb_bson_append_datetime(bson_.get(), name, ms);
        return *this;
    }

    mdb_bson_t *get() const { return bson_.get(); }

    mdb_bson_t *release() { return bson_.release(); }

    friend std::ostream &operator<<(std::ostream &os, const Document &doc) {
        uint32_t len = 0;
        uint8_t *bytes = mdb_bson_to_bytes(doc.bson_.get(), &len);
        if (!bytes) {
            os << "{ <invalid> }";
            return os;
        }
        os << "{ bytes: " << len << " }";
        free(bytes);
        return os;
    }

private:
    std::unique_ptr<mdb_bson_t, decltype(&mdb_bson_destroy)> bson_;
};

class Cursor {
public:
    explicit Cursor(mdb_cursor_t *raw) : cursor_(raw, mdb_cursor_destroy) {}

    Cursor(Cursor &&) noexcept = default;
    Cursor &operator=(Cursor &&) noexcept = default;

    bool next() { return mdb_cursor_next(cursor_.get()); }

    Document current() {
        mdb_bson_t *b = mdb_cursor_current(cursor_.get());
        if (!b) return Document();
        uint32_t len = 0;
        uint8_t *bytes = mdb_bson_to_bytes(b, &len);
        if (!bytes) return Document();
        mdb_bson_t *copy = mdb_bson_from_bytes(bytes, len);
        free(bytes);
        return Document(copy);
    }

    void sort(const Document &doc) { mdb_cursor_sort(cursor_.get(), doc.get()); }
    void limit(int64_t l) { mdb_cursor_limit(cursor_.get(), l); }
    void skip(int64_t s) { mdb_cursor_skip(cursor_.get(), s); }

    std::vector<Document> to_vector() {
        std::vector<Document> results;
        while (next()) {
            results.push_back(current());
        }
        return results;
    }

private:
    std::unique_ptr<mdb_cursor_t, decltype(&mdb_cursor_destroy)> cursor_;
};

class Collection;
class Database;

class Client {
public:
    explicit Client(const std::string &uri) {
        client_.reset(mdb_client_new(uri.c_str()));
        if (!client_) throw std::runtime_error("Failed to create client");
    }

    Client(Client &&) noexcept = default;
    Client &operator=(Client &&) noexcept = default;

    bool connect() { return mdb_client_connect(client_.get()); }
    void close() { mdb_client_close(client_.get()); }

    Database database(const std::string &name);

    Document list_databases() {
        mdb_bson_t *result = mdb_client_list_databases(client_.get());
        return Document(result);
    }

    bool ping() { return mdb_client_ping(client_.get()); }

    mdb_client_t *get() const { return client_.get(); }

private:
    std::unique_ptr<mdb_client_t, decltype(&mdb_client_destroy)> client_;
};

class Database {
public:
    Database(Client *parent, const std::string &name)
        : parent_(parent), name_(name) {
        db_.reset(mdb_client_database(parent_->get(), name.c_str()));
        if (!db_) throw std::runtime_error("Failed to get database");
    }

    Database(Database &&) noexcept = default;
    Database &operator=(Database &&) noexcept = default;

    Collection collection(const std::string &name);

    Document list_collections() {
        mdb_bson_t *result = mdb_database_list_collections(db_.get());
        return Document(result);
    }

    Document create_collection(const std::string &name) {
        mdb_bson_t *result = mdb_database_create_collection(db_.get(), name.c_str());
        return Document(result);
    }

    Document drop_collection(const std::string &name) {
        mdb_bson_t *result = mdb_database_drop_collection(db_.get(), name.c_str());
        return Document(result);
    }

    Document command(const Document &cmd) {
        mdb_bson_t *result = mdb_database_command(db_.get(), cmd.get());
        return Document(result);
    }

    mdb_database_t *get() const { return db_.get(); }
    const std::string &name() const { return name_; }

private:
    Client *parent_;
    std::string name_;
    std::unique_ptr<mdb_database_t, decltype(&mdb_database_destroy)> db_;
};

class Collection {
public:
    Collection(Database *parent, const std::string &name)
        : parent_(parent), name_(name) {
        coll_.reset(mdb_database_collection(parent_->get(), name.c_str()));
        if (!coll_) throw std::runtime_error("Failed to get collection");
    }

    Collection(Collection &&) noexcept = default;
    Collection &operator=(Collection &&) noexcept = default;

    Document insert_one(const Document &doc) {
        mdb_bson_t *result = nullptr;
        mdb_collection_insert_one(coll_.get(), doc.get(), &result);
        return Document(result);
    }

    Document insert_many(const std::vector<Document> &docs) {
        std::vector<const mdb_bson_t *> cdocs;
        for (auto &d : docs) cdocs.push_back(d.get());
        mdb_bson_t *result = nullptr;
        mdb_collection_insert_many(coll_.get(), cdocs.data(), cdocs.size(), &result);
        return Document(result);
    }

    Cursor find(const Document &filter = Document()) {
        mdb_cursor_t *c = mdb_collection_find(coll_.get(), filter.get());
        return Cursor(c);
    }

    Document find_one(const Document &filter = Document()) {
        mdb_bson_t *result = mdb_collection_find_one(coll_.get(), filter.get());
        return Document(result);
    }

    Document update_one(const Document &filter, const Document &update) {
        mdb_bson_t *result = nullptr;
        mdb_collection_update_one(coll_.get(), filter.get(), update.get(), &result);
        return Document(result);
    }

    Document update_many(const Document &filter, const Document &update) {
        mdb_bson_t *result = nullptr;
        mdb_collection_update_many(coll_.get(), filter.get(), update.get(), &result);
        return Document(result);
    }

    Document delete_one(const Document &filter) {
        mdb_bson_t *result = nullptr;
        mdb_collection_delete_one(coll_.get(), filter.get(), &result);
        return Document(result);
    }

    Document delete_many(const Document &filter) {
        mdb_bson_t *result = nullptr;
        mdb_collection_delete_many(coll_.get(), filter.get(), &result);
        return Document(result);
    }

    int64_t count_documents(const Document &filter = Document()) {
        return mdb_collection_count_documents(coll_.get(), filter.get());
    }

    Document create_index(const Document &keys, bool unique = false) {
        mdb_bson_t *result = nullptr;
        mdb_collection_create_index(coll_.get(), keys.get(), unique, &result);
        return Document(result);
    }

    Document drop_index(const std::string &name) {
        mdb_bson_t *result = nullptr;
        mdb_collection_drop_index(coll_.get(), name.c_str(), &result);
        return Document(result);
    }

    Cursor list_indexes() {
        mdb_cursor_t *c = mdb_collection_list_indexes(coll_.get());
        return Cursor(c);
    }

    mdb_collection_t *get() const { return coll_.get(); }
    const std::string &name() const { return name_; }

private:
    Database *parent_;
    std::string name_;
    std::unique_ptr<mdb_collection_t, decltype(&mdb_collection_destroy)> coll_;
};

inline Database Client::database(const std::string &name) {
    return Database(this, name);
}

inline Collection Database::collection(const std::string &name) {
    return Collection(this, name);
}

} // namespace mdb

#endif
