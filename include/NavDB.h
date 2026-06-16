#pragma once
#include <string>
#include <optional>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <memory>
#include <mutex>

#include <pqxx/pqxx>

using Tags = std::unordered_map<std::string, std::string>;

/**
 * NavDB wraps a single PostgreSQL connection and performs buffered bulk
 * inserts via COPY for nodes, ways, areas, roads and their tag tables.
 *
 * Each worker thread owns one NavDB instance.
 *
 * db_flush_mu is a shared mutex passed in from main — it serializes the
 * INSERT ... ON CONFLICT step across threads to avoid lock contention on
 * the shared way/area/road tables. The COPY into the per-thread temp table
 * is NOT held under this mutex (it's already isolated per thread).
 */
class NavDB {
public:
    NavDB(int thread_id,
          const std::string& host,
          const std::string& user,
          const std::string& database,
          std::mutex& db_flush_mu,
          int commit_interval = 10000);
    ~NavDB();

    NavDB(const NavDB&) = delete;
    NavDB& operator=(const NavDB&) = delete;

    // ---- Insert methods ----
    void insertNode(int64_t id, const std::string& name,
                    double lon_m, double lat_m,
                    const Tags& tags, const std::string& geog_wkb_hex);

    void insertWay(int64_t id, const std::string& name,
                   const Tags& tags, const std::string& geog_wkb_hex);

    void insertArea(int64_t id, const std::string& name,
                    const Tags& tags, const std::string& geog_wkb_hex);

    void insertRoad(int64_t id, const std::string& name,
                    const Tags& tags, const std::string& geog_wkb_hex);

    void insertRelation(int64_t id, const std::string& name,
                        const Tags& tags, const std::string& geog_wkb_hex);

    // ---- Index management ----
    void disableIndexes();   // call before way phase
    void enableIndexes();    // call before relation phase
    void createGistIndexes(); // call after all phases complete

    // ---- Finalizers ----
    void finalize_nodes();
    void finalize_ways();
    void finalize_roads();
    void finalize_relations();
    void finalize_tags(const std::string& table);

    // ---- Queries ----
    // Batched lookup — one round trip for many IDs instead of one per ID.
    // Returns a map of id -> geog (WKB hex). IDs with no match (or null
    // geog) are simply absent from the map.
    std::unordered_map<int64_t, std::string> getWays(const std::vector<int64_t>& ids);

    // ---- Delta / update methods ----
    void updateNode(int64_t id, const std::string& name,
                    double lon_m, double lat_m,
                    const Tags& tags, const std::string& geog_wkb_hex);

    void updateWay(int64_t id, const std::string& name,
                   const Tags& tags, const std::string& geog_wkb_hex);

    void updateArea(int64_t id, const std::string& name,
                    const Tags& tags, const std::string& geog_wkb_hex);

    void updateRelation(int64_t id, const std::string& name,
                        const Tags& tags, const std::string& geog_wkb_hex);

    // Delete entity and its tags from all relevant tables
    void deleteEntity(int64_t id, const std::string& type);

    // Replication sequence tracking
    int64_t getReplicationSequence();
    void    setReplicationSequence(int64_t seq);

private:
    struct NodeRecord { int64_t id; std::string name, geog; double lon_m, lat_m; };
    struct GeomRecord { int64_t id; std::string name, geog; };
    struct RelationRecord { int64_t id; std::string name; std::optional<std::string> geog; };
    struct TagRecord  { int64_t id; std::string key, value; };

    void addTags(int64_t id, const Tags& tags);
    void addWayTags(int64_t id, const Tags& tags);
    void addAreaTags(int64_t id, const Tags& tags);
    void flushNodes();
    void flushWays();
    void flushAreas();
    void flushRoads();
    void flushRelations();
    void finalize_tags_locked(const std::string& table);
    void flushViaTemp(const std::string& tmp_table, const std::string& real_table,
                      const std::string& tag_table, std::vector<GeomRecord>& buf);

    int thread_id_;
    int commit_interval_;

    std::unique_ptr<pqxx::connection> conn_;
    std::mutex& db_flush_mu_;  // shared across all NavDB instances

    std::vector<NodeRecord> node_buf_;
    std::vector<GeomRecord> way_buf_;
    std::vector<GeomRecord> area_buf_;
    std::vector<GeomRecord> road_buf_;
    std::vector<RelationRecord> relation_buf_;
    std::vector<TagRecord>  tag_buf_;
    std::vector<TagRecord>  way_tag_buf_;
    std::vector<TagRecord>  area_tag_buf_;

    static constexpr int NODE_BUFFER_SIZE = 20000;
    static constexpr int WAY_BUFFER_SIZE  = 10000;
    int way_buffer_size_ = WAY_BUFFER_SIZE;  // staggered per thread
};
