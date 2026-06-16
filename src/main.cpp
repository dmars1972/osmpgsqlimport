#include "OSMReader.h"
#include "OSMMMap.h"
#include "NavDB.h"
#include "Log.h"

#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <barrier>
#include <atomic>
#include <vector>
#include <queue>
#include <chrono>
#include <cstring>
#include <variant>
#include <mutex>  // for std::once_flag, std::call_once
#include <unistd.h> // for _exit
#include <fstream>
#include "OSCReader.h"
#include "DeltaApplier.h"
#include "Replicator.h"
#include "AirportsLoader.h"
#include <stdexcept>
#include <algorithm>
#include <iomanip>
#include <functional>
#include <memory>

// ---- Bounded blocking queue ----

template<typename T>
class BlockingQueue {
public:
    explicit BlockingQueue(int max_size) : max_size_(max_size) {}

    void push(T item) {
        std::unique_lock lk(mu_);
        cv_not_full_.wait(lk, [&]{ return static_cast<int>(q_.size()) < max_size_ || stop_; });
        q_.push(std::move(item));
        cv_not_empty_.notify_one();
    }

    bool pop(T& out) {
        std::unique_lock lk(mu_);
        cv_not_empty_.wait(lk, [&]{ return !q_.empty() || stop_; });
        if (q_.empty()) return false;
        out = std::move(q_.front());
        q_.pop();
        cv_not_full_.notify_one();
        return true;
    }

    void shutdown() {
        { std::lock_guard lk(mu_); stop_ = true; }
        cv_not_empty_.notify_all();
        cv_not_full_.notify_all();
    }

    int size() {
        std::lock_guard lk(mu_);
        return static_cast<int>(q_.size());
    }

private:
    std::queue<T> q_;
    std::mutex mu_;
    std::condition_variable cv_not_empty_, cv_not_full_;
    int max_size_;
    bool stop_ = false;
};

// ---- Status ----

enum class Phase { Nodes, Merging, Ways, Reindexing, Indexing, Relations, AirportsLoading, Done };

static const char* phaseName(Phase p) {
    switch (p) {
        case Phase::Nodes:      return "Nodes";
        case Phase::Merging:    return "Merging";
        case Phase::Ways:       return "Ways";
        case Phase::Reindexing: return "Reindexing";
        case Phase::Indexing:    return "Spatial Indexing";
        case Phase::Relations:  return "Relations";
        case Phase::AirportsLoading: return "Loading Airports";
        case Phase::Done:       return "Done";
    }
    return "";
}

struct Status {
    // Current phase counters — reset at each phase transition for display
    std::atomic<int64_t> nodes{0};
    std::atomic<int64_t> areas{0};
    std::atomic<int64_t> ways{0};
    std::atomic<int64_t> relations{0};
    std::atomic<double>  progress{0.0};
    std::atomic<int>     phase{static_cast<int>(Phase::Nodes)};
    // Running totals — never reset, used for final summary
    std::atomic<int64_t> total_nodes{0};
    std::atomic<int64_t> total_areas{0};
    std::atomic<int64_t> total_ways{0};
    std::atomic<int64_t> total_relations{0};
    // Phase start time and counter snapshot — used to compute a per-phase
    // rate without resetting the displayed running totals (nodes/areas/
    // ways/relations always show cumulative counts).
    std::atomic<int64_t> phase_start_us{0};     // microseconds since epoch
    std::atomic<int64_t> phase_count_at_start{0}; // nodes+areas+ways+relations at phase start
};

// ---- WKB helpers ----

static std::string buildWayGeom(const std::vector<std::pair<double,double>>& coords,
                                bool& is_closed) {
    if (coords.size() < 2) return "";
    is_closed = (coords.size() >= 4 &&
                 coords.front().first  == coords.back().first &&
                 coords.front().second == coords.back().second);

    auto toHex = [](const std::vector<uint8_t>& b) {
        static const char h[] = "0123456789ABCDEF";
        std::string s; s.reserve(b.size()*2);
        for (auto x : b) { s += h[x>>4]; s += h[x&0xF]; }
        return s;
    };
    auto wd = [](std::vector<uint8_t>& buf, double v) {
        uint8_t t[8]; memcpy(t, &v, 8);
        buf.insert(buf.end(), t, t+8);
    };
    auto wu32 = [](std::vector<uint8_t>& buf, uint32_t v) {
        buf.push_back(v&0xFF); buf.push_back((v>>8)&0xFF);
        buf.push_back((v>>16)&0xFF); buf.push_back((v>>24)&0xFF);
    };

    std::vector<uint8_t> buf;
    buf.push_back(0x01);
    if (is_closed) { wu32(buf, 3); wu32(buf, 1); }
    else             wu32(buf, 2);
    wu32(buf, static_cast<uint32_t>(coords.size()));
    for (auto& [x, y] : coords) { wd(buf, x); wd(buf, y); }
    return toHex(buf);
}

static std::string mergeWayGeoms(const std::vector<std::string>& wkb_hexes) {
    auto fromHex = [](const std::string& hex) {
        std::vector<uint8_t> b; b.reserve(hex.size()/2);
        for (size_t i = 0; i + 1 < hex.size(); i += 2) {
            auto n = [](char c) -> uint8_t {
                if (c >= '0' && c <= '9') return c-'0';
                if (c >= 'A' && c <= 'F') return c-'A'+10;
                if (c >= 'a' && c <= 'f') return c-'a'+10;
                return 0;
            };
            b.push_back((n(hex[i]) << 4) | n(hex[i+1]));
        }
        return b;
    };
    auto toHex = [](const std::vector<uint8_t>& b) {
        static const char h[] = "0123456789ABCDEF";
        std::string s; s.reserve(b.size()*2);
        for (auto x : b) { s += h[x>>4]; s += h[x&0xF]; }
        return s;
    };
    auto wu32 = [](std::vector<uint8_t>& buf, uint32_t v) {
        buf.push_back(v&0xFF); buf.push_back((v>>8)&0xFF);
        buf.push_back((v>>16)&0xFF); buf.push_back((v>>24)&0xFF);
    };

    // Only include LineString (type 2) and MultiLineString (type 5) —
    // skip Polygon (type 3) and other non-linear geometries
    auto isLinear = [](const std::vector<uint8_t>& b) -> bool {
        if (b.size() < 5) return false;
        // byte 0 = byte order, bytes 1-4 = geometry type (little-endian)
        uint32_t gtype = b[1] | (b[2]<<8) | (b[3]<<16) | (b[4]<<24);
        // strip SRID flag if present
        gtype &= 0xFFFF;
        return gtype == 2 || gtype == 5;
    };

    std::vector<std::vector<uint8_t>> parts;
    for (auto& h : wkb_hexes) {
        if (h.empty()) continue;
        auto b = fromHex(h);
        if (isLinear(b)) parts.push_back(std::move(b));
    }
    if (parts.empty()) return "";

    std::vector<uint8_t> buf;
    buf.push_back(0x01);
    wu32(buf, 5);
    wu32(buf, static_cast<uint32_t>(parts.size()));
    for (auto& p : parts) buf.insert(buf.end(), p.begin(), p.end());
    return toHex(buf);
}

// ---- Args ----

enum class Mode { Import, Delta, Poll };

struct Args {
    std::string infile;
    std::string server;
    std::string database;
    std::string user;
    std::string log_file       = "osm_import.log";
    std::string nodes_file     = "nodes.dat";
    std::string shard_dir      = ".";  // directory for shard files
    int64_t     max_node_id    = 20'000'000'000LL;
    bool        verbose        = false;
    int         queue_size     = 10000;
    int         node_threads   = 4;
    int         way_threads    = 8;
    // Delta mode
    Mode        mode           = Mode::Import;
    std::string osc_file;                         // for -m delta
    std::string replication    = "minute";         // minute|hour|day
    int64_t     sequence       = -1;               // starting sequence
    int         poll_interval  = 60;               // seconds
    // Resume support
    Phase       resume_phase   = Phase::Nodes;     // -R: start at this phase
};

static int safeInt(const char* val, const char* flag) {
    try { return std::stoi(val); }
    catch (...) {
        std::cerr << "Invalid integer for " << flag << ": " << val << "\n";
        std::cout.flush(); std::cerr.flush();
        _exit(1);
    }
}

static int64_t safeInt64(const char* val, const char* flag) {
    try { return std::stoll(val); }
    catch (...) {
        std::cerr << "Invalid integer for " << flag << ": " << val << "\n";
        std::cout.flush(); std::cerr.flush();
        _exit(1);
    }
}

static Args parseArgs(int argc, char** argv) {
    Args a;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if      ((arg == "--infile"       || arg == "-i") && i+1 < argc) a.infile       = argv[++i];
        else if ((arg == "--server"       || arg == "-s") && i+1 < argc) a.server       = argv[++i];
        else if ((arg == "--database"     || arg == "-d") && i+1 < argc) a.database     = argv[++i];
        else if ((arg == "--user"         || arg == "-u") && i+1 < argc) a.user         = argv[++i];
        else if ((arg == "--nodes-file"   || arg == "-f") && i+1 < argc) a.nodes_file   = argv[++i];
        else if ((arg == "--shard-dir"    || arg == "-S") && i+1 < argc) a.shard_dir    = argv[++i];
        else if ((arg == "--max-node-id"  || arg == "-n") && i+1 < argc) a.max_node_id  = safeInt64(argv[++i], "--max-node-id");
        else if ((arg == "--log-file"     || arg == "-l") && i+1 < argc) a.log_file     = argv[++i];
        else if ((arg == "--queue-size"   || arg == "-q") && i+1 < argc) a.queue_size   = safeInt(argv[++i], "--queue-size");
        else if ((arg == "--node-threads" || arg == "-t") && i+1 < argc) a.node_threads = safeInt(argv[++i], "--node-threads");
        else if ((arg == "--way-threads"  || arg == "-w") && i+1 < argc) a.way_threads  = safeInt(argv[++i], "--way-threads");
        else if  (arg == "--verbose"      || arg == "-v")                a.verbose      = true;
        else if ((arg == "--mode"         || arg == "-m") && i+1 < argc) {
            std::string m = argv[++i];
            if      (m == "import") a.mode = Mode::Import;
            else if (m == "delta")  a.mode = Mode::Delta;
            else if (m == "poll")   a.mode = Mode::Poll;
            else { std::cerr << "Unknown mode: " << m << "\n"; std::cerr.flush(); _exit(1); }
        }
        else if ((arg == "--osc-file"     || arg == "-o") && i+1 < argc) { a.osc_file     = argv[++i]; a.mode = Mode::Delta; }
        else if ((arg == "--replication"  || arg == "-r") && i+1 < argc) a.replication   = argv[++i];
        else if ((arg == "--sequence"     || arg == "-Q") && i+1 < argc) a.sequence      = safeInt64(argv[++i], "--sequence");
        else if ((arg == "--poll-interval"|| arg == "-p") && i+1 < argc) a.poll_interval = safeInt(argv[++i], "--poll-interval");
        else if ((arg == "--resume"       || arg == "-R") && i+1 < argc) {
            std::string ph = argv[++i];
            if      (ph == "nodes")      a.resume_phase = Phase::Nodes;
            else if (ph == "merge")      a.resume_phase = Phase::Merging;
            else if (ph == "ways")       a.resume_phase = Phase::Ways;
            else if (ph == "reindex")    a.resume_phase = Phase::Reindexing;
            else if (ph == "relations")  a.resume_phase = Phase::Relations;
            else if (ph == "indexing")   a.resume_phase = Phase::Indexing;
            else if (ph == "airports")   a.resume_phase = Phase::AirportsLoading;
            else { std::cerr << "Unknown resume phase: " << ph << "\n"; std::cerr.flush(); _exit(1); }
        }
        else if (arg == "--help" || arg == "-h") {
            std::cout <<
                "Usage: osm_import -s <host> -d <db> -u <user> [options]\n"
                "  Import mode (default):\n"
                "    -i <file.osm.pbf>  Input PBF file\n"
                "    -t node_threads    (default 4)\n"
                "    -w way_threads     (default 8)\n"
                "    -q queue_size      (default 10000)\n"
                "    -f nodes_file      (default nodes.dat)\n"
                "    -n max_node_id     (default 20000000000)\n"
                "    -R phase           Resume at phase: nodes|merge|ways|reindex|\n"
                "                       relations|indexing|airports (default nodes)\n"
                "                       Prerequisites for the chosen phase must\n"
                "                       already be complete (e.g. -R ways requires\n"
                "                       nodes.dat to already contain merged data and\n"
                "                       my_nodes to be fully populated)\n"
                "  Delta mode (-m delta):\n"
                "    -o <file.osc.gz>   OSC file to apply\n"
                "    -f nodes_file      Existing nodes.dat from initial import\n"
                "    -n max_node_id     Must match initial import\n"
                "  Poll mode (-m poll):\n"
                "    -r minute|hour|day Replication granularity (default minute)\n"
                "    -Q sequence        Starting sequence (default: from DB)\n"
                "    -p poll_interval   Seconds between checks (default 60)\n"
                "    -f nodes_file      Existing nodes.dat from initial import\n"
                "    -n max_node_id     Must match initial import\n"
                "  Common:\n"
                "    -l log_file        (default osm_import.log)\n"
                "    -v                 Verbose logging\n";
            std::cout.flush();
            _exit(0);
        } else {
                }
    }
    if (a.server.empty() || a.database.empty() || a.user.empty()) {
        std::cerr << "Error: -s, -d, -u are required\n"; std::cerr.flush(); _exit(1);
    }
    if (a.mode == Mode::Import && a.infile.empty()) {
        std::cerr << "Error: -i <file.osm.pbf> required for import mode\n"; std::cerr.flush(); _exit(1);
    }
    if (a.mode == Mode::Delta && a.osc_file.empty()) {
        std::cerr << "Error: -o <file.osc.gz> required for delta mode\n"; std::cerr.flush(); _exit(1);
    }
    return a;
}


// ---- PBF resume-offset state file ----
// Records the blob-aligned byte offset where the first non-node entity
// begins, so -R merge/ways/relations can seek past all node blobs.

static std::string offsetFilePath(const Args& args) {
    return args.nodes_file + ".offset";
}

static void writeResumeOffset(const Args& args, std::streampos offset) {
    std::ofstream f(offsetFilePath(args), std::ios::trunc);
    if (!f) {
        std::cerr << "[main] warning: could not write resume offset file\n";
        return;
    }
    f << static_cast<int64_t>(offset) << "\n";
}

static std::streampos readResumeOffset(const Args& args) {
    std::ifstream f(offsetFilePath(args));
    if (!f) return std::streampos(-1);
    int64_t v = -1;
    f >> v;
    if (!f) return std::streampos(-1);
    return std::streampos(v);
}

static std::string relationsOffsetFilePath(const Args& args) {
    return args.nodes_file + ".relations_offset";
}

static void writeRelationsResumeOffset(const Args& args, std::streampos offset) {
    std::ofstream f(relationsOffsetFilePath(args), std::ios::trunc);
    if (!f) {
        std::cerr << "[main] warning: could not write relations resume offset file\n";
        return;
    }
    f << static_cast<int64_t>(offset) << "\n";
}

static std::streampos readRelationsResumeOffset(const Args& args) {
    std::ifstream f(relationsOffsetFilePath(args));
    if (!f) return std::streampos(-1);
    int64_t v = -1;
    f >> v;
    if (!f) return std::streampos(-1);
    return std::streampos(v);
}

// ---- Human-readable number formatting ----

static std::string hr(int64_t n) {
    if (n >= 1'000'000'000LL) return std::to_string(n / 1'000'000'000LL) + "." +
        std::to_string((n % 1'000'000'000LL) / 100'000'000LL) + "B";
    if (n >= 1'000'000) return std::to_string(n / 1'000'000) + "." +
        std::to_string((n % 1'000'000) / 100'000) + "M";
    if (n >= 1'000) return std::to_string(n / 1'000) + "." +
        std::to_string((n % 1'000) / 100) + "K";
    return std::to_string(n);
}

// ---- Node-phase thread ----

void nodeThread(int tid,
                BlockingQueue<OSMEntry>& my_q,
                BlockingQueue<OSMEntry>& way_q,
                Status& status,
                std::barrier<std::function<void()>>& node_barrier,
                OSMMMap& osmmap,
                std::mutex& db_flush_mu,
                const Args& args) {
    LOGI(tid, "node thread started");
    NavDB db(tid, args.server, args.user, args.database, db_flush_mu, args.queue_size);
    LOGI(tid, "db connected");

    OSMEntry entry;
    while (my_q.pop(entry)) {
        if (!std::holds_alternative<NodeEntry>(entry)) {
            // Forward to way queue and stop
            LOGI(tid, "forwarding non-node to way_q");
            way_q.push(std::move(entry));
            break;
        }

        auto& item = std::get<NodeEntry>(entry);
        // Compute projection here in the node thread (parallelizes proj_trans)
        auto [mx, my] = toMercator(item.lon, item.lat);
        item.lon_m = mx;
        item.lat_m = my;
        item.geog_wkb_hex = pointWKB(item.lon, item.lat);
        osmmap.insert(tid, item.id, item.lon_m, item.lat_m);
        db.insertNode(item.id, item.name, item.lon_m, item.lat_m,
                      item.tags, item.geog_wkb_hex);
        status.nodes.fetch_add(1, std::memory_order_relaxed);
        status.total_nodes.fetch_add(1, std::memory_order_relaxed);
    }

    LOGI(tid, "flushing nodes, waiting at node_barrier");
    db.finalize_nodes();
    db.finalize_tags("node");
    node_barrier.arrive_and_wait();
    LOGI(tid, "node thread done");
}

// ---- Way/relation-phase thread ----

void wayThread(int tid,
               BlockingQueue<OSMEntry>& q,
               Status& status,
               std::barrier<>& way_barrier,
               OSMMMap& osmmap,
               std::mutex& db_flush_mu,
               const Args& args,
               bool skip_ways = false) {
    LOGI(tid, "way thread started");
    NavDB db(tid, args.server, args.user, args.database, db_flush_mu, args.queue_size);
    LOGI(tid, "db connected");

    bool way_phase_done = skip_ways;

    OSMEntry entry;
    while (q.pop(entry)) {
        std::visit([&](auto&& item) {
            using T = std::decay_t<decltype(item)>;

            if constexpr (std::is_same_v<T, NodeEntry>) {
                // Shouldn't happen — ignore
            } else if constexpr (std::is_same_v<T, WayEntry>) {
                if (skip_ways) return;  // -R relations: ways already in DB

                // Thread 0 disables indexes at start of way phase
                static std::once_flag indexes_disabled;
                // disableIndexes() disabled — avoids catastrophic index rebuild
                // burst at reindexing phase that was triggering kernel lockups
                // std::call_once(indexes_disabled, [&db]{ db.disableIndexes(); });

                std::vector<std::pair<double,double>> coords;
                coords.reserve(item.node_refs.size());
                for (int64_t nid : item.node_refs) {
                    auto pt = osmmap.select(nid);
                    if (pt) coords.push_back(*pt);
                }

                bool is_closed = false;
                std::string geog = buildWayGeom(coords, is_closed);
                if (geog.empty()) return;

                if (is_closed) {
                    db.insertArea(item.id, item.name, item.tags, geog);
                    status.areas.fetch_add(1, std::memory_order_relaxed);
                    status.total_areas.fetch_add(1, std::memory_order_relaxed);
                } else {
                    db.insertWay(item.id, item.name, item.tags, geog);
                    status.ways.fetch_add(1, std::memory_order_relaxed);
                    status.total_ways.fetch_add(1, std::memory_order_relaxed);
                }

            } else if constexpr (std::is_same_v<T, RelationEntry>) {
                if (!way_phase_done) {
                    LOGI(tid, "first relation — flushing ways, waiting at way_barrier");
                    db.finalize_ways();
                    way_barrier.arrive_and_wait();
                    LOGI(tid, "way_barrier passed");
                    // Thread 0 re-enables indexes before relation phase
                    static std::once_flag indexes_enabled;
                    std::call_once(indexes_enabled, [&db, &status]{
                        // enableIndexes() disabled — primary keys maintained inline
                        // status.phase.store(static_cast<int>(Phase::Reindexing)...)
                        // db.enableIndexes();
                        status.phase_count_at_start.store(
                            status.nodes.load(std::memory_order_relaxed) +
                            status.areas.load(std::memory_order_relaxed) +
                            status.ways.load(std::memory_order_relaxed) +
                            status.relations.load(std::memory_order_relaxed),
                            std::memory_order_relaxed);
                        status.phase_start_us.store(
                            std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now().time_since_epoch()).count(),
                            std::memory_order_relaxed);
                        status.phase.store(static_cast<int>(Phase::Relations), std::memory_order_relaxed);
                    });
                    way_phase_done = true;
                }

                // Collect member geometries from both ways and areas via a
                // single batched query (one round trip per relation instead
                // of one per member) — this was the dominant cost in the
                // relations phase for relations with many members.
                // mergeWayGeoms filters out polygons internally so both
                // rel_geog and road_geog will only contain linestrings.
                auto way_geoms_by_id = db.getWays(item.way_members);
                std::vector<std::string> geoms;
                geoms.reserve(item.way_members.size());
                for (int64_t wid : item.way_members) {
                    auto it = way_geoms_by_id.find(wid);
                    if (it != way_geoms_by_id.end() && !it->second.empty())
                        geoms.push_back(it->second);
                }
                std::string rel_geog  = geoms.empty() ? "" : mergeWayGeoms(geoms);
                std::string road_geog = rel_geog;

                db.insertRelation(item.id, item.name, item.tags, rel_geog);
                status.relations.fetch_add(1, std::memory_order_relaxed);
                status.total_relations.fetch_add(1, std::memory_order_relaxed);

                // Store route=road or highway=* relations in my_roads
                if (!road_geog.empty()) {
                    auto route_it   = item.tags.find("route");
                    auto highway_it = item.tags.find("highway");
                    bool is_road = (route_it   != item.tags.end() && route_it->second   == "road") ||
                                   (highway_it != item.tags.end());
                    if (is_road)
                        db.insertRoad(item.id, item.name, item.tags, road_geog);
                }
            }
        }, entry);

        db.finalize_roads();
    }

    if (!way_phase_done) {
        LOGI(tid, "cleanup: flushing ways, waiting at way_barrier");
        db.finalize_ways();
        way_barrier.arrive_and_wait();
    }

    db.finalize_relations();
    LOGI(tid, "way thread done");
}

// ---- Status printer ----

void statusThread(const Status& s, std::atomic<bool>& done,
                  BlockingQueue<OSMEntry>& q,
                  const OSMMMap& osmmap,
                  const std::atomic<bool>& merge_done) {
    auto start = std::chrono::steady_clock::now();
    while (!done.load(std::memory_order_relaxed)) {
        auto now      = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - start).count();
        int64_t total  = s.nodes + s.areas + s.ways + s.relations;
        // Rate is computed from only the count accrued since this phase
        // began, using a snapshot of the cumulative total taken at the
        // phase transition — this keeps N/A/W/R as running totals for
        // display while making the displayed rate phase-local.
        int64_t phase_start = s.phase_start_us.load(std::memory_order_relaxed);
        int64_t phase_count = total - s.phase_count_at_start.load(std::memory_order_relaxed);
        double phase_elapsed = phase_start > 0
            ? std::chrono::duration<double>(
                std::chrono::microseconds(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        now.time_since_epoch()).count() - phase_start)).count()
            : elapsed;
        double rps = phase_elapsed > 0 ? phase_count / phase_elapsed : 0;
        // Format elapsed as H:MM:SS
        int64_t secs = static_cast<int64_t>(elapsed);
        int h = secs / 3600, m = (secs % 3600) / 60, sec = secs % 60;
        char elapsed_str[16];
        snprintf(elapsed_str, sizeof(elapsed_str), "%d:%02d:%02d", h, m, sec);
        std::cout << "\r" << elapsed_str
                  << " " << std::fixed << std::setprecision(1)
                  << s.progress.load() << "%  "
                  << "N:" << hr(s.nodes) << " A:" << hr(s.areas)
                  << " W:" << hr(s.ways) << " R:" << hr(s.relations)
                  << " Q:" << q.size();
        if (!merge_done.load(std::memory_order_relaxed)) {
            int64_t mp = osmmap.mergeProgress();
            int64_t mt = osmmap.mergeTotal();
            double  mp_pct = mt > 0 ? (mp * 100.0 / mt) : 0.0;
            std::cout << " M:" << std::fixed << std::setprecision(1) << mp_pct << "%";
        }
        Phase ph = static_cast<Phase>(s.phase.load(std::memory_order_relaxed));
        std::cout << " | " << hr(static_cast<int64_t>(rps)) << "/s  "
                  << "[" << phaseName(ph) << "]   "
                  << std::flush;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

// ---- Delta / Poll entry point ----

static int runDelta(const Args& args) {
    // Open existing mmap (must already exist from initial import)
    // Use num_shards=1 since we're not writing shards — just reading merged file
    OSMMMap osmmap(args.nodes_file, args.max_node_id, 1, args.shard_dir);

    std::mutex db_flush_mu;
    NavDB db(0, args.server, args.user, args.database, db_flush_mu);
    DeltaApplier applier(osmmap, db);

    if (args.mode == Mode::Delta) {
        // Apply a single OSC file
        OSCReader reader(args.osc_file);
        int64_t n = reader.parse([&](OSCChange&& c) {
            applier.apply(std::move(c));
        });
        applier.flush();
        std::cout << "Applied " << n << " changes ("
                  << "created=" << applier.created()
                  << " modified=" << applier.modified()
                  << " deleted=" << applier.deleted() << ")\n";
    } else {
        // Poll mode
        ReplicationGranularity gran = ReplicationGranularity::Minute;
        if (args.replication == "hour") gran = ReplicationGranularity::Hour;
        if (args.replication == "day")  gran = ReplicationGranularity::Day;

        Replicator replicator(applier, db, gran);

        if (args.sequence >= 0)
            replicator.setSequence(args.sequence);

        replicator.poll(args.poll_interval);
    }

    std::cout.flush();
    _exit(0); // see comment near other _exit(0) calls in main()
}

// ---- main ----

int main(int argc, char** argv) {
    Args args = parseArgs(argc, argv);

    if (args.verbose)
        Log::get().open(args.log_file);

    // Dispatch to delta/poll mode if requested
    if (args.mode != Mode::Import)
        return runDelta(args);

    LOGI(-1, "starting infile=", args.infile,
         " node_threads=", args.node_threads,
         " way_threads=", args.way_threads,
         " queue=", args.queue_size,
         " nodes_file=", args.nodes_file,
         " max_node_id=", args.max_node_id);

    auto start = std::chrono::steady_clock::now();
    std::mutex db_flush_mu_early;

    // -R indexing / -R airports: no PBF processing needed at all
    if (args.resume_phase == Phase::Indexing || args.resume_phase == Phase::AirportsLoading) {
        if (args.resume_phase == Phase::Indexing) {
            LOGI(-1, "resume: creating GiST spatial indexes");
            NavDB db(0, args.server, args.user, args.database, db_flush_mu_early);
            db.createGistIndexes();
            LOGI(-1, "GiST indexes done");
        }
        LOGI(-1, "resume: loading airports data");
        loadAirportsData(args.server, args.user, args.database);
        LOGI(-1, "airports data loaded");

        auto elapsed = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - start).count();
        std::cout << "\nResumed run done in " << elapsed << "s\n";
        std::cout.flush();
        // _exit() rather than return: skips static/global destructor
        // teardown across PROJ/protobuf/pqxx, which has been observed to
        // cause "double free or corruption" crashes after all real work has
        // already completed successfully. Since nothing meaningful needs
        // cleanup at this point (all data is committed to PostgreSQL), this
        // is safe.
        _exit(0);
    }

    // Resuming at merge/ways/relations: nodes.dat and shard files already
    // exist from the previous run — do NOT truncate them.
    if (args.resume_phase == Phase::Nodes) {
        OSMMMap::createFile(args.nodes_file, args.max_node_id, args.node_threads, args.shard_dir);
    } else {
        LOGI(-1, "resume: reusing existing nodes.dat / shard files (no createFile)");
    }
    // Resuming at ways/relations: shard data is no longer needed at all
    // (merge already happened in a prior run); for -R merge, shards must be
    // preserved (open read-only) so merge() can consume them.
    bool open_shards_for_write = (args.resume_phase == Phase::Nodes);
    OSMMMap osmmap(args.nodes_file, args.max_node_id, args.node_threads,
                   args.shard_dir, open_shards_for_write);

    // One queue per node thread — no contention between node threads
    std::vector<std::unique_ptr<BlockingQueue<OSMEntry>>> node_queues;
    node_queues.reserve(args.node_threads);
    for (int i = 0; i < args.node_threads; ++i)
        node_queues.push_back(std::make_unique<BlockingQueue<OSMEntry>>(args.queue_size));

    // Shared queue for way/relation phase
    BlockingQueue<OSMEntry> way_q(args.queue_size);

    Status              status;
    std::atomic<bool>   nodes_done{false};
    std::atomic<bool>   merge_done{false};
    std::barrier<std::function<void()>> node_barrier(
        args.node_threads,
        [&osmmap, &merge_done, &status]() noexcept {
            Log::get().write(-1, "INFO", "merge starting");
            status.phase.store(static_cast<int>(Phase::Merging), std::memory_order_relaxed);
            osmmap.merge();
            osmmap.setRandomAccessHint();
            merge_done.store(true, std::memory_order_release);
            status.phase_count_at_start.store(
                status.nodes.load(std::memory_order_relaxed) +
                status.areas.load(std::memory_order_relaxed) +
                status.ways.load(std::memory_order_relaxed) +
                status.relations.load(std::memory_order_relaxed),
                std::memory_order_relaxed);
            status.phase_start_us.store(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now().time_since_epoch()).count(),
                std::memory_order_relaxed);
            status.phase.store(static_cast<int>(Phase::Ways), std::memory_order_relaxed);
            Log::get().write(-1, "INFO", "merge complete");
        }
    );
    std::barrier<>      way_barrier(args.way_threads);
    std::mutex          db_flush_mu;
    std::atomic<bool>   status_done{false};

    LOGI(-1, "launching ", args.way_threads, " way threads");
    std::vector<std::thread> way_workers;
    way_workers.reserve(args.way_threads);
    bool skip_ways_for_relations = (args.resume_phase == Phase::Relations);
    for (int i = 0; i < args.way_threads; ++i)
        way_workers.emplace_back(wayThread,
                                 args.node_threads + i,
                                 std::ref(way_q), std::ref(status),
                                 std::ref(way_barrier),
                                 std::ref(osmmap),
                                 std::ref(db_flush_mu), std::cref(args),
                                 skip_ways_for_relations);

    bool skip_nodes = (args.resume_phase == Phase::Ways ||
                       args.resume_phase == Phase::Relations);
    bool skip_merge = skip_nodes; // mmap already merged on disk in these modes

    std::vector<std::thread> node_workers;
    if (!skip_nodes) {
        LOGI(-1, "launching ", args.node_threads, " node threads");
        node_workers.reserve(args.node_threads);
        for (int i = 0; i < args.node_threads; ++i)
            node_workers.emplace_back(nodeThread, i,
                                      std::ref(*node_queues[i]),
                                      std::ref(way_q),
                                      std::ref(status),
                                      std::ref(node_barrier),
                                      std::ref(osmmap),
                                      std::ref(db_flush_mu), std::cref(args));
    } else {
        LOGI(-1, "resume: skipping node threads (resume_phase=", phaseName(args.resume_phase), ")");
        // Shut down node queues immediately — nothing will be pushed to them
        for (auto& nq : node_queues) nq->shutdown();
        nodes_done.store(true, std::memory_order_relaxed);
        if (skip_merge) {
            osmmap.setRandomAccessHint();
            merge_done.store(true, std::memory_order_release);
            status.phase.store(static_cast<int>(Phase::Ways), std::memory_order_relaxed);
        }
    }

    auto t_status = std::thread(statusThread, std::cref(status),
                                std::ref(status_done), std::ref(way_q),
                                std::cref(osmmap), std::cref(merge_done));

    OSMReader reader(args.infile);
    auto file_size = reader.getSize();
    std::vector<OSMEntry> batch;
    int64_t batch_count = 0;

    LOGI(-1, "reading PBF file_size=", static_cast<int64_t>(file_size));

    int64_t rr = 0;  // round-robin index — must be 64-bit to handle >2^31 nodes
    // -R merge: don't re-parse/re-insert nodes — shards already on disk from
    // the previous run. We still need the node threads to reach node_barrier
    // so merge() runs, which happens naturally once node_phase ends below.
    bool resume_skip_node_emit = (args.resume_phase == Phase::Merging);
    bool node_phase = !skip_nodes;

    // -R relations: prefer the relations-offset (skips ways AND nodes
    // entirely). Falls back to the node-offset (skip nodes, still read ways
    // to discard them) if no relations offset has been recorded yet.
    if (args.resume_phase == Phase::Relations) {
        std::streampos roff = readRelationsResumeOffset(args);
        if (roff >= 0) {
            LOGI(-1, "resume: seeking to saved relations offset ", static_cast<int64_t>(roff),
                 " (skipping node and way blobs)");
            reader.seekTo(roff);
        } else {
            std::streampos off = readResumeOffset(args);
            if (off >= 0) {
                LOGI(-1, "resume: no relations offset yet — seeking to node offset ",
                     static_cast<int64_t>(off), " (skipping node blobs, will discard ways)");
                reader.seekTo(off);
            } else {
                LOGI(-1, "resume: no offset files found — falling back to full re-read with skips");
            }
        }
    }
    // -R merge/ways: seek past all node blobs using the saved resume offset,
    // if available, so we don't decompress/parse them at all.
    else if (skip_nodes || resume_skip_node_emit) {
        std::streampos off = readResumeOffset(args);
        if (off >= 0) {
            LOGI(-1, "resume: seeking to saved offset ", static_cast<int64_t>(off),
                 " (skipping node blobs)");
            reader.seekTo(off);
        } else {
            LOGI(-1, "resume: no offset file found (", offsetFilePath(args),
                 ") — falling back to full re-read with node-skip");
        }
    }

    bool relations_offset_written = false;
    std::streampos blob_start = reader.getPosition();
    while (reader.next(batch)) {
        for (auto& entry : batch) {
            if (node_phase && std::holds_alternative<NodeEntry>(entry)) {
                if (resume_skip_node_emit) continue; // drop — shards already complete
                node_queues[rr % args.node_threads]->push(std::move(entry));
                ++rr;
            } else {
                if (node_phase) {
                    // First non-node — shut down node queues and wait for merge
                    node_phase = false;
                    for (auto& nq : node_queues) nq->shutdown();
                    // Record the blob-aligned offset of the first non-node
                    // entity so future -R merge/ways/relations runs can seek
                    // straight here, skipping all node blobs.
                    writeResumeOffset(args, blob_start);
                    LOGI(-1, "node phase complete, waiting for merge");
                    // Spin until merge() completes before feeding way_q
                    while (!merge_done.load(std::memory_order_acquire))
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    LOGI(-1, "merge done, switching to way phase");
                }
                if (args.resume_phase == Phase::Relations &&
                    std::holds_alternative<WayEntry>(entry))
                    continue; // -R relations: ways already in DB, skip re-reading

                // Record the blob-aligned offset of the first relation so
                // future -R relations runs can seek straight here, skipping
                // both node AND way blobs entirely.
                if (std::holds_alternative<RelationEntry>(entry) &&
                    !relations_offset_written) {
                    writeRelationsResumeOffset(args, blob_start);
                    relations_offset_written = true;
                }

                way_q.push(std::move(entry));
            }
        }
        batch.clear();

        ++batch_count;
        if (batch_count % 500 == 0)
            LOGI(-1, "producer batch=", batch_count,
                 " pos=", static_cast<int64_t>(reader.getPosition()),
                 " wq=", way_q.size());

        double pos = static_cast<double>(reader.getPosition());
        double sz  = static_cast<double>(file_size);
        status.progress.store(sz > 0 ? (pos / sz * 100.0) : 0.0,
                              std::memory_order_relaxed);

        // Track the start of the next blob, for resume-offset purposes
        blob_start = reader.getPosition();
    }

    LOGI(-1, "producer done — shutting down node queues");
    for (auto& nq : node_queues) nq->shutdown();
    LOGI(-1, "waiting for node threads");
    for (auto& w : node_workers) w.join();
    LOGI(-1, "node threads done — shutting down way queue");

    way_q.shutdown();
    LOGI(-1, "waiting for way threads");
    for (auto& w : way_workers) w.join();
    LOGI(-1, "all threads done");

    status.phase.store(static_cast<int>(Phase::Indexing), std::memory_order_relaxed);
    LOGI(-1, "creating GiST spatial indexes");
    {
        NavDB db(0, args.server, args.user, args.database, db_flush_mu);
        db.createGistIndexes();
    }
    LOGI(-1, "GiST indexes done");
    status.phase.store(static_cast<int>(Phase::AirportsLoading), std::memory_order_relaxed);
    LOGI(-1, "loading airports data");
    loadAirportsData(args.server, args.user, args.database);
    LOGI(-1, "airports data loaded");
    status.phase.store(static_cast<int>(Phase::Done), std::memory_order_relaxed);
    status_done.store(true, std::memory_order_relaxed);
    t_status.join();

    auto elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - start).count();
    int64_t total = status.total_nodes + status.total_areas + status.total_ways + status.total_relations;

    LOGI(-1, "done elapsed=", elapsed, "s total=", total);

    std::cout << "\nDone in " << elapsed << "s | "
              << "Nodes: " << hr(status.total_nodes) << ", "
              << "Areas: " << hr(status.total_areas) << ", "
              << "Ways: "  << hr(status.total_ways)  << ", "
              << "Relations: " << hr(status.total_relations) << " | "
              << hr(static_cast<int64_t>(total / elapsed)) << "/s\n";
    std::cout.flush();
    // _exit() rather than return: see comment in the -R indexing/airports
    // resume branch above — skips static teardown that has been observed
    // to crash with "double free or corruption" after all work completes.
    _exit(0);
}
