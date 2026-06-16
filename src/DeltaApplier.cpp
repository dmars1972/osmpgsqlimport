#include "DeltaApplier.h"
#include "OSMReader.h"

#include <cstring>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <cstdint>
#include <vector>
#include <optional>

// ---- WKB helpers (duplicated from main.cpp for standalone use) ----

static std::string toHex(const std::vector<uint8_t>& b) {
    std::ostringstream ss;
    for (auto c : b) ss << std::hex << std::setw(2) << std::setfill('0')
                        << static_cast<int>(c);
    return ss.str();
}

static void writeLE32(std::vector<uint8_t>& b, uint32_t v) {
    b.push_back(v & 0xFF);
    b.push_back((v >> 8) & 0xFF);
    b.push_back((v >> 16) & 0xFF);
    b.push_back((v >> 24) & 0xFF);
}

static void writeDouble(std::vector<uint8_t>& b, double v) {
    uint8_t buf[8];
    memcpy(buf, &v, 8);
    for (auto c : buf) b.push_back(c);
}

// Build WKB LineString or Polygon from Mercator coords
static std::string buildLineWKB(const std::vector<std::pair<double,double>>& pts,
                                 bool closed) {
    if (pts.size() < 2) return "";

    std::vector<uint8_t> b;
    b.push_back(1); // little endian
    if (closed && pts.size() >= 4) {
        writeLE32(b, 3); // Polygon
        writeLE32(b, 1); // 1 ring
        writeLE32(b, static_cast<uint32_t>(pts.size()));
    } else {
        writeLE32(b, 2); // LineString
        writeLE32(b, static_cast<uint32_t>(pts.size()));
    }
    for (auto& [x, y] : pts) { writeDouble(b, x); writeDouble(b, y); }
    return toHex(b);
}

// Build WKB MultiLineString from a list of WKB hex strings
static std::string mergeLinestrings(const std::vector<std::string>& hexes) {
    if (hexes.empty()) return "";

    // Collect only linestring/multilinestring parts
    std::vector<std::vector<uint8_t>> parts;
    for (auto& h : hexes) {
        if (h.size() < 10) continue;
        // Parse byte order and type
        auto fromHexChar = [](char c) -> uint8_t {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            if (c >= 'A' && c <= 'F') return c - 'A' + 10;
            return 0;
        };
        auto byte = [&](size_t i) -> uint8_t {
            return (fromHexChar(h[i*2]) << 4) | fromHexChar(h[i*2+1]);
        };
        uint32_t gtype = byte(1) | (byte(2)<<8) | (byte(3)<<16) | (byte(4)<<24);
        gtype &= 0xFFFF;
        if (gtype != 2 && gtype != 5) continue; // skip polygons

        std::vector<uint8_t> raw(h.size() / 2);
        for (size_t i = 0; i < raw.size(); ++i)
            raw[i] = (fromHexChar(h[i*2]) << 4) | fromHexChar(h[i*2+1]);
        parts.push_back(std::move(raw));
    }
    if (parts.empty()) return "";

    std::vector<uint8_t> b;
    b.push_back(1);
    writeLE32(b, 5); // MultiLineString
    writeLE32(b, static_cast<uint32_t>(parts.size()));
    for (auto& p : parts)
        b.insert(b.end(), p.begin(), p.end());
    return toHex(b);
}

// ---- DeltaApplier ----

DeltaApplier::DeltaApplier(OSMMMap& osmmap, NavDB& db)
    : osmmap_(osmmap), db_(db) {}

// ---- geometry helpers ----

std::string DeltaApplier::buildWayGeom(const WayEntry& w, bool& is_closed) {
    std::vector<std::pair<double,double>> coords;
    for (int64_t ref : w.node_refs) {
        auto pt = osmmap_.select(ref);
        if (pt) coords.push_back(*pt);
    }
    if (coords.size() < 2) { is_closed = false; return ""; }
    is_closed = (coords.front() == coords.back() && coords.size() >= 4);
    return buildLineWKB(coords, is_closed);
}

std::string DeltaApplier::buildRelationGeom(const RelationEntry& r) {
    auto way_geoms_by_id = db_.getWays(r.way_members);
    std::vector<std::string> geoms;
    geoms.reserve(r.way_members.size());
    for (int64_t wid : r.way_members) {
        auto it = way_geoms_by_id.find(wid);
        if (it != way_geoms_by_id.end() && !it->second.empty())
            geoms.push_back(it->second);
    }
    return mergeLinestrings(geoms);
}

// ---- apply dispatcher ----

void DeltaApplier::apply(OSCChange&& change) {
    std::visit([&](auto&& c) {
        using T = std::decay_t<decltype(c)>;
        if constexpr (std::is_same_v<T, NodeChange>) {
            switch (c.type) {
                case ChangeType::Create: createNode(c.node); ++created_; break;
                case ChangeType::Modify: modifyNode(c.node); ++modified_; break;
                case ChangeType::Delete: deleteNode(c.node.id); ++deleted_; break;
            }
        } else if constexpr (std::is_same_v<T, WayChange>) {
            switch (c.type) {
                case ChangeType::Create: createWay(c.way); ++created_; break;
                case ChangeType::Modify: modifyWay(c.way); ++modified_; break;
                case ChangeType::Delete: deleteWay(c.way.id); ++deleted_; break;
            }
        } else if constexpr (std::is_same_v<T, RelationChange>) {
            switch (c.type) {
                case ChangeType::Create: createRelation(c.relation); ++created_; break;
                case ChangeType::Modify: modifyRelation(c.relation); ++modified_; break;
                case ChangeType::Delete: deleteRelation(c.relation.id); ++deleted_; break;
            }
        }
    }, std::move(change));
}

void DeltaApplier::flush() {
    db_.finalize_ways();
    db_.finalize_roads();
    db_.finalize_relations();
}

// ---- node operations ----

void DeltaApplier::createNode(NodeEntry& n) {
    auto [mx, my] = toMercator(n.lon, n.lat);
    n.lon_m = mx; n.lat_m = my;
    n.geog_wkb_hex = pointWKB(n.lon, n.lat);
    osmmap_.insert(0, n.id, n.lon_m, n.lat_m);
    db_.insertNode(n.id, n.name, n.lon_m, n.lat_m, n.tags, n.geog_wkb_hex);
}

void DeltaApplier::modifyNode(NodeEntry& n) {
    auto [mx, my] = toMercator(n.lon, n.lat);
    n.lon_m = mx; n.lat_m = my;
    n.geog_wkb_hex = pointWKB(n.lon, n.lat);

    // Update mmap
    osmmap_.insert(0, n.id, n.lon_m, n.lat_m);

    // Update DB row
    db_.updateNode(n.id, n.name, n.lon_m, n.lat_m, n.tags, n.geog_wkb_hex);
}

void DeltaApplier::deleteNode(int64_t id) {
    osmmap_.remove(id);
    db_.deleteEntity(id, "node");
}

// ---- way/area operations ----

void DeltaApplier::createWay(WayEntry& w) {
    bool is_closed = false;
    std::string geom = buildWayGeom(w, is_closed);
    if (geom.empty()) return;
    if (is_closed)
        db_.insertArea(w.id, w.name, w.tags, geom);
    else
        db_.insertWay(w.id, w.name, w.tags, geom);
}

void DeltaApplier::modifyWay(WayEntry& w) {
    bool is_closed = false;
    std::string geom = buildWayGeom(w, is_closed);
    if (geom.empty()) return;

    // May have changed between way and area — delete from both, reinsert
    db_.deleteEntity(w.id, "way");
    db_.deleteEntity(w.id, "area");

    if (is_closed)
        db_.updateArea(w.id, w.name, w.tags, geom);
    else
        db_.updateWay(w.id, w.name, w.tags, geom);
}

void DeltaApplier::deleteWay(int64_t id) {
    db_.deleteEntity(id, "way");
    db_.deleteEntity(id, "area");
}

// ---- relation operations ----

void DeltaApplier::createRelation(RelationEntry& r) {
    std::string geom = buildRelationGeom(r);
    db_.insertRelation(r.id, r.name, r.tags, geom);

    auto route_it   = r.tags.find("route");
    auto highway_it = r.tags.find("highway");
    bool is_road = (route_it   != r.tags.end() && route_it->second == "road") ||
                   (highway_it != r.tags.end());
    if (is_road && !geom.empty())
        db_.insertRoad(r.id, r.name, r.tags, geom);
}

void DeltaApplier::modifyRelation(RelationEntry& r) {
    std::string geom = buildRelationGeom(r);
    db_.updateRelation(r.id, r.name, r.tags, geom);

    // Sync roads table
    db_.deleteEntity(r.id, "road");
    auto route_it   = r.tags.find("route");
    auto highway_it = r.tags.find("highway");
    bool is_road = (route_it   != r.tags.end() && route_it->second == "road") ||
                   (highway_it != r.tags.end());
    if (is_road && !geom.empty())
        db_.insertRoad(r.id, r.name, r.tags, geom);
}

void DeltaApplier::deleteRelation(int64_t id) {
    db_.deleteEntity(id, "relation");
    db_.deleteEntity(id, "road");
}
