# osmpgsqlimport

A high-performance OpenStreetMap PBF importer for PostgreSQL/PostGIS, written in C++20.

Designed for full-planet and regional imports with a custom Mercator schema, parallel processing, and efficient random-access node lookup via a memory-mapped flat file.

---

## Features

- **Multi-threaded node processing** — parallel Mercator projection across configurable node threads
- **LZ4-compressed per-thread shard files** — compact on-disk node store during import, merged into a flat mmap for O(1) random coordinate lookup during way/relation processing
- **Direct PostgreSQL COPY** — no ORM, no temp tables, no ON CONFLICT overhead; bulk-loads via `pqxx::stream_to`
- **Custom Mercator schema** — all coordinates stored as EPSG:3857 (Web Mercator) to match tile rendering pipelines
- **Normalized tag tables** — separate `*_tags` tables per entity type rather than hstore columns
- **Phase-resumable** — blob-aligned PBF offset checkpoints allow restarting at any phase (nodes, merge, ways, relations, indexing, airports) without re-processing earlier phases
- **Delta/replication support** — applies OSC change files (`.osc`/`.osc.gz`) with tag diffing; polling mode tracks sequences via the database
- **Integrated OurAirports data** — loads airports, runways, frequencies, navaids, countries, and regions from [OurAirports](https://ourairports.com/) as a final step
- **GiST spatial indexes** — built after all data is loaded, not during import, for maximum efficiency

---

## Schema

### OSM tables

| Table | Description |
|---|---|
| `my_nodes` | Point geometries with Mercator coordinates |
| `my_ways` | Linear geometries (roads, paths, rivers, etc.) |
| `my_areas` | Polygon geometries (closed ways) |
| `my_roads` | Route relations tagged `route=road` or `highway=*` |
| `my_relations` | Relation geometries (MultiLineString, nullable) |
| `my_node_tags` | Key/value tags for nodes |
| `my_way_tags` | Key/value tags for ways |
| `my_area_tags` | Key/value tags for areas |
| `my_road_tags` | Key/value tags for roads |
| `my_relation_tags` | Key/value tags for relations |
| `osm_replication_state` | Tracks last applied replication sequence number |

### OurAirports tables

| Table | Description |
|---|---|
| `ap_airports` | Airports with Mercator point geometry |
| `ap_frequencies` | Radio frequencies per airport |
| `ap_runways` | Runway endpoints with Mercator geometry |
| `ap_navaids` | Navigation aids with Mercator geometry |
| `ap_countries` | Country reference data |
| `ap_regions` | Region reference data |
| `ap_tags` | Unified key/value tag table for airports and navaids |

All geometry columns use PostGIS `geometry` type in EPSG:3857 (Web Mercator), consistent with the OSM tables.

---

## Dependencies

```bash
sudo apt install \
  build-essential cmake \
  libboost-program-options-dev \
  libexpat1-dev \
  zlib1g-dev \
  libpqxx-dev \
  libpq-dev \
  libproj-dev \
  protobuf-compiler \
  libprotobuf-dev \
  liblz4-dev \
  postgresql postgresql-contrib postgis
```

On Raspberry Pi, if osmium reports a missing boost library:
```bash
sudo ln -s /usr/lib/aarch64-linux-gnu/libboost_program_options.so.1.90.0 \
           /usr/lib/aarch64-linux-gnu/libboost_program_options.so.1.83.0
sudo ldconfig
```

---

## Building

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

Produces two binaries:
- `build/osm_import` — main importer
- `build/airports_load` — standalone OurAirports loader (for testing)

---

## Database Setup

```bash
# Create database and enable PostGIS
createdb <your_db>
psql -d <your_db> -c "CREATE EXTENSION postgis;"

# Create OSM schema
psql -h localhost -U daniel -d <your_db> -f create.sql

# Create OurAirports schema (imported automatically at end of import)
psql -h localhost -U daniel -d <your_db> -f create_airports.sql
```

### PostgreSQL tuning (recommended for import)

Add to `postgresql.conf` and restart **before** starting the import:

```
shared_buffers = 1GB
wal_buffers = 16MB
work_mem = 64MB
max_wal_size = 1GB
synchronous_commit = off
autovacuum = off
```

The importer runs `VACUUM ANALYZE` on all tables automatically as its final
step (with `autovacuum` disabled during import, statistics and dead-tuple
cleanup only happen when explicitly requested). Re-enable autovacuum once
the import is complete:
```sql
ALTER SYSTEM SET autovacuum = on;
SELECT pg_reload_conf();
```

---

## Usage

### Initial import

```bash
# Get max node ID from PBF
osmium fileinfo -e planet-latest.osm.pbf | grep "Max ID"

./build/osm_import \
  -i planet-latest.osm.pbf \
  -s <your_db_server> -d <your_db> -u <your_user_id> \
  -n 20000000000 \
  -f nodes.dat \
  -v -l osm.log
```

The defaults (`-t 1 -w 6`) are tuned conservatively for low-power hardware
such as a Raspberry Pi. On a proper desktop/server, increase both for much
higher throughput — see [Hardware notes](#hardware-notes) below.

### Options

| Flag | Description | Default |
|---|---|---|
| `-i <file>` | Input PBF file | required |
| `-s <host>` | PostgreSQL host | required |
| `-d <db>` | Database name | required |
| `-u <user>` | Database user | required |
| `-t <n>` | Node threads | 1 (see [Hardware notes](#hardware-notes)) |
| `-w <n>` | Way threads | 6 (see [Hardware notes](#hardware-notes)) |
| `-q <n>` | Queue size per node thread | 10000 |
| `-f <file>` | Node mmap file path | `nodes.dat` |
| `-n <id>` | Max node ID (sizes `nodes.dat`) | 20000000000 |
| `-S <dir>` | Shard file directory | `.` |
| `-l <file>` | Log file | `osm_import.log` |
| `-v` | Verbose logging | off |
| `-R <phase>` | Resume at phase (see below) | `nodes` |

### Resume support

If an import is interrupted, resume at any phase without reprocessing earlier ones:

```bash
# Resume from merge phase (nodes already written to shards)
./build/osm_import -i planet.osm.pbf -s <your_db_server> -d <your_db> -u <your_user_id> \
  -n 20000000000 -f nodes.dat -R merge

# Resume from ways (nodes.dat already fully merged)
./build/osm_import -i planet.osm.pbf -s <your_db_server> -d <your_db> -u <your_user_id> \
  -n 20000000000 -f nodes.dat -R ways

# Resume from relations (ways already in DB)
./build/osm_import -i planet.osm.pbf -s <your_db_server> -d <your_db> -u <your_user_id> \
  -n 20000000000 -f nodes.dat -R relations

# Re-run spatial indexing only
./build/osm_import -s <your_db_server> -d <your_db> -u <your_user_id> -R indexing

# Re-run airports loading only
./build/osm_import -s <your_db_server> -d <your_db> -u <your_user_id> -R airports

# Re-run VACUUM ANALYZE only
./build/osm_import -s <your_db_server> -d <your_db> -u <your_user_id> -R vacuum
```

Resume phases: `nodes` | `merge` | `ways` | `reindex` | `relations` | `indexing` | `airports` | `vacuum`

On the first successful import, two checkpoint files are written alongside `nodes.dat`:
- `nodes.dat.offset` — byte offset of first non-node blob in the PBF (skips node section on resume)
- `nodes.dat.relations_offset` — byte offset of first relation blob (skips nodes + ways on `-R relations`)

### Delta mode

Apply a single OSC change file:
```bash
./build/osm_import -m delta -o changes.osc.gz \
  -s <your_db_server> -d <your_db> -u <your_user_id> \
  -f nodes.dat -n 20000000000
```

### Poll mode

Continuously apply replication diffs from planet.openstreetmap.org:
```bash
./build/osm_import -m poll -r minute \
  -s <your_db_server> -d <your_db> -u <your_user_id> \
  -f nodes.dat -n 20000000000

# Start from a specific sequence number
./build/osm_import -m poll -r minute -Q 5123456 \
  -s <your_db_server> -d <your_db> -u <your_user_id> \
  -f nodes.dat -n 20000000000
```

Replication granularities: `minute` | `hour` | `day`

### OurAirports standalone loader

```bash
# Run create_airports.sql first if not already done
psql -h localhost -U daniel -d <your_db> -f create_airports.sql

./build/airports_load -s <your_db_server> -d <your_db> -u <your_user_id>
```

---


## Status line

During import, a status line is printed to stdout once per second:

```
0:45:12 60.2%  N:2.1B A:0 W:0 R:0 Q:847 M:42.3% | 487.2K/s  [Nodes]
```

| Field | Description |
|---|---|
| `0:45:12` | Total elapsed time since process start (H:MM:SS) |
| `60.2%` | PBF file read progress (bytes read / file size) |
| `N:2.1B` | Total nodes processed (running total, never resets) |
| `A:137.4M` | Total areas processed (running total, never resets) |
| `W:85.5M` | Total ways processed (running total, never resets) |
| `R:122.9K` | Total relations processed (running total, never resets) |
| `Q:847` | Way queue depth (entries waiting to be processed by way threads) |
| `M:42.3%` | Merge progress — only shown during the Merging phase |
| `487.2K/s` | Throughput for the **current phase only** — computed from the count accrued since the phase began, so it reflects current speed rather than being diluted by time spent in earlier phases |
| `[Nodes]` | Current phase |

### Phases

| Phase | Description |
|---|---|
| `[Nodes]` | Reading nodes from PBF, projecting to Mercator, writing to shard files and `my_nodes` |
| `[Merging]` | Decompressing and merging per-thread shard files into the flat `nodes.dat` mmap |
| `[Ways]` | Reading ways/areas from PBF, looking up node coordinates from mmap, writing to `my_ways`/`my_areas` |
| `[Reindexing]` | Rebuilding primary key indexes on way/area/road/relation tables |
| `[Relations]` | Processing relation members, merging geometries, writing to `my_relations`/`my_roads` |
| `[Spatial Indexing]` | Building GiST spatial indexes on all geometry columns |
| `[Loading Airports]` | Downloading and loading OurAirports data |
| `[Vacuuming]` | Running `VACUUM ANALYZE` on all tables |
| `[Done]` | Import complete |

## Architecture

### Import pipeline

```
PBF file
   │
   ▼
OSMReader (producer, single thread)
   │  parses PBF blobs, emits OSMEntry variants
   │
   ├──▶ node_queues[0..N]
   │         │
   │         ▼
   │    nodeThread × N
   │    toMercator() + pointWKB()   ← parallelized across N threads
   │    OSMMMap::insert()           ← writes to per-thread LZ4 shard files
   │    NavDB::insertNode()         ← bulk COPY to my_nodes
   │         │
   │    node_barrier
   │    OSMMMap::merge()            ← decompress + merge shards into flat mmap
   │    OSMMMap::setRandomAccessHint()
   │
   └──▶ way_q
             │
             ▼
        wayThread × M
        OSMMMap::select()        ← O(1) random coord lookup via mmap
        buildWayGeom()
        NavDB::insertWay/Area()  ← bulk COPY to my_ways / my_areas
             │
        way_barrier
             │
             ▼
        RelationEntry processing
        NavDB::getWay()          ← SELECT geog FROM my_ways UNION ALL my_areas
        mergeWayGeoms()          ← filters to LineString/MultiLineString only
        NavDB::insertRelation()
             │
             ▼
        NavDB::createGistIndexes()  ← GiST spatial indexes on all geog columns
             │
             ▼
        loadAirportsData()          ← downloads + loads OurAirports CSVs
```

### Node store

The node store (`nodes.dat`) is a flat binary file: `node_id × 16 bytes = (lon_m: f64, lat_m: f64)`. Node ID is the implicit array index, so `select(id)` is a single pointer dereference with zero search overhead.

During the node phase, coordinates are written to **per-thread LZ4-compressed shard files** to avoid write contention. After all nodes are processed, shards are decompressed and merged into the flat mmap in a single pass.

For a full planet import, `nodes.dat` is sized at `max_node_id × 16 bytes` (a sparse file — actual disk usage reflects only written node IDs). With `-n 20000000000`, the logical size is ~298 GB but actual usage is ~200 GB for a 2026 planet dump.

---

## Performance

Tested on Raspberry Pi 5 (16GB RAM, dual NVMe via PCIe 2.0) running Ubuntu 26.04:

- **Node phase**: ~500-800K nodes/sec (varies with thread count and chunk size)
- **Way phase**: throughput primarily I/O-bound on node coordinate lookups from `nodes.dat`
- **Full North America import**: ~3-4 hours end-to-end

### Tuning recommendations

- Use `-t 3 -w 12` on a modern multi-core machine
- Place `nodes.dat` on your fastest storage (NVMe preferred)
- Use `-S <dir>` to place shard files on a separate drive from `nodes.dat` if available
- PostgreSQL should be on a separate drive from `nodes.dat` if possible

### Known issue: kernel lockups on Raspberry Pi with kernel 7.0.0-1011-raspi

A kernel regression in `7.0.0-1011-raspi` (Ubuntu 26.04) causes full system lockups under sustained heavy mmap write workloads. Workarounds applied in this codebase:

- `CHUNK_SIZE` reduced from 16MB to 4MB to limit write burst size
- Periodic `fsync`/`msync` every 64 chunks to bound dirty page accumulation
- `MADV_RANDOM` hint on the merged node file is applied **after** merge completes rather than at construction time (applying it early on a large sparse mapping triggers the lockup)

If you experience lockups during the node phase on other platforms, try reducing `CHUNK_SIZE` further in `OSMMMap.cpp`.

---


## Hardware notes

This importer was originally developed and tuned on a **Raspberry Pi 5**
with dual NVMe drives attached via PCIe 2.0. That combination turned out to
be a poor fit for this workload — not because of anything in the import
logic itself, but because **sustained heavy I/O (large concurrent writes
during the node phase, large concurrent random reads during ways/relations)
reliably triggered full system lockups** requiring a hard power cycle. This
happened across many different phases and code paths, which pointed at a
platform-level reliability issue (kernel/driver/PCIe-adapter related) rather
than an application bug.

### What helped on the Pi

- Reducing `CHUNK_SIZE` for the LZ4 shard writer (16MB → 4MB) and adding
  periodic `fsync`/`msync` calls bounded dirty-page accumulation and fixed
  lockups during the node phase.
- Reducing thread counts (`-t 1`, `-w 6`) reduced concurrent I/O pressure and
  allowed runs to progress significantly further before any instability.
- None of this fully eliminated the lockups under all conditions — they
  still occurred at lower frequency, in different phases, as data volume
  increased.

### Recommended hardware

If you're running this on anything other than a Pi — a normal x86_64
desktop or server with a proper PCIe 3.0/4.0 NVMe slot — none of the above
should be necessary, and you can scale thread counts up substantially:

```bash
./build/osm_import -i planet-latest.osm.pbf -s <host> -d <db> -u <user> \
  -t 3 -w 12 -n 20000000000 -f nodes.dat -v -l osm.log
```

Minimum specs for a comfortable full-planet import:
- **CPU**: any modern 4+ core x86_64 CPU — this workload is I/O-bound, not
  CPU-bound, so raw core count/clock speed matters far less than storage
- **RAM**: 16GB minimum, 32GB+ recommended (helps PostgreSQL buffer/cache
  hit rates and OS page cache for the node mmap file)
- **Storage**: a real PCIe 3.0 x4 (or better) M.2 NVMe slot, directly on the
  motherboard — avoid PCIe 2.0 and avoid USB/adapter-based NVMe enclosures
  for the node store file, since random-access throughput there is the
  primary bottleneck during the ways/relations phases
- A second NVMe/SSD for PostgreSQL's data directory, separate from the node
  store file, is recommended if available

If you do hit instability on non-Pi hardware, the same mitigations apply:
reduce `-t`/`-w`, and consider shrinking `CHUNK_SIZE` in `OSMMMap.cpp`
further.

## Data sources

- **OSM planet/extracts**: [planet.openstreetmap.org](https://planet.openstreetmap.org) or [Geofabrik](https://download.geofabrik.de)
- **OurAirports**: [davidmegginson.github.io/ourairports-data](https://davidmegginson.github.io/ourairports-data/) (downloaded automatically at import time)

---

## License

MIT License — see [LICENSE](LICENSE)

---

## Contributing

Issues and pull requests welcome. The codebase targets C++20 and is tested on aarch64 (Raspberry Pi 5) and x86_64 Linux.
