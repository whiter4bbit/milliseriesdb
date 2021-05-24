[![Rust](https://github.com/whiter4bbit/milliseriesdb/actions/workflows/release.yml/badge.svg)](https://github.com/whiter4bbit/milliseriesdb/actions/workflows/release.yml)

# milliseriesdb

Oversimplified time series database. I created one for personal use only. The main use case is to store the data sent by [co2-monitor](https://github.com/whiter4bbit/co2-monitor).

## Data model

`Series` is ordered sequence of entries `(i64, f64)`.

## Storage

Each series is stored in three files:
 * `/{series_name}/series.dat` - data file
 * `/{series_name}/series.idx` - index
 * `/{series_name}/series.log.{0,1,2,3...}` - rotated commit log file

Numbers (u32, u64, etc..) are encoded in `bigendian`.

### Data file
 
Data file stores sequential blocks of compressed entries.

```
+--------------------+------------------+-------------------+------------+
| entries_count: u32 | compression: u8  | payload_size: u32 | crc16: u16 | -> header
+--------------------+------------------+-------------------+------------+
| compressed_payload: u8[payload_size]                                   |
+------------------------------------------------------------------------+
```

`crc16` sum is computed for `(entries_count, compression, payload_size)` triple.

Currently self-implemented [delta compression](src/storage/compression.rs) is used.

Decoded payload has the following format:
```
+----------------+------------+  \
| ts: i64        | value: f64 |   |
+----------------+------------+   |
| ts: i64        | value: f64 |   | 
+----------------+------------+    \  
| ts: i64        | value: f64 |     | entries_count
+----------------+------------+    /
...                               |
+----------------+------------+   |
| ts: i64        | value: f64 |   | 
+----------------+------------+  /
```

Entries within block are stored in non-decreasing order by timestamp. The ordering between blocks in maintained non-decreasing as well, so the last entry of the block `i` is not bigger than the first entry of the block `i + 1`.

### Index file

Index file stores entry as pair `(highest_ts, block_start_offset)` for each block:

```
+-------------------+---------------+
| highest_ts: i64   | offset: u64   |
+-------------------+---------------+
| highest_ts: i64   | offset: u64   |
+-------------------+---------------+
...
+-------------------+---------------+
| highest_ts: i64   | offset: u64   |
+-------------------+---------------+
```

Entries within index are stored in non-decreasing order by timestamp.

### Commit Log

Commit log contains entries of the following format:

```
+-------------------+-------------------+-----------------+------------+
| data_offset: u32  | index_offset: u32 | highest_ts: i64 | crc16: u16 |
+-------------------+-------------------+-----------------+------------+
...
```

Each entry corresponds to the committed offset of data file, index_file and highest timestamp of the last block in data file.