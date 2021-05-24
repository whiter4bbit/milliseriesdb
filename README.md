[![Rust](https://github.com/whiter4bbit/milliseriesdb/actions/workflows/release.yml/badge.svg)](https://github.com/whiter4bbit/milliseriesdb/actions/workflows/release.yml)

# milliseriesdb

Oversimplified time series database. I use it to collect and query to the temperature and co2 metrics sent by [co2-monitor](https://github.com/whiter4bbit/co2-monitor).

Data is collected in batches and stored in corresponding `series`:

```
POST http://localhost:8080/series/t/
{
 "entries": [
     {
         "ts": "1621890712512",
         "value": "23.0"
     },
     {
         "ts": "1621890714512",
         "value": "24.0"
     },
     {
         "ts": "1621890715512",
         "value": "26.0"
     }
 ]
}
```

Upon receiving the batch is sorted by timestamp in non-decreasing order and all entries that are higher that the last entry in the series is filtered out. That is, milliseriesdb assumes that the data send in non-decreasing order.

Query pattern:

```
GET http://localhost:8080/series/t
    ?from=2019-08-01
    &group_by=hour
    &aggregators=mean,min,max
    &limit=1000
```

Result: 

```
{
  "rows": [
    {
      "timestamp": "2019-08-18T20:00:00+00:00",
      "values": [
        {
          "Mean": 22.962649253731286
        },
        {
          "Min": 22.85
        },
        {
          "Max": 23.1
        }
      ]
    },
    {
      "timestamp": "2019-08-18T21:00:00+00:00",
      "values": [
        {
          "Mean": 22.757492625368506
        },
        {
          "Min": 22.66
        },
        {
          "Max": 22.91
        }
      ]
    }
  ]
}
```

## Storage

Each series is stored in separate directory `{db_path}/{series_name}`. Incoming batch of entries is compressed and appended to data file (as `block`) `series.dat`. For each block index entry is created (`highest ts of the block -> block offset`). Index is stored in `series.idx` and mmaped. 

Commit log is used to maintain consistency. Each entry from commit log represents:
* `data_offset: u32` - offset points to the end of last appended block in `series.dat`. The next block is written at this offset
* `index_offset: u32` - offset points to the end of last appended index entry in `series.idx`. The next index entry is written at this offset
* `highest_ts: i64` - highest timestamp of the series. Used to filter out incoming entries

After data and index files are updated and fsynced, the new commit log entry is created. Commit log is rotated (every 2Mb). Each commit log entry contains crc16 sum.

When the data is queried, last (valid) commit log entry is read. Only index entries before `commit.index_offset` and data blocks before `commit.data_offset` are considered. 

Ceiling block offset for the queried timestamp is located using binary search in the index.

### Files

Each series is stored in three files:
 * `/{series_name}/series.dat` - data file
 * `/{series_name}/series.idx` - index file
 * `/{series_name}/series.log.{0,1,2,3...}` - rotated commit log file

Numbers (u32, u64, etc..) are encoded in `bigendian`.

#### Data file
 
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

#### Index file

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

#### Commit Log

Commit log contains entries of the following format:

```
+-------------------+-------------------+-----------------+------------+
| data_offset: u32  | index_offset: u32 | highest_ts: i64 | crc16: u16 |
+-------------------+-------------------+-----------------+------------+
...
```

Each entry corresponds to the committed offset of data file, index_file and highest timestamp of the last block in data file.
