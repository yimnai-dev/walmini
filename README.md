# WALMINI

## A mini Write-Ahead Log (WAL) with basic read and write capabilities

**Walmini** is a small WAL library written in Go, with a **stdin-driven CLI** so you can try appends, replay, and seeding by hand. It is meant for learning and experiments, not as a production database component.

### What is a WAL?

A **WAL** (write-ahead log) is a mechanism used in databases and other systems where **changes are written to a durable log first**, before (or in coordination with) applying them to the “real” store. That way you can **recover** after a crash and you preserve important guarantees: in particular, **atomicity** and **durability** are usually discussed alongside WALs as part of the **ACID** story—your change is on disk in the log in an ordered way you can reason about and replay.

### What Walmini does

- **Append-only writing** to the log: records are not updated in place; new data is appended to segment files.
- **Reading and replay** from a logical read position, driven by a **checkpoint** and which segment and index you treat as the read head.
- **Moving the read cursor** forward or backward by a number of **records** (via `Seek` / the CLI `--offset` path), for example: read the next *x* records after skipping forward 10 records, or after moving 5 records back from the current checkpoint.
- **Durability policy**: after each successful append, the segment is **synced**; each new index entry is **synced**; ref files and the checkpoint are **synced** when they are updated (e.g. segment rotation, seek, or after advancing a read). That is how the design tries to make “where we are” and “what was written” consistent on disk as much as this single-process, local implementation allows.

### Segments, indices, and why they pair

The log is split into **segment** files. Each segment has a **matching index** file:

- The **segment** stores the real payloads: a repeated pattern of a **4-byte little-endian length** and then that many bytes of data (the record).
- The **index** for that same segment stores, for every record, the **byte offset in the segment file** where that record **starts** (one `uint32` per record, little-endian).

So for a given segment id, you always know how to go from “record *k* in this segment” to an exact byte position in the `.seg` file, without scanning the whole file from the start on every read.

**Refs** and the **read checkpoint** tell you *which* segment and *which* index are active for reading, and **which record slot** in the current index to read next:

- `write` / `read` refs track the current **segment** id (for write and read, respectively; they can differ if the reader lags the writer).
- `write_index` / `read_index` refs track the current **index** file id, aligned 1:1 with segments.
- **`read_checkpoint.meta`** holds the next **record index** (slot) to read in the current index file, as an 8-byte little-endian `uint64`.

If you know the checkpoint, which **read** segment and **read** index you are on, and you keep the index in sync with the segment, you do not have to “guess” the next read position: the index gives byte offsets, and the checkpoint gives the next logical record in that index. **Pairing one index file to one segment file** is what makes that direct lookup and replay straightforward.

#### Where data lives (default)

With an empty `WALConfig` (as in `cmd/main`), the library uses a default `RootDir` of `..` (see `DEFAULT_ROOT_DIR` in `internal/wal/wal.go`). All paths below are under **`<RootDir>/data/`**:

| Path | Role |
|------|------|
| `segments/NNNNNNNNN.seg` | Binary log: repeated `[4-byte LE uint32 length][len bytes payload]`. |
| `indices/NNNNNNNNN.idx` | For each record in that segment, **4-byte LE** offset of the **start** of that record in the **`.seg`** file. |
| `write.ref` / `read.ref` | One line: current **segment** id (e.g. `000000001`). |
| `write_index.ref` / `read_index.ref` | One line: current **index** id (same numbering as segments). |
| `read_checkpoint.meta` | 8-byte **LE** `uint64`: next **record index** in the current `.idx` to read. |

If you prefer `data/` next to the module, pass `WALConfig{RootDir: "."}` when calling `Init`.

### How to run

From the **walmini** module root (where `go.mod` is):

```bash
go run ./cmd
```

The program starts and reads **stdin** line by line until you stop it. Each line should start with a command flag.

### CLI

| Input line | What it does |
|------------|----------------|
| `--write <text>` | Appends one record. Everything after the first `--write` on the line is the payload (see `cmd/main.go` for exact behavior). |
| `--read` | Reads up to the **default batch size** of **5** records from the current checkpoint, then advances the checkpoint. |
| `--read <n>` | Reads up to **n** positive records (same as above, but batch size is `n`). |
| `--read <n> --offset <k>` or `--read --offset <k>` | Before reading, moves the read position by **k** **records** (forward if `k > 0`, backward if `k < 0`) via the same path as `Seek` + `ReadNext` in the library. The default `n` when you only pass `--offset` is still **5** if you write `--read --offset 3`. |
| `--seed` | Appends a batch of **fake** records (uses `go-faker`). The library’s `SeedWAL` actually runs `max(1000, n)` appends, where `n` is what you pass (default behavior when you use `--seed` without a number is driven by the same helper—see `SeedWAL` in `internal/wal/wal.go`). |
| `--seed <n>` | Same as `--seed`, but `n` must be a positive integer; the seed loop size uses `max(1000, n)` in the current implementation. |
| `--help` | Prints a short help string (note: some help text there may not match the code in every detail—for exact behavior, prefer this README and the source). |

If a line is empty, you get a small reminder; unknown lines are rejected with a hint to use `--help`.

### Library API (high level)

| Method | Role |
|--------|------|
| `WAL.Init(config)` | Configures paths, default **2 MiB** `MaxSegmentSize`, 9-digit segment names, and creates `data/segments` and `data/indices` as needed. |
| `Append(record string)` | Appends one length-prefixed record to the current segment; appends the segment **byte offset** of that record’s start to the current index. Rotates to a new segment and index when the next write would exceed `MaxSegmentSize`. |
| `ReadNext(size, delta int)` | Reads up to `size` records; if `delta != 0`, calls `Seek(delta)` first. Updates `read_checkpoint.meta` after a successful read. |
| `Seek(delta int)` | Moves the logical read position by `delta` **records** (can cross to adjacent index/segment files when needed). |
| `SeedWAL(n int)` | Appends `max(DEFAULT_SEED_SIZE, n)` records with generated sentences; `DEFAULT_SEED_SIZE` is **1000** in `wal.go`. |

`Append` does not return a byte offset; the durable story for replay is in the **checkpoint** and the **ref** files on disk.

### Module

`module github.com/yimnai-dev/walmini` — Go **1.25**+ (see `go.mod`).

---