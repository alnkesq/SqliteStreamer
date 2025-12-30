# SqliteStreamer

Parses a SQLite database in a streaming way, without requiring random access.

For example, you can read a `.sqlite.zstd`, `.sqlite.gz`, or `.sqlite.br` file without materializing it to disk first.

It requires 2 passes over the file. One to read metadata and b-tree indexes, and a second one to read the actual data.
Data is emitted to a callback in whatever order it physically appears in the file.
If a file is highly fragmented (not vacuumed), more passes might be performed.

For [example](https://bsky.app/profile/alnkq.bsky.social/post/3mb7nwlhier2g), you can use it to convert a sqlite dump into a more efficient format (Parquet): [Anna's Archive Spotify metadata dump](https://annas-archive.li/torrents/spotify)
| Format    | Size | Queryable |
| -------- | -------: | ------- | 
| `spotify_clean.sqlite3.zst`  | 34 GB     |⛔ No | 
| `spotify_clean.sqlite3` | 116 GB   | ⚠ OLAP queries slow | 
| `spotify_clean/*.parquet`    | 17 GB    | ✅ Yes | 



## Usage (API)

```csharp
var metadata = SQLiteStreamer.ReadMetadataCached(dbpath);

SQLiteStreamer.ReadRows(dbpath, metadata, row =>
{
    Console.WriteLine(row.Table.Name + ": " + row.RowId + " = " + string.Join(", ", row.Values));
});
```
## Usage (CLI)

```bash
SqliteStreamer /path/to/source.sqlite.zstd /path/to/destination-folder
```
A parquet file will be created for each table in the database.

The first 10 rows of each encountered table will be printed to the console.

## Disclaimer
I'm not an expert on SQLite's low level file format structure, and this project was vibe-coded, so don't use it if data integrity is critical.

I diffed the output of both SQLite and SqliteStreamer on a dozen of databases, and they matched...

Except for 22 missing rows out of 5634 in an FTS `_idx` table, which I haven't investigated yet.
