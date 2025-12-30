using DuckDbSharp;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SqliteStreamer
{
    public static partial class SQLiteStreamer
    {
       
        private static object ConvertValue(object val, Type desired)
        {
            var actual = val.GetType();
            if (actual == desired) return val;

            if (desired == typeof(string))
            {
                if (actual == typeof(byte[])) return Encoding.UTF8.GetString((byte[])val);
                return Convert.ToString(val, CultureInfo.InvariantCulture)!;
            }
            if (desired == typeof(byte[]))
            {
                if (actual == typeof(string)) return Encoding.UTF8.GetBytes((string)val);
            }
            return Convert.ChangeType(val, desired);
        }

        private static Type? GetColumnTypeOverride(Dictionary<(string? Table, string? Column), ColumnTypeOverride> columnTypeOverridesDict, string tableName, string columnName)
        {
            ColumnTypeOverride o;
            if (columnTypeOverridesDict.TryGetValue((tableName, columnName), out o)) return o.Type;
            if (columnTypeOverridesDict.TryGetValue((null, columnName), out o)) return o.Type;
            if (columnTypeOverridesDict.TryGetValue((tableName, null), out o)) return o.Type;
            if (columnTypeOverridesDict.TryGetValue((null, null), out o)) return o.Type;
            return null;
        }

        private static Type GetClrType(ColumnInfo x)
        {
            var type = x.Type;
            var parens = type.IndexOf('(');
            if (parens != -1) type = type.Substring(0, parens).Trim();
            return type switch
            {
                "TEXT" => typeof(string),
                "STRING" => typeof(string),
                "VARCHAR" => typeof(string),
                "LONGVARCHAR" => typeof(string),
                "INT" => typeof(long?),
                "LONG" => typeof(long?),
                "INTEGER" => typeof(long?),
                "DOUBLE" => typeof(double?),
                "FLOAT" => typeof(double?),
                "REAL" => typeof(double?),
                "BLOB" => typeof(byte[]),
                "" => typeof(string),
                _ => typeof(string),
            };
        }

        public static Func<Stream> CreateStreamFactory(string path)
        {

            return () =>
            {
                var fileStream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);
                Stream stream = fileStream;
                var lastprint = Stopwatch.StartNew();
                stream = new ProgressCallbackStream(fileStream, pos =>
                {
                    if (lastprint.ElapsedMilliseconds > 1000)
                    {
                        var progress = ((double)pos / fileStream.Length) * 100;
                        Console.Error.WriteLine($"Reading {path}: " + progress.ToString("0.000") + " %" + " (" + ((double)pos / (1024 * 1024)).ToString("0.0")  + " MB)");
                        lastprint.Restart();
                    }
                });
                if (path.EndsWith(".zstd", StringComparison.OrdinalIgnoreCase) || path.EndsWith(".zst", StringComparison.OrdinalIgnoreCase))
                {
                    stream = new ZstdSharp.DecompressionStream(stream, leaveOpen: false);
                }
                if (path.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
                {
                    stream = new GZipStream(stream, CompressionMode.Decompress);
                }
                if (path.EndsWith(".br", StringComparison.OrdinalIgnoreCase))
                {
                    stream = new BrotliStream(stream, CompressionMode.Decompress);
                }

                return stream;
            };
        }
        public static SQLiteBookkeeping ReadMetadataCached(string dbpath)
        {
            return ReadMetadataCached(dbpath, CreateStreamFactory(dbpath));
        }
        public static SQLiteBookkeeping ReadMetadataCached(string dbpath, Func<Stream> streamFactory)
        {

            var bookkeepingCacheFile = dbpath + ".reader-bookkeeping-cache.cache";
            var lastWriteTime = File.GetLastWriteTimeUtc(dbpath);
            if (File.Exists(dbpath + "-wal")) throw new NotSupportedException("WAL file exists, refusing to continue.");
            if (!File.Exists(bookkeepingCacheFile) || File.GetLastWriteTimeUtc(bookkeepingCacheFile) != lastWriteTime)
            {
                using var sqliteStream = streamFactory();
                var bookeeping = SQLiteStructureReader.Pass1CollectBookkeeping(sqliteStream);
                var tmpPath = bookkeepingCacheFile + ".tmp";
                using (var pbstream = File.Create(tmpPath))
                {
                    ProtoBuf.Serializer.Serialize(pbstream, bookeeping);
                }
                File.SetLastWriteTimeUtc(tmpPath, lastWriteTime);
                File.Move(tmpPath, bookkeepingCacheFile, true);
                //Console.WriteLine(JsonConvert.SerializeObject(bookkeeping, Formatting.Indented));
            }

            using (var pbstream = File.Open(bookkeepingCacheFile, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                return ProtoBuf.Serializer.Deserialize<SQLiteBookkeeping>(pbstream);
            }
            
        }
        public static void ReadRows(Func<Stream> streamFactory, SQLiteBookkeeping bookkeeping, Action<DiscoveredRow> onRowDiscovered, IReadOnlySet<TableInfo>? tables = null)
        {
            SQLiteStructureReader.Pass2StreamRows(streamFactory, bookkeeping, onRowDiscovered, tables);
        }
        public static void ReadRows(string dbpath, SQLiteBookkeeping bookkeeping, Action<DiscoveredRow> onRowDiscovered, IReadOnlySet<TableInfo>? tables = null)
        {
            SQLiteStructureReader.Pass2StreamRows(CreateStreamFactory(dbpath), bookkeeping, onRowDiscovered, tables);
        }
        public static void ReadRows(string dbpath, Action<DiscoveredRow> onRowDiscovered, IReadOnlySet<TableInfo>? tables = null)
        {
            var factory = CreateStreamFactory(dbpath);
            var bookkeeping = ReadMetadataCached(dbpath, factory);
            SQLiteStructureReader.Pass2StreamRows(factory, bookkeeping, onRowDiscovered, tables);
        }


        private record TableOutputter(ConsumablePushEnumerable<object> Enqueuer, Func<object?[], object> Serializer, Type RowType)
        {
            public long ProducedRows;
        }
        public static void ConvertToParquetDirectory(string dbpath, string destfolder, IReadOnlyList<ColumnTypeOverride>? columnTypeOverrides = null, bool ignoreTypeErrors = false, bool eraseExtraFiles = false)
        {
            Directory.CreateDirectory(destfolder);
            if (eraseExtraFiles)
            {
                foreach (var old in Directory.EnumerateFiles(destfolder, "*.parquet"))
                {
                    File.Delete(old);
                }
            }
            
            var sourceFileDate = File.GetLastWriteTimeUtc(dbpath);
            var tables = new Dictionary<TableInfo, TableOutputter>();
            var markAsComplete = new List<Action>();
            var loggedTypeErrors = ignoreTypeErrors ? new HashSet<string>() : null;
            var columnTypeOverridesDict = (columnTypeOverrides ?? []).ToDictionary(x => (x.Table, x.Column), x => x);
            var printedRows = new Dictionary<TableInfo, int>();
            SqliteStreamer.SQLiteStreamer.ReadRows(dbpath, row =>
            {
                var table = row.Table;
                printedRows.TryGetValue(table, out var v);
                v++;
                printedRows[table] = v;
                if (!tables.TryGetValue(table, out var writer))
                {
                    var (rowType, serializer) = CreateRowSerializer(table, columnTypeOverridesDict, loggedTypeErrors);
                    writer = new TableOutputter(new ConsumablePushEnumerable<object>(), serializer, rowType);
                    writer.Enqueuer.ConsumeAsync(rows =>
                    {
                        Directory.CreateDirectory(destfolder);

                        var destpath = Path.Combine(destfolder, table.Name + ".parquet");
                        typeof(SQLiteStreamer).GetMethod(nameof(WriteParquet), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(writer.RowType).Invoke(null, [destpath, rows]);
                        File.SetLastWriteTimeUtc(destpath, sourceFileDate);
                    }, out var m);
                    markAsComplete.Add(m);
                    tables.Add(table, writer);
                }
                var typedRow = writer.Serializer(row.Values);
                writer.ProducedRows++;
                if (writer.ProducedRows <= 10)
                {
                    Console.Error.WriteLine(table.Name + ": " + JsonConvert.SerializeObject(typedRow));
                }
                writer.Enqueuer.Add(typedRow);
            });
            foreach (var m in markAsComplete)
            {
                m();
            }
        }

        
        private static void WriteParquet<T>(string dest, IEnumerable<object> rows)
        {
            DuckDbUtils.WriteParquet(dest, rows.Cast<T>());
        }

        private static (Type RowType, Func<object?[], object> Transformer) CreateRowSerializer(TableInfo table, Dictionary<(string? Table, string? Column), ColumnTypeOverride> columnTypeOverridesDict, HashSet<string>? logErrorsAndContinue)
        {
            var columns = table.Columns.Select(x =>
            {
                var overridden = GetColumnTypeOverride(columnTypeOverridesDict, table.Name, x.Name);
                var clrFieldType = overridden ?? GetClrType(x);
                return (ColumnInfo: x, x.Name, ClrField: (FieldInfo?)null, ClrFieldType: clrFieldType, ClrFieldTypeNonNull: Nullable.GetUnderlyingType(clrFieldType) ?? clrFieldType, IsOverridden: overridden != null);
            }).ToArray();



            var rowType = AnonymousTypeCreator.CreateAnonymousType(columns.Select(x =>
            {
                return (x.Name, (Type?)x.ClrFieldType);
            }).ToArray());
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i].ClrField = rowType.GetField(columns[i].Name);
            }


            return (rowType, (row) =>
            {
                var rowObj = Activator.CreateInstance(rowType)!;
                // row can have fewer fields than columns, if column was added after table creation.
                if (row.Length > columns.Length) throw new Exception("Unexpected number of fields in row.");
                for (int i = 0; i < row.Length; i++)
                {
                    var val = row[i];
                    if (val == null) continue;
                    var col = columns[i];
                    var actualType = col.GetType();
                    try
                    {
                        val = ConvertValue(val, col.ClrFieldTypeNonNull);
                        col.ClrField!.SetValue(rowObj, val);
                    }
                    catch
                    {
                        var error = $"Column {table.Name}.{col.Name} is defined as {(col.IsOverridden ? col.ClrFieldType : col.ColumnInfo.Type)}, but in one of the rows, its value is '{{0}}' ({val.GetType()})";
                        if (logErrorsAndContinue != null)
                        {
                            lock (logErrorsAndContinue)
                            {
                                if (logErrorsAndContinue.Add(error))
                                {
                                    if (logErrorsAndContinue.Count == 1) Console.Error.WriteLine("Note: only the first encountered type error per column is displayed.");
                                    Console.Error.WriteLine(string.Format(error, val));
                                }
                            }
                        }
                        else
                        {
                            throw new Exception(string.Format(error, val) + ". This is supported in SQLite, but not in Parquet. Consider rerunning with logErrorsAndContinue: true, then specify overrideColumnTypes to fix all the reported errors, and rerun again but with logErrorsAndContinue: false.");
                        }
                    }
                }
                return rowObj;

            });
        }



    }

    public record struct ColumnTypeOverride(string? Table, string? Column, Type Type);
}
