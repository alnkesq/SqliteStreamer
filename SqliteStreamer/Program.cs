using System;
using System.Linq;

namespace SqliteStreamer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.Error.WriteLine(@"Syntax: SqliteStreamer.exe C:\path\to\db.sqlite.zstd C:\parquet-destination-directory");
                Environment.Exit(1);
            }
            var dbpath = args[0];
            var destdir = args[1];

            SQLiteStreamer.ConvertToParquetDirectory(dbpath, destdir);
            //var bookkeeping = SQLiteStreamer.ReadMetadataCached(dbpath);

            //SQLiteStreamer.ReadRows(dbpath, bookkeeping, row =>
            //{
            //    Console.WriteLine(row.Table.Name + ": " + row.RowId + " = " + string.Join(", ", row.Values));
            //});
            
        }
    }



}
