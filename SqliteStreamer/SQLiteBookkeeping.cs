using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SqliteStreamer
{
    /// <summary>
    /// Minimal bookkeeping information collected during the first pass
    /// </summary>
    [ProtoContract]
    public class SQLiteBookkeeping
    {
        [ProtoMember(1)] public SQLiteHeader Header { get; set; } = null!;
        [ProtoMember(2)] public List<TableInfo> Tables { get; set; } = new();
        [ProtoMember(3)] public Dictionary<int, PageInfo> PageIndex { get; set; } = new();
    }

    /// <summary>
    /// Information about a table discovered in sqlite_master
    /// </summary>
    [ProtoContract]
    public class TableInfo
    {
        [ProtoMember(1)] public string Type { get; set; } = string.Empty; // "table", "index", "view"
        [ProtoMember(2)] public string Name { get; set; } = string.Empty;
        [ProtoMember(3)] public string TableName { get; set; } = string.Empty;
        [ProtoMember(4)] public int RootPage { get; set; }
        [ProtoMember(5)] public string Sql { get; set; } = string.Empty;
        [ProtoMember(6)] public List<ColumnInfo> Columns { get; set; } = new();
        [ProtoMember(7)] public bool IsWithoutRowId { get; set; }
        
        internal int? PkColumnIndex;
        public override string ToString()
        {
            return $"{Type} '{Name}' (root page: {RootPage}, columns: {Columns.Count}, without_rowid: {IsWithoutRowId})";
        }
    }

    /// <summary>
    /// Information about a column in a table
    /// </summary>
    [ProtoContract]
    public class ColumnInfo
    {
        [ProtoMember(1)] public string Name { get; set; } = string.Empty;
        [ProtoMember(2)] public string Type { get; set; } = string.Empty;
        [ProtoMember(3)] public bool NotNull { get; set; }
        [ProtoMember(4)] public bool PrimaryKey { get; set; }

        public override string ToString()
        {
            return $"{Name} {Type}";
        }
    }

    /// <summary>
    /// Minimal information about a page's location and type
    /// </summary>
    [ProtoContract]
    public class PageInfo
    {
        [ProtoMember(1)] public int PageNumber { get; set; }
        [ProtoMember(2)] public byte PageType { get; set; }
        [ProtoMember(3)] public int CellCount { get; set; }

        // For interior pages: child page numbers
        [ProtoMember(4)] public List<int> ChildPages { get; set; } = new();
        [ProtoMember(5)] public int RightMostPointer { get; set; }

        public bool IsInteriorPage => PageType == 0x02 || PageType == 0x05;
        public bool IsLeafPage => PageType == 0x0a || PageType == 0x0d;
        public bool IsTablePage => PageType == 0x05 || PageType == 0x0d;
        public bool IsIndexPage => PageType == 0x02 || PageType == 0x0a;
    }

    /// <summary>
    /// Represents a discovered row during streaming
    /// </summary>
    public class DiscoveredRow
    {
        public required TableInfo Table { get; set; }
        public long RowId { get; set; }
        public object?[] Values { get; set; } = Array.Empty<object?>();
        public List<ColumnInfo> Columns { get; set; } = new();

        public override string ToString()
        {
            var values = string.Join(", ", Values.Select(v => v?.ToString() ?? "NULL"));
            return $"[{Table.Name}#{RowId}] {values}";
        }
    }
}
