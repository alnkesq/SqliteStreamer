using Newtonsoft.Json;
using ProtoBuf;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace SqliteStreamer
{
    /// <summary>
    /// Represents the SQLite database header (first 100 bytes of the database file)
    /// </summary>
    [ProtoContract]
    public class SQLiteHeader
    {
        [ProtoMember(1)] public string MagicString { get; set; } = string.Empty;
        [ProtoMember(2)] public int PageSize { get; set; }
        [ProtoMember(3)] public byte FileFormatWriteVersion { get; set; }
        [ProtoMember(4)] public byte FileFormatReadVersion { get; set; }
        [ProtoMember(5)] public byte ReservedSpacePerPage { get; set; }
        [ProtoMember(6)] public byte MaxEmbeddedPayloadFraction { get; set; }
        [ProtoMember(7)] public byte MinEmbeddedPayloadFraction { get; set; }
        [ProtoMember(8)] public byte LeafPayloadFraction { get; set; }
        [ProtoMember(9)] public uint FileChangeCounter { get; set; }
        [ProtoMember(10)] public uint DatabaseSizeInPages { get; set; }
        [ProtoMember(11)] public uint FirstFreelistTrunkPage { get; set; }
        [ProtoMember(12)] public uint TotalFreelistPages { get; set; }
        [ProtoMember(13)] public uint SchemaCookie { get; set; }
        [ProtoMember(14)] public uint SchemaFormatNumber { get; set; }
        [ProtoMember(15)] public uint DefaultPageCacheSize { get; set; }
        [ProtoMember(16)] public uint LargestRootBTreePage { get; set; }
        [ProtoMember(17)] public uint TextEncoding { get; set; }
        [ProtoMember(18)] public uint UserVersion { get; set; }
        [ProtoMember(19)] public uint IncrementalVacuumMode { get; set; }
        [ProtoMember(20)] public uint ApplicationId { get; set; }
        [ProtoMember(21)] public uint VersionValidFor { get; set; }
        [ProtoMember(22)] public uint SQLiteVersionNumber { get; set; }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine($"Magic String: {MagicString}");
            sb.AppendLine($"Page Size: {PageSize} bytes");
            sb.AppendLine($"Database Size: {DatabaseSizeInPages} pages");
            sb.AppendLine($"Text Encoding: {GetEncodingName(TextEncoding)}");
            sb.AppendLine($"User Version: {UserVersion}");
            sb.AppendLine($"SQLite Version: {SQLiteVersionNumber}");
            sb.AppendLine($"Freelist Pages: {TotalFreelistPages}");
            return sb.ToString();
        }

        private string GetEncodingName(uint encoding)
        {
            return encoding switch
            {
                1 => "UTF-8",
                2 => "UTF-16le",
                3 => "UTF-16be",
                _ => $"Unknown ({encoding})"
            };
        }
    }

    /// <summary>
    /// Represents a SQLite page
    /// </summary>
    public class SQLitePage
    {
        public int PageNumber { get; set; }
        public byte PageType { get; set; }
        public string PageTypeName => GetPageTypeName(PageType);
        public byte[] RawData { get; set; } = Array.Empty<byte>();

        // B-tree page specific fields
        public ushort FirstFreeBlockOffset { get; set; }
        public ushort CellCount { get; set; }
        public ushort CellContentAreaOffset { get; set; }
        public byte FragmentedFreeBytes { get; set; }
        public uint RightMostPointer { get; set; } // For interior pages

        private string GetPageTypeName(byte type)
        {
            return type switch
            {
                0x02 => "Interior Index B-Tree",
                0x05 => "Interior Table B-Tree",
                0x0a => "Leaf Index B-Tree",
                0x0d => "Leaf Table B-Tree",
                _ => $"Unknown (0x{type:X2})"
            };
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine($"Page #{PageNumber}: {PageTypeName}");
            sb.AppendLine($"  Cell Count: {CellCount}");
            sb.AppendLine($"  Cell Content Offset: {CellContentAreaOffset}");
            if (PageType == 0x02 || PageType == 0x05)
            {
                sb.AppendLine($"  Right-Most Pointer: {RightMostPointer}");
            }
            return sb.ToString();
        }
    }

    /// <summary>
    /// Reads the low-level structure of a SQLite database from a zstd-compressed file
    /// </summary>
    public class SQLiteStructureReader
    {
        private const int HEADER_SIZE = 100;

        /// <summary>
        /// Reads the SQLite database header
        /// </summary>
        public static SQLiteHeader ReadHeader(Stream sqliteStream)
        {
            var headerBytes = new byte[HEADER_SIZE];
            sqliteStream.ReadExactly(headerBytes, 0, HEADER_SIZE);
            
            var header = new SQLiteHeader();

            // Read magic string (16 bytes)
            header.MagicString = Encoding.ASCII.GetString(headerBytes, 0, 16).TrimEnd('\0');
            
            if (!header.MagicString.StartsWith("SQLite format 3"))
            {
                throw new InvalidDataException($"Invalid SQLite magic string: {header.MagicString}");
            }

            // Page size (bytes 16-17)
            ushort pageSizeRaw = BinaryPrimitives.ReadUInt16BigEndian(headerBytes.AsSpan(16, 2));
            header.PageSize = pageSizeRaw == 1 ? 65536 : pageSizeRaw;

            // File format versions
            header.FileFormatWriteVersion = headerBytes[18];
            header.FileFormatReadVersion = headerBytes[19];

            // Reserved space per page
            header.ReservedSpacePerPage = headerBytes[20];

            // Payload fractions
            header.MaxEmbeddedPayloadFraction = headerBytes[21];
            header.MinEmbeddedPayloadFraction = headerBytes[22];
            header.LeafPayloadFraction = headerBytes[23];

            // File change counter
            header.FileChangeCounter = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(24, 4));

            // Database size in pages
            header.DatabaseSizeInPages = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(28, 4));

            // Freelist information
            header.FirstFreelistTrunkPage = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(32, 4));
            header.TotalFreelistPages = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(36, 4));

            // Schema cookie
            header.SchemaCookie = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(40, 4));

            // Schema format number
            header.SchemaFormatNumber = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(44, 4));

            // Default page cache size
            header.DefaultPageCacheSize = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(48, 4));

            // Largest root b-tree page
            header.LargestRootBTreePage = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(52, 4));

            // Text encoding
            header.TextEncoding = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(56, 4));

            // User version
            header.UserVersion = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(60, 4));

            // Incremental vacuum mode
            header.IncrementalVacuumMode = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(64, 4));

            // Application ID
            header.ApplicationId = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(68, 4));

            // Version valid for
            header.VersionValidFor = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(92, 4));

            // SQLite version number
            header.SQLiteVersionNumber = BinaryPrimitives.ReadUInt32BigEndian(headerBytes.AsSpan(96, 4));

            return header;
        }

        /// <summary>
        /// Reads a single page from the SQLite database
        /// </summary>
        private static SQLitePage? ReadPage(Stream sqliteStream, int pageNumber, int pageSize, int headerOffset = 0, bool isAlreadySeekedOnEndOfHeader = false)
        {
            var pageData = new byte[pageSize];
            int bytesRead;
            if (isAlreadySeekedOnEndOfHeader)
            {
                var remaing = pageSize - headerOffset;
                bytesRead = sqliteStream.ReadFully(pageData, headerOffset, remaing);
                if (bytesRead == 0) return null;
                bytesRead += headerOffset;
            }
            else
            {
                bytesRead = sqliteStream.ReadFully(pageData, 0, pageSize);
            }

            if (bytesRead == 0)
            {
                return null; // End of stream
            }

            if (bytesRead < pageSize)
            {
                // Partial page - might be the last page
                Array.Resize(ref pageData, bytesRead);
            }

            var page = new SQLitePage
            {
                PageNumber = pageNumber,
                RawData = pageData
            };

            // Skip the database header on page 1
            int offset = headerOffset;

            if (pageData.Length <= offset)
            {
                return page; // Not enough data to read page type
            }

            // Read page type
            page.PageType = pageData[offset];

            // Only parse B-tree pages
            if (page.PageType == 0x02 || page.PageType == 0x05 || 
                page.PageType == 0x0a || page.PageType == 0x0d)
            {
                if (pageData.Length >= offset + 8)
                {
                    // First freeblock offset
                    page.FirstFreeBlockOffset = BinaryPrimitives.ReadUInt16BigEndian(pageData.AsSpan(offset + 1, 2));

                    // Number of cells
                    page.CellCount = BinaryPrimitives.ReadUInt16BigEndian(pageData.AsSpan(offset + 3, 2));

                    // Cell content area offset
                    page.CellContentAreaOffset = BinaryPrimitives.ReadUInt16BigEndian(pageData.AsSpan(offset + 5, 2));

                    // Fragmented free bytes
                    page.FragmentedFreeBytes = pageData[offset + 7];

                    // Right-most pointer for interior pages
                    if ((page.PageType == 0x02 || page.PageType == 0x05) && pageData.Length >= offset + 12)
                    {
                        page.RightMostPointer = BinaryPrimitives.ReadUInt32BigEndian(pageData.AsSpan(offset + 8, 4));
                    }
                }
            }

            return page;
        }

        /// <summary>
        /// PASS 1: Collect minimal bookkeeping information
        /// Reads schema from sqlite_master and builds page index
        /// </summary>
        public static SQLiteBookkeeping Pass1CollectBookkeeping(Stream sqliteStream)
        {
            LogDebug("=== PASS 1: Collecting Bookkeeping ===");

            var bookkeeping = new SQLiteBookkeeping();

            int pageSize;


            // Read header
            bookkeeping.Header = ReadHeader(sqliteStream);
            pageSize = bookkeeping.Header.PageSize;

            LogDebug($"Page Size: {pageSize} bytes");
            LogDebug($"Database Size: {bookkeeping.Header.DatabaseSizeInPages} pages");
            


            int pageNumber = 1;
            
            // Store sqlite_master leaf pages for later parsing
            var sqliteMasterLeafPages = new List<(int pageNum, byte[] data)>();

            // Read page 1 (root of sqlite_master B-tree)
            var page1 = ReadPage(sqliteStream, pageNumber, pageSize, HEADER_SIZE, isAlreadySeekedOnEndOfHeader: true);
            if (page1 != null)
            {
                var pageInfo = CreatePageInfo(page1, pageSize, HEADER_SIZE);
                bookkeeping.PageIndex[pageNumber] = pageInfo;
                
                // Check if page 1 itself is a leaf page
                if (pageInfo.IsLeafPage && pageInfo.IsTablePage)
                {
                    LogDebug($"DEBUG: Page 1 is a leaf page, parsing directly");
                    sqliteMasterLeafPages.Add((1, page1.RawData));
                }
                else
                {
                    // Find all leaf pages in sqlite_master B-tree
                    var masterLeafPageNums = FindLeafPagesForTable(1, bookkeeping);
                    LogDebug($"DEBUG: sqlite_master B-tree has {masterLeafPageNums.Count} leaf pages: {string.Join(", ", masterLeafPageNums)}");
                }
                
                pageNumber++;
            }

            // Read remaining pages and build index
            // Also collect sqlite_master leaf pages (if page 1 was interior)
            while (true)
            {
                var page = ReadPage(sqliteStream, pageNumber, pageSize, 0);
                if (page == null)
                    break;

                var pageInfo = CreatePageInfo(page, pageSize, 0);
                bookkeeping.PageIndex[pageNumber] = pageInfo;
                
                // Check if this is a sqlite_master leaf page
                var masterLeafPages = FindLeafPagesForTable(1, bookkeeping);
                if (masterLeafPages.Contains(pageNumber))
                {
                    LogDebug($"DEBUG: Collecting sqlite_master leaf page {pageNumber}");
                    sqliteMasterLeafPages.Add((pageNumber, page.RawData));
                }
                
                pageNumber++;
            }

            // Now parse sqlite_master from the collected leaf pages
            LogDebug($"DEBUG: Parsing {sqliteMasterLeafPages.Count} sqlite_master leaf pages");
            ParseSqliteMasterFromLeafPages(sqliteMasterLeafPages, bookkeeping);

            LogDebug($"Discovered {bookkeeping.Tables.Count} tables/indexes:");
            foreach (var table in bookkeeping.Tables.Where(t => t.Type == "table"))
            {
                LogDebug($"  {table}");
                foreach (var col in table.Columns)
                {
                    LogDebug($"    - {col}");
                }
            }
            LogDebug($"Indexed {bookkeeping.PageIndex.Count} pages");

            return bookkeeping;
        }

        private static void Log(object v)
        {
            Console.Error.WriteLine(v);
        }
        private static void LogDebug(object v)
        {
            //Console.Error.WriteLine(v);
        }

        /// <summary>
        /// PASS 2: Stream actual row data using bookkeeping information
        /// </summary>
        public static void Pass2StreamRows(
            Func<Stream> streamFactory,
            SQLiteBookkeeping bookkeeping,
            Action<DiscoveredRow> onRowDiscovered,
            IReadOnlySet<TableInfo>? tables = null)
        {
            LogDebug("=== PASS 2: Streaming Rows ===");

            int pageSize = bookkeeping.Header.PageSize;

            // Stream all rows in a single pass (null means all user tables)
            foreach (var row in StreamTableRows(streamFactory, tables, bookkeeping, pageSize))
            {
                onRowDiscovered(row);
            }
        }

        /// <summary>
        /// Streams rows from the specified tables (or all user tables if null).
        /// Reads the file only once and emits rows in the order they appear in the file.
        /// </summary>
        /// <param name="streamFactory">Function to open a fresh stream to the SQLite file</param>
        /// <param name="tables">Set of tables to read, or null to read all user tables</param>
        /// <param name="bookkeeping">Bookkeeping information from Pass 1</param>
        /// <param name="pageSize">Size of each page in bytes</param>
        private static IEnumerable<DiscoveredRow> StreamTableRows(
            Func<Stream> streamFactory, 
            IReadOnlySet<TableInfo>? tables, 
            SQLiteBookkeeping bookkeeping, 
            int pageSize)
        {
            // Determine which tables to process
            var tablesToProcess = tables ?? bookkeeping.Tables
                .Where(t => t.Type == "table" && !t.Name.StartsWith("sqlite_"))
                .ToHashSet();

            if (tablesToProcess.Count == 0)
            {
                yield break;
            }

            // Build a mapping from page number to table for all leaf pages
            var pageToTable = new Dictionary<int, TableInfo>();
            
            foreach (var table in tablesToProcess)
            {
                var leafPages = FindLeafPagesForTable(table.RootPage, bookkeeping);
                LogDebug($"Table '{table.Name}' has {leafPages.Count} leaf pages: {string.Join(", ", leafPages)}");

                table.PkColumnIndex = null;

                for (int i = 0; i < table.Columns.Count; i++)
                {
                    var col = table.Columns[i];
                    if (col.PrimaryKey)
                    {
                        table.PkColumnIndex = i;
                        break;
                    }
                }

                foreach (var pageNum in leafPages)
                {
                    pageToTable[pageNum] = table;
                }
            }

            if (pageToTable.Count == 0)
            {
                yield break;
            }

            // Waiters for overflow pages: Key = PageNumber, Value = PendingRow
            var overflowWaiters = new Dictionary<int, PendingRow>();

            int reservedSpace = bookkeeping.Header.ReservedSpacePerPage;

            // Multi-pass loop
            bool isFirstPass = true;
            
            // Loop until we have done the first pass AND there are no more pending overflow pages to resolve
            while (isFirstPass || overflowWaiters.Count > 0)
            {
                int processedWaiters = 0;
                using var sqliteStream = streamFactory();
                
                Log($"=== Starting Pass (IsFirstPass={isFirstPass}, PendingWaiters={overflowWaiters.Count}) ===");

                int currentPage = 1;
                
                // Determine the maximum page number we need to read for this pass
                int maxPageToRead = 0;
                
                if (isFirstPass)
                {
                    maxPageToRead = pageToTable.Count > 0 ? pageToTable.Keys.Max() : 0;
                }
                
                if (overflowWaiters.Count > 0)
                {
                    int maxWaiter = overflowWaiters.Keys.Max();
                    if (maxWaiter > maxPageToRead) maxPageToRead = maxWaiter;
                }

                while (currentPage <= maxPageToRead)
                {
                    // Check if we can stop early in subsequent passes
                    if (!isFirstPass && overflowWaiters.Count == 0)
                    {
                        break;
                    }

                    // Check if we need to read this page
                    bool isPageNeeded = (isFirstPass && pageToTable.ContainsKey(currentPage)) || overflowWaiters.ContainsKey(currentPage);

                    if (!isPageNeeded)
                    {
                        // Optimization: Skip logic if stream supports seeking
                        if (sqliteStream.CanSeek)
                        {
                            // Find next interesting page
                            int nextInteresting = int.MaxValue;
                            
                            if (isFirstPass)
                            {
                                // Find next key in pageToTable > currentPage
                                // This is slow O(N) but accurate. For better perf we could use a sorted list of keys
                                // Given valid page count is usually manageable, this might be ok or we simplify.
                                // Let's just do skip by page size for now.
                            }
                            
                             // If seeking is too complex to implement efficiently right now, we fallback to read-skip
                             // But we can at least skip the Read call and just Seek
                             sqliteStream.Seek(pageSize, SeekOrigin.Current);
                             currentPage++;
                             continue;
                        }
                        
                         // If we can't seek, we must read (or skip if stream supports it) to consume bytes
                         // We'll just read to keep it simple and robust for Zstd streams
                    }

                    // Read the page
                    var pageData = new byte[pageSize];
                    int bytesRead = sqliteStream.ReadFully(pageData, 0, pageSize);
                    
                    if (bytesRead == 0)
                    {
                        break; // End of file
                    }

                    // 1. Check if this is an overflow page we are waiting for
                    if (overflowWaiters.TryGetValue(currentPage, out var pending))
                    {
                        overflowWaiters.Remove(currentPage);
                        processedWaiters++;

                        // Parse overflow page
                        // Format: [4 bytes next_page] [Data...]
                        int usableOverflowSpace = pageSize - reservedSpace - 4;
                        
                        uint nextPage = BinaryPrimitives.ReadUInt32BigEndian(pageData.AsSpan(0, 4));
                        
                        int bytesNeeded = pending.Payload.Length - pending.BytesCollected;
                        int bytesToCopy = Math.Min(bytesNeeded, usableOverflowSpace);
                        
                        if (bytesToCopy > 0)
                        {
                            Buffer.BlockCopy(pageData, 4, pending.Payload, pending.BytesCollected, bytesToCopy);
                            pending.BytesCollected += bytesToCopy;
                        }

                        if (pending.BytesCollected >= pending.Payload.Length)
                        {
                            // Successfully collected all data, parse the row
                            var row = ParseRecord(pending.Payload, pending.Table, pending.RowId);
                            if (row != null)
                            {
                                yield return row;
                            }
                        }
                        else
                        {
                            // Still incomplete, queue up for next overflow page
                            int next = (int)nextPage;
                            
                            // Always add to waiters. 
                            // If it's a future page, we might catch it in this pass if we extend maxPageToRead.
                            overflowWaiters[next] = pending;
                            
                            if (next > currentPage && next > maxPageToRead)
                            {
                                maxPageToRead = next;
                            }
                        }
                    }

                    // 2. Check if this page contains data for any of our tables (Only on First Pass)
                    if (isFirstPass && pageToTable.TryGetValue(currentPage, out var table))
                    {
                        // Parse cells from this leaf page
                        int headerOffset = (currentPage == 1) ? HEADER_SIZE : 0;
                        
                        var result = ParseTableLeafPage(pageData, headerOffset, table, pageSize, reservedSpace);
                        
                        foreach (var row in result.CompleteRows)
                        {
                            yield return row;
                        }

                        foreach (var p in result.PendingRows)
                        {
                            // Add all pending rows to waiters
                            overflowWaiters[p.NextOverflowPage] = p;
                            
                            // If the overflow page is in the future, we can potentially read it in this pass
                            if (p.NextOverflowPage > currentPage && p.NextOverflowPage > maxPageToRead)
                            {
                                maxPageToRead = p.NextOverflowPage;
                            }
                        }
                    }

                    currentPage++;
                }
                
                // Safety break to prevent infinite loops if we are not making progress
                if (!isFirstPass && processedWaiters == 0 && overflowWaiters.Count > 0)
                {
                    throw new Exception($"Aborting multi-pass stream. {overflowWaiters.Count} waiters remaining but no progress made in pass.");
                    // break;
                }

                isFirstPass = false;
            }
        }

        /// <summary>
        /// Finds all leaf pages in a B-tree starting from the root
        /// </summary>
        private static List<int> FindLeafPagesForTable(int rootPage, SQLiteBookkeeping bookkeeping)
        {
            var leafPages = new List<int>();
            var toVisit = new Queue<int>();
            toVisit.Enqueue(rootPage);

            while (toVisit.Count > 0)
            {
                int pageNum = toVisit.Dequeue();
                
                if (!bookkeeping.PageIndex.TryGetValue(pageNum, out var pageInfo))
                    continue;

                if (pageInfo.IsLeafPage)
                {
                    leafPages.Add(pageNum);
                }
                else if (pageInfo.IsInteriorPage)
                {
                    // Add child pages
                    foreach (var childPage in pageInfo.ChildPages)
                    {
                        toVisit.Enqueue(childPage);
                    }
                    if (pageInfo.RightMostPointer > 0)
                    {
                        toVisit.Enqueue(pageInfo.RightMostPointer);
                    }
                }
            }

            return leafPages;
        }

        /// <summary>
        /// Parses cells from a table leaf page to extract rows
        /// </summary>
        private static ScanResult ParseTableLeafPage(byte[] pageData, int headerOffset, TableInfo table, int pageSize, int reservedSpace)
        {
            var result = new ScanResult();
            var span = pageData.AsSpan();
            
            // Read cell count
            if (span.Length < headerOffset + 8) return result;

            // byte pageType = span[headerOffset]; 
            ushort cellCount = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(headerOffset + 3, 2));

            // Read cell pointer array
            int cellPointerArrayStart = headerOffset + 8;

            for (int i = 0; i < cellCount; i++)
            {
                ushort cellOffset = SQLiteParser.ReadCellPointer(span.Slice(cellPointerArrayStart), i);
                
                if (cellOffset == 0 || cellOffset >= pageData.Length) continue;

                // Parse the cell header
                var cellSpan = span.Slice(cellOffset);
                
                long rowId = 0;
                int payloadOffset;
                int payloadSize;
                int headerSize;

                if (table.IsWithoutRowId)
                {
                    (payloadOffset, payloadSize, headerSize) = SQLiteParser.ParseIndexLeafCell(cellSpan);
                }
                else
                {
                    (rowId, payloadOffset, payloadSize, headerSize) = SQLiteParser.ParseTableLeafCell(cellSpan);
                }

                // Determine how much payload is local vs overflow
                int localSize = CalculateLocalCellSize(payloadSize, pageSize, reservedSpace, table.IsWithoutRowId);

                // Ensure strict bounds (corruption protection)
                if (headerSize + localSize > cellSpan.Length) localSize = cellSpan.Length - headerSize;

                if (localSize == payloadSize)
                {
                    // Case 1: All data is local
                    var payload = cellSpan.Slice(headerSize, localSize);
                    var row = ParseRecord(payload, table, rowId);
                    if (row != null) result.CompleteRows.Add(row);
                }
                else
                {
                    // Case 2: Data spills to overflow pages
                    // Read the first overflow page number from the last 4 bytes of the local part
                    // The standard says: "If the payload spills, the last 4 bytes of the cell content area contain the page number for the first overflow page."
                    // Wait, accurate spec check: "The last 4 bytes of the cell ... is the page number..."
                    // Yes, specifically immediately following the local payload bytes?
                    // Spec: "If P > X ... then the cell has a 4-byte overflow page pointer appended to it".
                    // So at offset: headerSize + localSize?
                    
                    if (cellSpan.Length >= headerSize + localSize + 4)
                    {
                        uint firstOverflowPage = BinaryPrimitives.ReadUInt32BigEndian(cellSpan.Slice(headerSize + localSize, 4));

                        var pending = new PendingRow
                        {
                            Table = table,
                            RowId = rowId,
                            Payload = new byte[payloadSize],
                            BytesCollected = 0,
                            NextOverflowPage = (int)firstOverflowPage
                        };

                        // Copy local data
                        // The local payload is at cellSpan[headerSize ... headerSize + localSize]
                        var localData = cellSpan.Slice(headerSize, localSize);
                        localData.CopyTo(pending.Payload.AsSpan(0, localSize));
                        pending.BytesCollected = localSize;

                        result.PendingRows.Add(pending);
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Parses a record payload into a DiscoveredRow
        /// </summary>
        private static DiscoveredRow? ParseRecord(ReadOnlySpan<byte> payload, TableInfo table, long rowId)
        {
            try
            {
                var (serialTypes, headerSize) = SQLiteParser.ParseRecordHeader(payload);
                
                var values = new object?[serialTypes.Length];
                int offset = headerSize;

                for (int i = 0; i < serialTypes.Length; i++)
                {
                    int contentSize = SQLiteParser.GetSerialTypeContentSize(serialTypes[i]);
                    
                    if (offset + contentSize > payload.Length)
                        break;

                    var valueData = payload.Slice(offset, contentSize);
                    values[i] = SQLiteParser.ParseValue(valueData, serialTypes[i]);
                    offset += contentSize;
                }

                // PK can never be null, so if it's missing, it's because we have an INTEGER PRIMARY KEY that is implicitly the ROWID.
                if (table.PkColumnIndex != null && values[table.PkColumnIndex.Value] == null) values[table.PkColumnIndex.Value] = rowId;

                return new DiscoveredRow
                {
                    Table = table,
                    RowId = rowId,
                    Values = values,
                    Columns = table.Columns
                };
            }
            catch
            {
                return null;
            }
        }

        private class PendingRow
        {
            public TableInfo Table { get; set; } = null!;
            public long RowId { get; set; }
            public byte[] Payload { get; set; } = Array.Empty<byte>();
            public int BytesCollected { get; set; }
            public int NextOverflowPage { get; set; }
        }

        private struct ScanResult
        {
            public List<DiscoveredRow> CompleteRows;
            public List<PendingRow> PendingRows;
            
            public ScanResult()
            {
                CompleteRows = new List<DiscoveredRow>();
                PendingRows = new List<PendingRow>();
            }
        }

        private static int CalculateLocalCellSize(int payloadSize, int pageSize, int reservedSpace, bool isIndex)
        {
            // SQLite spec logic 
            int U = pageSize - reservedSpace;
            
            int X;
            int M = ((U - 12) * 32 / 255) - 23;
            
            if (isIndex)
            {
                // Index B-Tree Leaf
                X = ((U - 12) * 64 / 255) - 23;
            }
            else
            {
                // Table B-Tree Leaf
                X = U - 35;
            }
            
            if (payloadSize <= X) 
            {
                return payloadSize;
            }
            
            int K = M + ((payloadSize - M) % (U - 4));
            
            if (K <= X) 
            {
                return K;
            }
            
            return M;
        }


        /// <summary>
        /// Parses sqlite_master from collected leaf pages
        /// </summary>
        private static void ParseSqliteMasterFromLeafPages(List<(int pageNum, byte[] data)> leafPages, SQLiteBookkeeping bookkeeping)
        {
            var masterTable = new TableInfo
            {
                Name = "sqlite_master",
                RootPage = 1,
                Columns = new List<ColumnInfo>
                {
                    new() { Name = "type", Type = "TEXT" },
                    new() { Name = "name", Type = "TEXT" },
                    new() { Name = "tbl_name", Type = "TEXT" },
                    new() { Name = "rootpage", Type = "INTEGER" },
                    new() { Name = "sql", Type = "TEXT" }
                }
            };

            foreach (var (pageNum, pageData) in leafPages)
            {
                LogDebug($"DEBUG: Parsing sqlite_master leaf page {pageNum}");
                
                // Page 1 has the 100-byte database header, others don't
                int headerOffset = (pageNum == 1) ? HEADER_SIZE : 0;
                
                // For sqlite_master, we assume no overflow in minimal schema case, or use defaults
                // bookkeeping.Header might be available if Pass1 is running
                int pageSize = bookkeeping.Header?.PageSize ?? 4096;
                int reserved = bookkeeping.Header?.ReservedSpacePerPage ?? 0;

                var result = ParseTableLeafPage(pageData, headerOffset, masterTable, pageSize, reserved);
                // Ignore pending rows for sqlite_master (unlikely to have overflow in schema for normal cases)
                
                LogDebug($"DEBUG: Found {result.CompleteRows.Count} rows in page {pageNum}");

                foreach (var row in result.CompleteRows)
                {
                    if (row.Values.Length >= 5)
                    {
                        var tableInfo = new TableInfo
                        {
                            Type = row.Values[0]?.ToString() ?? "",
                            Name = row.Values[1]?.ToString() ?? "",
                            TableName = row.Values[2]?.ToString() ?? "",
                            RootPage = Convert.ToInt32(row.Values[3] ?? 0),
                            Sql = row.Values[4]?.ToString() ?? ""
                        };

                        LogDebug($"DEBUG: Discovered {tableInfo.Type} '{tableInfo.Name}' at page {tableInfo.RootPage}");

                        // Parse column information from SQL
                        if (tableInfo.Type == "table" && !string.IsNullOrEmpty(tableInfo.Sql))
                        {
                            tableInfo.Columns = SQLiteParser.ParseCreateTableSQL(tableInfo.Sql);
                            tableInfo.IsWithoutRowId = SQLiteParser.IsWithoutRowId(tableInfo.Sql);
                        }

                        bookkeeping.Tables.Add(tableInfo);
                    }
                }
            }
        }

        /// <summary>
        /// Parses the sqlite_master table from page 1
        /// </summary>
        private void ParseSqliteMaster(SQLitePage page1, int pageSize, int headerOffset, SQLiteBookkeeping bookkeeping)
        {
            // sqlite_master has columns: type, name, tbl_name, rootpage, sql
            var masterTable = new TableInfo
            {
                Name = "sqlite_master",
                RootPage = 1,
                Columns = new List<ColumnInfo>
                {
                    new() { Name = "type", Type = "TEXT" },
                    new() { Name = "name", Type = "TEXT" },
                    new() { Name = "tbl_name", Type = "TEXT" },
                    new() { Name = "rootpage", Type = "INTEGER" },
                    new() { Name = "sql", Type = "TEXT" }
                }
            };

            // Check if page 1 is a leaf or interior page
            byte pageType = page1.RawData[headerOffset];
            
            List<DiscoveredRow> rows;
            
            if (pageType == 0x0d) // Leaf table page
            {
                // Page 1 is a leaf, parse it directly
                // bookkeeping.Header is populated by now in Pass1
                int pageSizeReserved = bookkeeping.Header.PageSize;
                int reserved = bookkeeping.Header.ReservedSpacePerPage;
                var res = ParseTableLeafPage(page1.RawData, headerOffset, masterTable, pageSizeReserved, reserved);
                rows = res.CompleteRows;
            }
            else if (pageType == 0x05) // Interior table page
            {
                // Page 1 is an interior page, we need to read all leaf pages
                // For now, we'll defer this and read them during the full page scan
                LogDebug($"DEBUG: Page 1 is interior page (type 0x{pageType:X2}), sqlite_master data is in leaf pages");
                rows = new List<DiscoveredRow>();
            }
            else
            {
                LogDebug($"DEBUG: Unexpected page type for page 1: 0x{pageType:X2}");
                rows = new List<DiscoveredRow>();
            }

            LogDebug($"DEBUG: Found {rows.Count} rows in sqlite_master from page 1");

            foreach (var row in rows)
            {
                if (row.Values.Length >= 5)
                {
                    var tableInfo = new TableInfo
                    {
                        Type = row.Values[0]?.ToString() ?? "",
                        Name = row.Values[1]?.ToString() ?? "",
                        TableName = row.Values[2]?.ToString() ?? "",
                        RootPage = Convert.ToInt32(row.Values[3] ?? 0),
                        Sql = row.Values[4]?.ToString() ?? ""
                    };

                    LogDebug($"DEBUG: Discovered {tableInfo.Type} '{tableInfo.Name}' at page {tableInfo.RootPage}");

                    // Parse column information from SQL
                    if (tableInfo.Type == "table" && !string.IsNullOrEmpty(tableInfo.Sql))
                    {
                        tableInfo.Columns = SQLiteParser.ParseCreateTableSQL(tableInfo.Sql);
                        tableInfo.IsWithoutRowId = SQLiteParser.IsWithoutRowId(tableInfo.Sql);
                    }

                    bookkeeping.Tables.Add(tableInfo);
                }
            }
        }

        /// <summary>
        /// Creates PageInfo from a SQLitePage
        /// </summary>
        private static PageInfo CreatePageInfo(SQLitePage page, int pageSize, int headerOffset)
        {
            var pageInfo = new PageInfo
            {
                PageNumber = page.PageNumber,
                PageType = page.PageType,
                CellCount = page.CellCount,
                RightMostPointer = (int)page.RightMostPointer
            };

            // For interior pages, read child page pointers
            if (pageInfo.IsInteriorPage)
            {
                int cellPointerArrayStart = headerOffset + 12; // Interior pages have 12-byte header
                var span = page.RawData.AsSpan();

                for (int i = 0; i < page.CellCount; i++)
                {
                    ushort cellOffset = ushort.MaxValue;

                    cellOffset = SQLiteParser.ReadCellPointer(span.Slice(cellPointerArrayStart), i);
                    if (cellOffset > 0 && cellOffset + 4 <= page.RawData.Length)
                    {
                        // First 4 bytes of interior cell is the child page number

                        int childPage = (int)BinaryPrimitives.ReadUInt32BigEndian(span.Slice(cellOffset, 4));
                        pageInfo.ChildPages.Add(childPage);


                    }

                }
            }

            return pageInfo;
        }
    }
}

