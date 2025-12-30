using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SqliteStreamer
{
    /// <summary>
    /// Utilities for parsing SQLite's binary format
    /// </summary>
    public static class SQLiteParser
    {
        /// <summary>
        /// Reads a variable-length integer (varint) from the buffer
        /// Returns the value and the number of bytes consumed
        /// </summary>
        public static (long value, int bytesRead) ReadVarint(ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length == 0)
                return (0, 0);

            long value = 0;
            int bytesRead = 0;

            // Varint encoding: up to 9 bytes
            for (int i = 0; i < 9 && i < buffer.Length; i++)
            {
                byte b = buffer[i];
                bytesRead++;

                if (i == 8)
                {
                    // 9th byte uses all 8 bits
                    value = (value << 8) | b;
                    break;
                }
                else
                {
                    // First 7 bits are data, 8th bit indicates continuation
                    value = (value << 7) | (b & 0x7F);
                    if ((b & 0x80) == 0)
                    {
                        break;
                    }
                }
            }

            return (value, bytesRead);
        }

        /// <summary>
        /// Reads a cell pointer from the cell pointer array
        /// </summary>
        public static ushort ReadCellPointer(ReadOnlySpan<byte> buffer, int index)
        {
            int offset = index * 2;
            if (offset + 2 > buffer.Length)
                return 0;
            
            return BinaryPrimitives.ReadUInt16BigEndian(buffer.Slice(offset, 2));
        }

        /// <summary>
        /// Parses a table leaf cell to extract rowid and payload
        /// Returns (rowid, payload offset in page, payload size, bytes consumed from cell start)
        /// </summary>
        public static (long rowId, int payloadOffset, int payloadSize, int headerSize) ParseTableLeafCell(ReadOnlySpan<byte> cellData)
        {
            int offset = 0;

            // Read payload size (varint)
            var (payloadSize, payloadSizeBytes) = ReadVarint(cellData.Slice(offset));
            offset += payloadSizeBytes;

            // Read rowid (varint)
            var (rowId, rowIdBytes) = ReadVarint(cellData.Slice(offset));
            offset += rowIdBytes;

            int headerSize = offset;
            return (rowId, offset, (int)payloadSize, headerSize);
        }

        /// <summary>
        /// Parses a record header to get column serial types
        /// Returns the serial types and the size of the header
        /// </summary>
        public static (long[] serialTypes, int headerSize) ParseRecordHeader(ReadOnlySpan<byte> payload)
        {
            // First varint is the header size
            var (headerSize, headerSizeBytes) = ReadVarint(payload);
            int offset = headerSizeBytes;

            var serialTypes = new List<long>();

            // Read serial types until we reach the header size
            while (offset < headerSize)
            {
                var (serialType, serialTypeBytes) = ReadVarint(payload.Slice(offset));
                serialTypes.Add(serialType);
                offset += serialTypeBytes;
            }

            return (serialTypes.ToArray(), (int)headerSize);
        }

        /// <summary>
        /// Gets the content size for a serial type
        /// </summary>
        public static int GetSerialTypeContentSize(long serialType)
        {
            return serialType switch
            {
                0 => 0,  // NULL
                1 => 1,  // 8-bit int
                2 => 2,  // 16-bit int
                3 => 3,  // 24-bit int
                4 => 4,  // 32-bit int
                5 => 6,  // 48-bit int
                6 => 8,  // 64-bit int
                7 => 8,  // IEEE 754 float
                8 => 0,  // Integer constant 0
                9 => 0,  // Integer constant 1
                10 => 0, // Reserved
                11 => 0, // Reserved
                >= 12 when serialType % 2 == 0 => (int)(serialType - 12) / 2, // BLOB
                >= 13 when serialType % 2 == 1 => (int)(serialType - 13) / 2, // TEXT
                _ => 0
            };
        }

        /// <summary>
        /// Parses a value from the payload based on its serial type
        /// </summary>
        public static object? ParseValue(ReadOnlySpan<byte> data, long serialType)
        {
            return serialType switch
            {
                0 => null, // NULL
                1 => (long)(sbyte)data[0], // 8-bit signed int
                2 => (long)BinaryPrimitives.ReadInt16BigEndian(data), // 16-bit signed int
                3 => (long)ReadInt24BigEndian(data), // 24-bit signed int
                4 => (long)BinaryPrimitives.ReadInt32BigEndian(data), // 32-bit signed int
                5 => (long)ReadInt48BigEndian(data), // 48-bit signed int
                6 => (long)BinaryPrimitives.ReadInt64BigEndian(data), // 64-bit signed int
                7 => BinaryPrimitives.ReadDoubleBigEndian(data), // IEEE 754 float
                8 => (long)0, // Integer constant 0
                9 => (long)1, // Integer constant 1
                >= 12 when serialType % 2 == 0 => data.ToArray(), // BLOB
                >= 13 when serialType % 2 == 1 => Encoding.UTF8.GetString(data), // TEXT (UTF-8)
                _ => null
            };
        }

        private static int ReadInt24BigEndian(ReadOnlySpan<byte> data)
        {
            int value = (data[0] << 16) | (data[1] << 8) | data[2];
            // Sign extend if negative
            if ((value & 0x800000) != 0)
                value |= unchecked((int)0xFF000000);
            return value;
        }

        private static long ReadInt48BigEndian(ReadOnlySpan<byte> data)
        {
            long value = ((long)data[0] << 40) | ((long)data[1] << 32) | 
                        ((long)data[2] << 24) | ((long)data[3] << 16) |
                        ((long)data[4] << 8) | data[5];
            // Sign extend if negative
            if ((value & 0x800000000000L) != 0)
                value |= unchecked((long)0xFFFF000000000000L);
            return value;
        }

        /// <summary>
        /// Simple SQL parser to extract column names from CREATE TABLE statement
        /// This is a minimal parser - not production quality but good enough for basic tables
        /// </summary>
        public static List<ColumnInfo> ParseCreateTableSQL(string sql)
        {
            var columns = new List<ColumnInfo>();

            try
            {
                // Find the column definitions between parentheses
                int startParen = sql.IndexOf('(');
                int endParen = sql.LastIndexOf(')');

                if (startParen == -1 || endParen == -1)
                    return columns;

                string columnDefs = sql.Substring(startParen + 1, endParen - startParen - 1);

                // Split by commas (this is simplistic and won't handle all cases)
                var parts = SplitColumnDefinitions(columnDefs);

                foreach (var part in parts)
                {
                    var trimmed = part.Trim();
                    if (string.IsNullOrWhiteSpace(trimmed) || 
                        trimmed.StartsWith("PRIMARY KEY", StringComparison.OrdinalIgnoreCase) ||
                        trimmed.StartsWith("FOREIGN KEY", StringComparison.OrdinalIgnoreCase) ||
                        trimmed.StartsWith("UNIQUE", StringComparison.OrdinalIgnoreCase) ||
                        trimmed.StartsWith("CHECK", StringComparison.OrdinalIgnoreCase) ||
                        trimmed.StartsWith("CONSTRAINT", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    var column = ParseColumnDefinition(trimmed);
                    if (column != null)
                    {
                        columns.Add(column);
                    }
                }
            }
            catch
            {
                // If parsing fails, return what we have
            }

            return columns;
        }

        private static List<string> SplitColumnDefinitions(string columnDefs)
        {
            var result = new List<string>();
            int parenDepth = 0;
            int lastSplit = 0;

            for (int i = 0; i < columnDefs.Length; i++)
            {
                char c = columnDefs[i];
                if (c == '(') parenDepth++;
                else if (c == ')') parenDepth--;
                else if (c == ',' && parenDepth == 0)
                {
                    result.Add(columnDefs.Substring(lastSplit, i - lastSplit));
                    lastSplit = i + 1;
                }
            }

            if (lastSplit < columnDefs.Length)
            {
                result.Add(columnDefs.Substring(lastSplit));
            }

            return result;
        }

        private static ColumnInfo? ParseColumnDefinition(string def)
        {
            var tokens = def.Split(new[] { ' ', '\t', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);
            if (tokens.Length < 1)
                return null;

            var tokensRest = tokens.Skip(1);
            var column = new ColumnInfo
            {
                Name = tokens[0].Trim('"', '\'', '`', '[', ']'),
                Type = tokensRest.FirstOrDefault(x => !x.Equals("PRIMARY", StringComparison.OrdinalIgnoreCase))?.ToUpperInvariant() ?? string.Empty
            };

            // Check for constraints
            string defUpper = def.ToUpperInvariant();
            column.NotNull = defUpper.Contains("NOT NULL");
            column.PrimaryKey = defUpper.Contains("PRIMARY KEY");

            return column;
        }

        /// <summary>
        /// Checks if the CREATE TABLE SQL contains WITHOUT ROWID
        /// </summary>
        public static bool IsWithoutRowId(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql)) return false;
            // A simple check is usually sufficient, though theoretically it could be in a comment.
            // But for sqlite_master sql column, it's usually normalized.
            return sql.IndexOf("WITHOUT ROWID", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        /// <summary>
        /// Parses an index leaf cell (used for WITHOUT ROWID tables)
        /// Returns (payload offset, payload size, header size)
        /// </summary>
        public static (int payloadOffset, int payloadSize, int headerSize) ParseIndexLeafCell(ReadOnlySpan<byte> cellData)
        {
            int offset = 0;
            // Read payload size (varint)
            var (payloadSize, payloadSizeBytes) = ReadVarint(cellData.Slice(offset));
            offset += payloadSizeBytes;

            // Index leaf cells do not have a rowid
            int headerSize = offset;
            
            return (offset, (int)payloadSize, headerSize);
        }
    }
}
