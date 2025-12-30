using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SqliteStreamer
{
    internal static class ExtensionMethods
    {
        public static int ReadFully(this Stream stream, byte[] buffer, int offset, int length)
        {
            return ReadFully(stream, buffer.AsSpan(offset, length));
        }

        private static int ReadFully(Stream stream, Span<byte> span)
        {
            return stream.ReadAtLeast(span, span.Length, throwOnEndOfStream: false);
        }
    }
}
