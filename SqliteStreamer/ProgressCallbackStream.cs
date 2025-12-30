using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqliteStreamer
{
    public class ProgressCallbackStream : Stream
    {
        private readonly Stream _stream;
        private readonly Action<long> callback;
        public ProgressCallbackStream(Stream stream, Action<long> callback)
        {
            this._stream = stream;
            this.callback = callback;
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override long Length
        {
            get { return _stream.Length; }
        }

        public override long Position
        {
            get
            {
                return _stream.Position;
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var c = _stream.Read(buffer, offset, count);
            callback(this.Position);
            return c;
        }
        public override int Read(Span<byte> buffer)
        {
            var c = _stream.Read(buffer);
            callback(this.Position);
            return c;
        }
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _stream.Dispose();
            }
        }
    }
}
