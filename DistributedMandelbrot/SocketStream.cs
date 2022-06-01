using System;
using System.Net.Sockets;

namespace DistributedMandelbrot
{
    public class SocketStream : Stream
    {
        private ulong sentBytes;
        private readonly Socket socket;

        public SocketStream(Socket socket)
        {
            this.socket = socket;
            sentBytes = 0;
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotSupportedException();

        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {

            if (offset != 0)
                throw new NotSupportedException("Offset must be 0 for socket stream reading");

            socket.Receive(buffer, count, SocketFlags.None);

            return count;

        }

        public override void Write(byte[] buffer, int offset, int count)
        {

            if (offset != 0)
                throw new NotSupportedException("Offset must be 0 for socket stream writing");

            socket.Send(buffer, count, SocketFlags.None);
            sentBytes += Convert.ToUInt64(count);
        }

    }
}
