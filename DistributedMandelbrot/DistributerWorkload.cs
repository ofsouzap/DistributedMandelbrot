using System;
using System.Net.Sockets;

namespace DistributedMandelbrot
{
    public partial class Distributer
    {
        public struct Workload
        {

            public uint level;
            public uint indexReal;
            public uint indexImag;

            public Workload(uint level, uint indexReal, uint indexImag)
            {
                this.level = level;
                this.indexReal = indexReal;
                this.indexImag = indexImag;
            }

            public override bool Equals(object? obj)
            {
                return obj is Workload workload &&
                       level == workload.level &&
                       indexReal == workload.indexReal &&
                       indexImag == workload.indexImag;
            }

            public static bool operator ==(Workload left, Workload right)
            {
                return left.Equals(right);
            }

            public static bool operator !=(Workload left, Workload right)
            {
                return !(left == right);
            }

            public override int GetHashCode()
                => base.GetHashCode();

            public void Send(Socket socket)
            {

                byte[] buffer;

                // Level (uint32)
                buffer = BitConverter.GetBytes(level);
                socket.Send(buffer);

                // Index Real (uint32)
                buffer = BitConverter.GetBytes(indexReal);
                socket.Send(buffer);

                // Index Imag (uint32)
                buffer = BitConverter.GetBytes(indexImag);
                socket.Send(buffer);

            }

            public static Workload Receive(Socket socket)
            {

                byte[] buffer = new byte[4];

                uint level, indexReal, indexImag;

                socket.Receive(buffer, 4, SocketFlags.None);
                level = BitConverter.ToUInt32(buffer, 0);

                socket.Receive(buffer, 4, SocketFlags.None);
                indexReal = BitConverter.ToUInt32(buffer, 0);

                socket.Receive(buffer, 4, SocketFlags.None);
                indexImag = BitConverter.ToUInt32(buffer, 0);

                return new Workload(level, indexReal, indexImag);

            }

        }
    }
}
