﻿using System;
using System.Net.Sockets;

namespace DistributedMandelbrot
{
    public partial class Distributer
    {

        public struct Workload
        {

            public uint level;

            /// <summary>
            /// The workload's maxiumum recursion depth or null if a maximum recursion depth doesn't need to be defined.
            /// When comparing workloads, if either workload's maximum recusion depth is null, this property won't be compared
            /// </summary>
            public uint? maximumRecusionDepth;

            public uint indexReal;
            public uint indexImag;

            public Workload(uint level, uint? maximumRecusionDepth, uint indexReal, uint indexImag)
            {
                this.level = level;
                this.maximumRecusionDepth = maximumRecusionDepth;
                this.indexReal = indexReal;
                this.indexImag = indexImag;
            }

            public override bool Equals(object? obj)
            {
                return obj is Workload workload &&
                       level == workload.level &&
                       (maximumRecusionDepth == null || workload.maximumRecusionDepth == null || maximumRecusionDepth == workload.maximumRecusionDepth) &&
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

                if (maximumRecusionDepth == null)
                    throw new Exception("Trying to send workload with null maximum recursion depth");

                byte[] buffer;

                // Level (uint32)
                buffer = BitConverter.GetBytes(level);
                socket.Send(buffer);

                // Maximum Recusion Depth (uint32)
                buffer = BitConverter.GetBytes((uint)maximumRecusionDepth);
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

                uint level, maximumRecusionDepth, indexReal, indexImag;

                socket.Receive(buffer, 4, SocketFlags.None);
                level = BitConverter.ToUInt32(buffer, 0);

                socket.Receive(buffer, 4, SocketFlags.None);
                maximumRecusionDepth = BitConverter.ToUInt32(buffer, 0);

                socket.Receive(buffer, 4, SocketFlags.None);
                indexReal = BitConverter.ToUInt32(buffer, 0);

                socket.Receive(buffer, 4, SocketFlags.None);
                indexImag = BitConverter.ToUInt32(buffer, 0);

                return new(level, maximumRecusionDepth, indexReal, indexImag);

            }

        }

        public struct DistributedWorkload
        {

            public Workload workload;
            public long timeoutTime;

            public DistributedWorkload(Workload workload, long timeoutTime)
            {
                this.workload = workload;
                this.timeoutTime = timeoutTime;
            }

            public bool Matches(Workload workload, long currentTime)
                => workload == this.workload && !CheckHasTimedOut(currentTime);

            public bool CheckHasTimedOut(long currentTime)
                => currentTime > timeoutTime;

        }

    }
}
