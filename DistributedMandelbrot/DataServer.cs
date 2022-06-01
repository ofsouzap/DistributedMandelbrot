using System;
using System.Net;
using System.Net.Sockets;

namespace DistributedMandelbrot
{
    public class DataServer
    {

        private const int listenBacklog = 32;

        private const uint maximumPermittedRequestChunkCount = 16;

        #region Message Codes

        private const byte requestAcceptedCode = 0x00;
        private static byte[] RequestAcceptedCodeBytes => new byte[1] { requestAcceptedCode };
        private const byte requestRejectedCode = 0x01;
        private static byte[] RequestRejectedCodeBytes => new byte[1] { requestRejectedCode };
        private const byte requestNotAvailableCode = 0x02;
        private static byte[] RequestNotAvailableCodeBytes => new byte[1] { requestNotAvailableCode };

        #endregion

        private bool listening;
        private readonly object listeningLock = new();

        public delegate void LogCallback(string msg);

        private event LogCallback InfoLog;
        private event LogCallback ErrorLog;

        private readonly Socket socket;

        public DataServer(IPEndPoint endpoint,
            LogCallback InfoLog,
            LogCallback ErrorLog)
        {

            this.InfoLog = InfoLog;
            this.ErrorLog = ErrorLog;

            socket = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            InfoLog("Created socket");

            socket.Bind(endpoint);

            InfoLog("Bound socket to endpoint");

        }

        private void SetListening(bool state)
        {
            lock (listeningLock)
            {
                listening = state;
            }
        }

        /// <summary>
        /// Checks if the distributer has started listening yet
        /// </summary>
        private bool GetListening()
        {
            lock (listeningLock)
            {
                return listening;
            }
        }

        /// <summary>
        /// Synchronously starts the data server listening and handling requests
        /// </summary>
        public void StartListeningSync()
        {

            // Check not already listening

            if (GetListening())
                throw new Exception("Data server already listening");

            // Set data server to listening

            SetListening(true);

            // Set socket to listen

            socket.Listen(listenBacklog);

            InfoLog("Socket set to listen");

            // Listening loop
            while (listening)
            {

                // Accept connection
                Socket client = socket.Accept();

                InfoLog("Client accepted");

                // Serve client

                InfoLog("Serving client...");

                ServeClient(client);

                InfoLog("Served client");

                // Close connection

                client.Close();

                InfoLog("Connection closed");

            }


        }

        /// <summary>
        /// Serve a client by listening to what it wants and sending the chunks to it if valid and acceptable
        /// </summary>
        private void ServeClient(Socket client)
        {

            uint level, indexReal, indexImag;

            // Receive parameters

            byte[] buffer = new byte[4];

            client.Receive(buffer, 4, SocketFlags.None);
            level = BitConverter.ToUInt32(buffer, 0);

            client.Receive(buffer, 4, SocketFlags.None);
            indexReal = BitConverter.ToUInt32(buffer, 0);

            client.Receive(buffer, 4, SocketFlags.None);
            indexImag = BitConverter.ToUInt32(buffer, 0);

            // Check parameters valid

            if (indexReal >= level
                || indexImag >= level)
            {
                client.Send(RequestRejectedCodeBytes);
                ErrorLog("Client requested with invalid parameters. Rejecting request");
                return;
            }

            // Get requested chunks

            DataStorage.QueryChunk qChunk = new(level, indexReal, indexImag);

            DataChunk? nChunk = DataStorage.TryLoadChunks(new DataStorage.QueryChunk[1] { qChunk })[0];

            if (nChunk == null)
            {
                client.Send(RequestNotAvailableCodeBytes);
                ErrorLog("Requested chunk not available");
                return;
            }
            else
            {
                client.Send(RequestAcceptedCodeBytes);
                InfoLog("Accepting request");
            }

            // Send requested chunk

            MemoryStream chunkStream = new(DataChunk.dataChunkSize + 1); // Should never need more space than this (every value written directly and a byte for the serialization type)
            nChunk.Serialize(chunkStream);

            int serializedLength = Convert.ToInt32(chunkStream.Position);

            // Send the size of the serialized data as UNsigned 32-bit integer
            client.Send(BitConverter.GetBytes(Convert.ToUInt32(serializedLength)), 4, SocketFlags.None);

            InfoLog("Sent output length");

            // Get byte array of data
            byte[] data = new byte[serializedLength];
            chunkStream.Position = 0;
            chunkStream.Read(data, 0, serializedLength);

            // Send the serialized data
            client.Send(data, serializedLength, SocketFlags.None);

            InfoLog("Sent requested chunk");
            
        }

    }
}
