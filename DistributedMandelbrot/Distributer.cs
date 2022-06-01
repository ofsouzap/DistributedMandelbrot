using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace DistributedMandelbrot
{
    public partial class Distributer
    {

        private readonly static ConcurrentSet<uint> distributerHandledLevels = new();

        private const int listenBacklog = 16;

        #region Message Codes

        // Connection Purpose

        private const byte workloadRequestCode = 0x00;
        private const byte workloadResponseCode = 0x01;

        // Workload Availability

        private const byte workloadAvailableCode = 0x10;
        private static byte[] WorkloadAvailableCodeBytes => new byte[1] { workloadAvailableCode };
        private const byte workloadNotAvailableCode = 0x11;
        private static byte[] WorkloadNotAvailableCodeBytes => new byte[1] { workloadNotAvailableCode };

        // Workload Response Acceptance

        private const byte workloadResponseAcceptCode = 0x20;
        private static byte[] WorkloadResponseAcceptCodeBytes => new byte[1] { workloadResponseAcceptCode };
        private const byte workloadResponseRejectCode = 0x21;
        private static byte[] WorkloadResponseRejectCodeBytes => new byte[1] { workloadResponseRejectCode };

        #endregion

        private readonly Socket socket;

        private bool listening;
        private readonly object listeningLock = new();

        public delegate void LogCallback(string msg);

        /// <summary>
        /// The levels the distributer distributes tasks for
        /// </summary>
        private readonly uint[] levels;

        private event LogCallback InfoLog;
        private event LogCallback ErrorLog;

        //TODO - add timeout to distributed workloads so if a worker takes too long, the job can be reassigned

        private readonly ConcurrentSet<Workload> distributedWorkloads;
        private readonly ConcurrentSet<Workload> completedWorkloads;

        public Distributer(IPEndPoint endpoint,
            uint[] levels,
            LogCallback InfoLog,
            LogCallback ErrorLog)
        {

            // Set logs

            this.InfoLog = InfoLog;
            this.ErrorLog = ErrorLog;

            // Check and update handled levels

            foreach (uint level in levels)
                if (distributerHandledLevels.Contains(level))
                    throw new ArgumentException("One of the chosen levels for this distributer is already being handled by another distributer");
                else
                    distributerHandledLevels.Add(level);

            // Copy levels into this.levels so it can't be edited whilst the distributer runs
            this.levels = new uint[levels.Length];
            Array.Copy(levels, this.levels, levels.Length);

            // Set up workload bags

            distributedWorkloads = new ConcurrentSet<Workload>();
            completedWorkloads = new ConcurrentSet<Workload>(GetRelevantCompletedWorkloads());

            // Set up socket

            socket = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            InfoLog("Created socket");

            socket.Bind(endpoint);

            InfoLog("Bound socket to endpoint");

        }

        ~Distributer()
        {
            foreach (uint level in levels)
                distributerHandledLevels.Remove(level);
        }

        /// <summary>
        /// Gets all the workloads that are already completed with a level that this distributer is registered to distribute tasks for
        /// </summary>
        private IEnumerable<Workload> GetRelevantCompletedWorkloads()
        {

            if (levels == null || levels.Length < 1)
                throw new Exception("Levels not initialised before getting completed levels");

            return DataStorage.GetIndexEntriesEnumerator()
                .Where(entry => levels.Contains(entry.level))
                .Select(entry => new Workload(entry.level, entry.indexReal, entry.indexImag));

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
        /// Synchronously starts the distributer listening and handling requests
        /// </summary>
        public void StartListeningSync()
        {

            // Check not already listening

            if (GetListening())
                throw new Exception("Distributer already listening");

            // Set distributer to listening

            SetListening(true);

            // Set socket to listen

            socket.Listen(listenBacklog);

            InfoLog("Socket set to listen");

            // Listening loop
            while (listening)
            {

                // Accept connection
                Socket client = socket.Accept();

                InfoLog("Worker accepted");

                // Determine connection purpose

                byte[] buffer = new byte[1];
                client.Receive(buffer, 1, SocketFlags.None);

                InfoLog("Connection purpose recevied");

                // Handle connection by purpose

                switch (buffer[0])
                {

                    case workloadRequestCode:
                        InfoLog("Handling workload request...");
                        HandleWorkloadRequest(client);
                        InfoLog("Workload request handled");
                        break;

                    case workloadResponseCode:
                        InfoLog("Handling workload response...");
                        HandleWorkloadResponse(client);
                        InfoLog("Handling workload response");
                        break;

                    default:
                        ErrorLog("Unknown connection purpose recevied");
                        break;

                }

                // Close connection

                client.Close();

                InfoLog("Connection closed");

            }


        }

        //TODO - async listening?

        /// <summary>
        /// Checks if the provided workload has already been completed
        /// </summary>
        private bool CheckWorkloadCompleted(Workload workload)
        {

            if (completedWorkloads == null)
                throw new Exception("Completed workloads not initialised when trying to get workload completed");

            return completedWorkloads.Contains(workload);

        }

        /// <summary>
        /// Checks if a workload is already completed or has already been distributed
        /// </summary>
        private bool WorkloadNeedsToBeRequested(Workload workload)
        {

            if (distributedWorkloads.Contains(workload))
                return false;

            if (CheckWorkloadCompleted(workload))
                return false;

            return true;

        }

        /// <summary>
        /// Tries to get the next workload that needs to be sent to a worker to process. If there are no more needed workloads, returns false
        /// </summary>
        private bool TryGetNextNeededWorkload(out Workload workload)
        {

            foreach (uint level in levels)
                for (uint indexReal = 0; indexReal < level; indexReal++)
                    for (uint indexImag = 0; indexImag < level; indexImag++)
                    {

                        workload = new(level, indexReal, indexImag);

                        if (WorkloadNeedsToBeRequested(workload))
                            return true;

                    }

            workload = default;
            return false;

        }

        /// <summary>
        /// Handle a worker that is requesting a workload
        /// </summary>
        private void HandleWorkloadRequest(Socket worker)
        {

            if (TryGetNextNeededWorkload(out Workload workload))
            {

                // Workload available to distribute

                worker.Send(WorkloadAvailableCodeBytes);

                InfoLog("Told worker that workload is available");

                workload.Send(worker);

                InfoLog("Sent worker workload to complete");

                distributedWorkloads.Add(workload);

                InfoLog("Registered workload as distributed");

            }
            else
            {

                // No workloads available to distribute

                worker.Send(WorkloadNotAvailableCodeBytes);

                InfoLog("Told worker that no workload is available");

            }

        }

        /// <summary>
        /// Handle a worker that is returning a completed workload's data
        /// </summary>
        private void HandleWorkloadResponse(Socket worker)
        {

            Workload workload = Workload.Receive(worker);

            if (distributedWorkloads.Contains(workload))
            {

                // Send acceptance code

                worker.Send(WorkloadResponseAcceptCodeBytes);

                InfoLog("Accepted worker workload");

                // Receive worker data

                byte[] data = new byte[DataChunk.dataChunkSize];
                worker.Receive(data, DataChunk.dataChunkSize, SocketFlags.None);

                InfoLog("Received worker workload data");

                //TODO - check a few random values of the response to verify

                // Mark workload completed

                distributedWorkloads.Remove(workload);
                completedWorkloads.Add(workload);

                InfoLog("Moved workload from distributed workloads to completed workloads");

                // Save data chunk

                DataChunk newChunk = new(level: workload.level,
                    indexReal: workload.indexReal,
                    indexImag: workload.indexImag,
                    data: data);

                DataStorage.SaveDataChunk(newChunk);

                InfoLog("Saved data chunk");

            }
            else
            {

                // Send rejection code

                worker.Send(WorkloadResponseRejectCodeBytes);

                InfoLog("Rejected worker workload");

            }

        }

    }
}
