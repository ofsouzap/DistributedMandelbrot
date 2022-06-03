using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Diagnostics;
using System.Timers;

namespace DistributedMandelbrot
{
    public partial class Distributer
    {

        private readonly static ConcurrentSet<uint> distributerHandledLevels = new();

        private const int listenBacklog = 16;
        private const int receiveTimeout = 100;

        /// <summary>
        /// How long in milliseconds a worker has from being sent their workload to completing it in milliseconds
        /// </summary>
        private const long distributedWorkloadTimeout = 1000 * 3600; // An hour

        private const long distributedWorkloadCleanupDelay = 1000 * 60 * 5; // 5 minutes

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

        private readonly Stopwatch stopwatch;
        private long StopwatchMilliseconds => stopwatch.ElapsedMilliseconds;

        private readonly System.Timers.Timer distributedWorkloadCleanupTimer;

        private bool listening;
        private readonly object listeningLock = new();

        public delegate void LogCallback(string msg);

        public struct LevelSetting
        {

            public uint level;
            public uint maximumRecusionDepth;

            public LevelSetting(uint level, uint maximumRecusionDepth)
            {
                this.level = level;
                this.maximumRecusionDepth = maximumRecusionDepth;
            }

        }

        /// <summary>
        /// The levels and maximum recursion depths the distributer distributes tasks for
        /// </summary>
        private readonly LevelSetting[] levelSettings;

        private event LogCallback InfoLog;
        private event LogCallback ErrorLog;

        private readonly ConcurrentSet<DistributedWorkload> distributedWorkloads;
        private readonly ConcurrentSet<Workload> completedWorkloads;

        public Distributer(IPEndPoint endpoint,
            LevelSetting[] levelSettings,
            LogCallback InfoLog,
            LogCallback ErrorLog)
        {

            // Create stopwatch

            stopwatch = new Stopwatch();
            stopwatch.Start();

            // Create distributed workload cleanup timer

            distributedWorkloadCleanupTimer = new System.Timers.Timer(distributedWorkloadCleanupDelay);
            distributedWorkloadCleanupTimer.Elapsed += (Object? sender, ElapsedEventArgs e) => CleanupDistributedWorkloads();
            distributedWorkloadCleanupTimer.Enabled = true;
            distributedWorkloadCleanupTimer.Start();

            // Set logs

            this.InfoLog = InfoLog;
            this.ErrorLog = ErrorLog;

            // Check and update handled levels

            foreach (LevelSetting levelSetting in levelSettings)
                if (distributerHandledLevels.Contains(levelSetting.level))
                    throw new ArgumentException("One of the chosen levels for this distributer is already being handled by a distributer");
                else
                    distributerHandledLevels.Add(levelSetting.level);

            // Copy level settings into this.levelSettings so it can't be edited whilst the distributer runs
            this.levelSettings = new LevelSetting[levelSettings.Length];
            Array.Copy(levelSettings, this.levelSettings, levelSettings.Length);

            // Set up workload bags

            distributedWorkloads = new();
            completedWorkloads = new(GetRelevantCompletedWorkloads());

            // Set up socket

            socket = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            InfoLog("Created socket");

            socket.Bind(endpoint);

            InfoLog("Bound socket to endpoint");

        }

        ~Distributer()
        {

            stopwatch.Stop();

            distributedWorkloadCleanupTimer.Stop();
            distributedWorkloadCleanupTimer.Dispose();

            foreach (LevelSetting levelSetting in levelSettings)
                distributerHandledLevels.Remove(levelSetting.level);

            socket.Close();

        }

        private void CleanupDistributedWorkloads()
        {

            long currentTime = StopwatchMilliseconds;

            distributedWorkloads.RemoveAllWhere(dw => dw.CheckHasTimedOut(currentTime));

        }

        /// <summary>
        /// Gets all the workloads that are already completed with a level that this distributer is registered to distribute tasks for
        /// </summary>
        private IEnumerable<Workload> GetRelevantCompletedWorkloads()
        {

            if (levelSettings == null || levelSettings.Length < 1)
                throw new Exception("Levels not initialised before getting completed levels");

            return DataStorage.GetIndexEntriesEnumerator()
                .Where(entry => levelSettings.Any(ls => ls.level == entry.level))
                .Select(entry => new Workload(entry.level, null, entry.indexReal, entry.indexImag));

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

        private static void ConfigureClientSocket(Socket socket)
        {

            if (Program.TimeoutEnabled)
                socket.ReceiveTimeout = receiveTimeout;

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

                // Configure client socket settings

                ConfigureClientSocket(client);

                try
                {

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
                catch (SocketException e)
                {
                    switch (e.SocketErrorCode)
                    {

                        case SocketError.TimedOut:
                        case SocketError.ConnectionReset:
                        case SocketError.Interrupted:
                            ErrorLog("Connection error, closing client connection:\n" + e.Message);
                            client.Close();
                            continue;

                        default:
                            throw e;

                    }
                }

            }

        }

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

            long currentTime = StopwatchMilliseconds;

            if (distributedWorkloads.ContainsWhere(dw => dw.Matches(workload, currentTime)))
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

            foreach (LevelSetting levelSetting in levelSettings)
                for (uint indexReal = 0; indexReal < levelSetting.level; indexReal++)
                    for (uint indexImag = 0; indexImag < levelSetting.level; indexImag++)
                    {

                        workload = new(levelSetting.level, levelSetting.maximumRecusionDepth, indexReal, indexImag);

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

                DistributedWorkload newDistributedWorkload = new(workload, StopwatchMilliseconds + distributedWorkloadTimeout);

                distributedWorkloads.Add(newDistributedWorkload);

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

            long currentTime = StopwatchMilliseconds;

            if (distributedWorkloads.ContainsWhere(dw => dw.Matches(workload, currentTime)))
            {

                // Send acceptance code

                worker.Send(WorkloadResponseAcceptCodeBytes);

                InfoLog("Accepted worker workload");

                // Receive worker data

                byte[] data = new byte[DataChunk.dataChunkSize];
                worker.Receive(data, DataChunk.dataChunkSize, SocketFlags.None);

                InfoLog("Received worker workload data");

                // Mark workload completed

                distributedWorkloads.RemoveOneWhere(dw => dw.Matches(workload, currentTime));
                completedWorkloads.Add(workload);

                InfoLog("Moved workload from distributed workloads to completed workloads");

                // Create data chunk to save

                DataChunk newChunk = new(level: workload.level,
                    indexReal: workload.indexReal,
                    indexImag: workload.indexImag,
                    data: data);

                // Have chunk saved asynchronously so distributer can handle other clients in the meantime

                Task saveChunkTask = new(() =>
                {
                    DataStorage.SaveDataChunk(newChunk);
                    InfoLog("A data chunk has finished being saved");
                });

                saveChunkTask.Start();

                InfoLog("Sent data chunk to be saved");

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
