using System;
using System.Net;

namespace DistributedMandelbrot
{
    public class Program
    {

        private static bool _timeoutEnabled = true;
        public static bool TimeoutEnabled
        {
            get => _timeoutEnabled;
        }

        public static void Main()
        {

            //TODO - use command-line arguments to customise launching instead of being hard-coded

            // Distributer

            IPEndPoint distributerEndpoint = new(IPAddress.Any, 59010);

            Task distributerTask = CreateDistributerTask(distributerEndpoint, new uint[] { 4, 10, 20 }, s => Console.WriteLine("D Info: " + s), s => Console.WriteLine("D Error: " + s));

            // Data Server

            IPEndPoint dataServerEndpoint = new(IPAddress.Any, 59011);

            Task dataServerTask = CreateDataServerTask(dataServerEndpoint, s => Console.WriteLine("S Info: " + s), s => Console.WriteLine("S Error: " + s));

            // Run tasks

            distributerTask.Start();
            dataServerTask.Start();

            // Join tasks

            distributerTask.Wait();
            dataServerTask.Wait();
            
        }

        private static Task CreateDistributerTask(IPEndPoint endpoint,
            uint[] levels,
            Distributer.LogCallback infoCallback,
            Distributer.LogCallback errCallback)
        {

            Distributer distributer = new(endpoint, levels, infoCallback, errCallback);

            Task task = new(() => distributer.StartListeningSync());

            return task;

        }

        private static Task CreateDataServerTask(IPEndPoint endpoint,
            DataServer.LogCallback infoCallback,
            DataServer.LogCallback errCallback)
        {

            DataServer dataServer = new(endpoint, infoCallback, errCallback);

            Task task = new(() => dataServer.StartListeningSync());

            return task;

        }

    }
}