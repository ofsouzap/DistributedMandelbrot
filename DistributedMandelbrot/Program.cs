using System;
using System.Net;

namespace DistributedMandelbrot
{
    public class Program
    {

        public static void Main()
        {

            // Distributer

            IPEndPoint distributerEndpoint = new(IPAddress.Any, 59010);
            Distributer distributer = new(distributerEndpoint, new uint[] { 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }, s => Console.WriteLine("D Info: " + s), s => Console.WriteLine("D Error: " + s));

            Task distributerTask = new(() => distributer.StartListeningSync());

            // Data Server

            IPEndPoint dataServerEndpoint = new(IPAddress.Any, 59011);
            DataServer dataServer = new(dataServerEndpoint, s => Console.WriteLine("S Info: " + s), s => Console.WriteLine("S Error: " + s));

            Task dataServerTask = new(() => dataServer.StartListeningSync());

            // Run tasks

            distributerTask.Start();
            dataServerTask.Start();

            distributerTask.Wait();
            dataServerTask.Wait();

        }
        
    }
}