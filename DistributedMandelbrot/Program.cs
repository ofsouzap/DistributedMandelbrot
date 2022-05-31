using System;
using System.Net;

namespace DistributedMandelbrot
{
    public class Program
    {

        public static void Main()
        {

            IPEndPoint endpoint = new(IPAddress.Any, 59010);
            Distributer distributer = new(endpoint, new uint[] { 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }, s => Console.WriteLine("Info: " + s), s => Console.WriteLine("Error: " + s));

            distributer.StartListeningSync();

        }

    }
}