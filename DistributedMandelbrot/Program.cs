using System;
using System.Net;
using System.Security.AccessControl;

namespace DistributedMandelbrot
{
    public sealed class Program
    {

        private class RunSettings
        {

            private const int defaultDistributerPort = 59010;
            private const int defaultDataServerPort = 59011;
            private const string defaultDataDirectoryParent = "";

            public bool helpRequested;

            public bool timeoutEnabled;

            public IPAddress distributerAddress;
            public int distributerPort;
            public Distributer.LevelSetting[] distributerLevelSettings;
            public bool distributerInfoLogEnabled;
            public bool distributerErrorLogEnabled;

            public IPAddress dataServerAddress;
            public int dataServerPort;
            public bool dataServerInfoLogEnabled;
            public bool dataServerErrorLogEnabled;

            public string dataDirectoryParent;

            public bool DistributerLevelsSet => distributerLevelSettings != null && distributerLevelSettings.Length > 0;

            public IPEndPoint GetDistributerEndpoint()
                => new(distributerAddress, distributerPort);

            public IPEndPoint GetDataServerEndpoint()
                => new(dataServerAddress, dataServerPort);

            public Distributer.LogCallback GetDistributerInfoLogCallback()
                => distributerInfoLogEnabled
                ? s => Console.WriteLine("Distributer Info: " + s)
                : s => { };

            public Distributer.LogCallback GetDistributerErrorLogCallback()
                => distributerErrorLogEnabled
                ? s => Console.WriteLine("Distributer Error: " + s)
                : s => { };

            public DataServer.LogCallback GetDataServerInfoLogCallback()
                => dataServerInfoLogEnabled
                ? s => Console.WriteLine("Data Server Info: " + s)
                : s => { };

            public DataServer.LogCallback GetDataServerErrorLogCallback()
                => dataServerErrorLogEnabled
                ? s => Console.WriteLine("Data Server Error: " + s)
                : s => { };

            public RunSettings()
            {

                helpRequested = false;

                timeoutEnabled = true;

                distributerAddress = IPAddress.Any;
                distributerPort = defaultDistributerPort;
                distributerLevelSettings = Array.Empty<Distributer.LevelSetting>();
                distributerInfoLogEnabled = true;
                distributerErrorLogEnabled = true;

                dataServerAddress = IPAddress.Any;
                dataServerPort = defaultDataServerPort;
                dataServerInfoLogEnabled = true;
                dataServerErrorLogEnabled = true;

                dataDirectoryParent = defaultDataDirectoryParent;

            }

        }

        public static bool TimeoutEnabled => Settings.timeoutEnabled;
        public static string DataDirectoryParent => Settings.dataDirectoryParent;

        private static RunSettings? _settings = null;
        private static RunSettings Settings
        {
            get
            {
                if (_settings == null)
                    throw new Exception("Trying to get settings before settings have been set");
                else
                    return _settings;
            }
            set
            {
                _settings = value;
            }
        }

        public static void Main()
        {

            Settings = ParseCommandLineArguments(Environment.GetCommandLineArgs());

            if (Settings.helpRequested)
            {
                Console.WriteLine(helpMessage);
            }
            else
            {

                // Check Settings

                if (!Settings.DistributerLevelsSet)
                    throw new ArgumentException("Distributer levels weren't set when trying to run program");

                // Distributer

                Task distributerTask = CreateDistributerTask(
                    endpoint: Settings.GetDistributerEndpoint(),
                    levels: Settings.distributerLevelSettings,
                    infoCallback: Settings.GetDistributerInfoLogCallback(),
                    errCallback: Settings.GetDistributerErrorLogCallback()
                );

                // Data Server

                Task dataServerTask = CreateDataServerTask(
                    endpoint: Settings.GetDataServerEndpoint(),
                    infoCallback: Settings.GetDataServerInfoLogCallback(),
                    errCallback: Settings.GetDataServerErrorLogCallback()
                );

                // Run tasks

                distributerTask.Start();
                dataServerTask.Start();

                // Join tasks

                distributerTask.Wait();
                dataServerTask.Wait();

            }
            
        }

        /// <summary>
        /// Tests to see if the program is able to write to a directory
        /// </summary>
        private static bool TestDirectorWriteable(string path)
        {

            try
            {

                using FileStream file = File.Create(Path.Combine(path, Path.GetRandomFileName()), 1, FileOptions.DeleteOnClose);
                file.Close();

                return true;

            }
            catch (UnauthorizedAccessException)
            {
                return false;
            }

        }

        #region Command Line Arguments

        private const string invalidBoolArgErrMsg = "Invalid boolean argument encountered";

        private const string helpMessage =
@"
DistributedMandelbrot server program

Settings:
-h, --help - help page
-l, --levels [l1:mrd1,l2:mrd2,...] - specify the levels (l) and the respective maximum recursion depths (mrd) that the job distributer will send jobs for (required)
-t, --timeout [true|false] - timeout enabled (default true)
-di, -da, --distributer-ip, --distributer-addr [ipaddress] - specify job distributer ip address (default any address)
-dp, --distributer-port [port] - specify job distributer port (default 59010)
-dli, --distributer-log-info [true|false] - if the job distributer's info log should be enabled (default true)
-dle, --distributer-log-error [true|false] - if the job distributer's error log should be enabled (default true)
-si, -sa, --data-server-ip, --data-server-addr [ipaddress] - specify job data server ip address (default any address)
-sp, --data-server-port [port] - specify data server port (default 59011)
-sli, --data-server-log-info [true|false] - if the data server's info log should be enabled (default true)
-sle, --data-server-log-error [true|false] - if the data server's error log should be enabled (default true)
-o, --data-directory [path] - path to the directory to use to store program data in. The data will be stored in a new directory within the specified directory (default is working directory)
";

        private static RunSettings ParseCommandLineArguments(string[] args)
        {

            // Initialise RunSettings instance with default values
            RunSettings settings = new();

            Queue<string> q = new(args);

            q.Dequeue(); // Process name

            while (q.Count > 0)
            {

                string arg = q.Dequeue();

                if (arg[0] != '-')
                    throw new ArgumentException("Unexpected literal encountered: " + arg);

                switch (arg)
                {

                    case "-h":
                    case "--help":
                        settings.helpRequested = true;
                        return settings; // If help requested, other arguments don't need to be read

                    case "-l":
                    case "--levels":

                        if (q.Count == 0)
                            throw new ArgumentException("No argument provided for levels setting");

                        string[] levelSettingValues = q.Dequeue().Split(',');

                        settings.distributerLevelSettings = new Distributer.LevelSetting[levelSettingValues.Length];

                        for (int i = 0; i < levelSettingValues.Length; i++)
                        {

                            string levelSettingValue = levelSettingValues[i];

                            string[] levelSettingParts = levelSettingValue.Split(':');

                            if (levelSettingParts.Length != 2)
                                throw new ArgumentException("Invalid level setting provided");

                            if (!uint.TryParse(levelSettingParts[0], out uint levelSettingLevel))
                                throw new ArgumentException("Invalid level provided");

                            if (!uint.TryParse(levelSettingParts[1], out uint levelSettingMaximumRecursionDepth))
                                throw new ArgumentException("Invalid maximum recusion depth provided");

                            settings.distributerLevelSettings[i] = new Distributer.LevelSetting(levelSettingLevel, levelSettingMaximumRecursionDepth);

                        }

                        break;

                    case "-t":
                    case "--timeout":

                        if (q.Count == 0)
                            throw new ArgumentException("No argument provided for timeout setting");

                        if (!TryParseCommandLineArgumentBool(q.Dequeue(), out settings.timeoutEnabled))
                            throw new ArgumentException(invalidBoolArgErrMsg);

                        break;

                    case "-da":
                    case "-di":
                    case "--distributer-addr":
                    case "--distributer-ip":

                        if (q.Count == 0)
                            throw new ArgumentException("No argument provided for distributer address setting");

                        if (!IPAddress.TryParse(q.Dequeue(), out IPAddress? nDistributerAddress))
                        {
                            throw new ArgumentException("Invalid distributer address");
                        }
                        else
                            if (nDistributerAddress == null)
                                throw new Exception("Unexpected situation");
                            else
                                settings.distributerAddress = nDistributerAddress;

                        break;

                    case "-dp":
                    case "--distributer-port":

                        if (q.Count == 0)
                            throw new ArgumentException("No argument provided for distributer port setting");

                        if (!int.TryParse(q.Dequeue(), out settings.distributerPort)
                            || settings.distributerPort < IPEndPoint.MinPort
                            || settings.distributerPort > IPEndPoint.MaxPort)
                        {
                            throw new ArgumentException("Invalid distributer port");
                        }

                        break;

                    case "-dli":
                    case "--distributer-log-info":

                        if (q.Count == 0)
                            throw new ArgumentException("No argument provided for distributer log info setting");

                        if (!TryParseCommandLineArgumentBool(q.Dequeue(), out settings.distributerInfoLogEnabled))
                            throw new ArgumentException(invalidBoolArgErrMsg);

                        break;

                    case "-dle":
                    case "--distributer-log-error":

                        if (q.Count == 0)
                            throw new ArgumentException("No argument provided for distributer log error setting");

                        if (!TryParseCommandLineArgumentBool(q.Dequeue(), out settings.distributerErrorLogEnabled))
                            throw new ArgumentException(invalidBoolArgErrMsg);

                        break;

                    case "-sa":
                    case "-si":
                    case "--data-server-addr":
                    case "--data-server-ip":

                        if (q.Count == 0)
                            throw new ArgumentException("No argument provided for data server address setting");

                        if (!IPAddress.TryParse(q.Dequeue(), out IPAddress? nDataServerAddress))
                        {
                            throw new ArgumentException("Invalid data server address");
                        }
                        else
                            if (nDataServerAddress == null)
                                throw new Exception("Unexpected situation");
                            else
                                settings.dataServerAddress = nDataServerAddress;

                        break;

                    case "-sp":
                    case "--data-server-port":

                        if (q.Count == 0)
                            throw new ArgumentException("No argument provided for data server port setting");

                        if (!int.TryParse(q.Dequeue(), out settings.dataServerPort)
                            || settings.dataServerPort < IPEndPoint.MinPort
                            || settings.dataServerPort > IPEndPoint.MaxPort)
                        {
                            throw new ArgumentException("Invalid data server port");
                        }

                        break;

                    case "-sli":
                    case "--data-server-log-info":

                        if (q.Count == 0)
                            throw new ArgumentException("No argument provided for data server log info setting");

                        if (!TryParseCommandLineArgumentBool(q.Dequeue(), out settings.dataServerInfoLogEnabled))
                            throw new ArgumentException(invalidBoolArgErrMsg);

                        break;

                    case "-sle":
                    case "--data-server-log-error":

                        if (q.Count == 0)
                            throw new ArgumentException("No argument provided for data server log error setting");

                        if (!TryParseCommandLineArgumentBool(q.Dequeue(), out settings.dataServerErrorLogEnabled))
                            throw new ArgumentException(invalidBoolArgErrMsg);

                        break;

                    case "-o":
                    case "--data-directory":

                        if (q.Count == 0)
                            throw new ArgumentException("No argument provided for data directory path setting");

                        settings.dataDirectoryParent = q.Dequeue();

                        if (!Directory.Exists(settings.dataDirectoryParent))
                            throw new ArgumentException("Data directory path provided doesn't exist");

                        if (!TestDirectorWriteable(settings.dataDirectoryParent))
                            throw new ArgumentException("Data directory provided isn't writeable");

                        break;

                    default:
                        throw new ArgumentException("Unknown setting encountered: " + arg);

                }

            }

            return settings;

        }

        private static bool TryParseCommandLineArgumentBool(string arg,
            out bool b)
        {

            switch (arg.ToLower())
            {

                case "true":
                case "yes":
                case "y":
                case "1":
                    b = true;
                    return true;

                case "false":
                case "no":
                case "n":
                case "0":
                    b = false;
                    return true;

                default:
                    b = default;
                    return false;

            }

        }

        #endregion

        private static Task CreateDistributerTask(IPEndPoint endpoint,
            Distributer.LevelSetting[] levels,
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