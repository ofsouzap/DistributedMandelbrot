using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Text;

namespace DistributedMandelbrot
{
    public class DataStorageManager
    {

        /// <summary>
        /// Whether or not an instance of this exists. There should only ever be one instance of this
        /// </summary>
        private static bool singletonExists = false;

        // Index file is contiguous list of entries
        // Entry format:
        //     (level[uint32])(indexReal[uint32])(indexImag[uint32])(entryType[uint8])(filenameLength[int32])(filename[char[filenameLength]])
        // N.B. level, indexReal and indexImag are unsigned but filenameLength is signed!

        public const string dataDirectoryName = "Data/";
        public static string DataDirectoryPath => Path.Combine(Program.DataDirectoryParent, dataDirectoryName);

        public const string indexFilename = "_index.dat";

        public static string IndexFilePath => Path.Combine(DataDirectoryPath, indexFilename);

        private readonly object indexFileLock;

        private readonly ConcurrentSet<string> dataFilesBeingAccessed;

        private readonly ConcurrentBag<Job> jobsToProcess;

        private static string DataChunkFilenameToPath(string filename)
            => Path.Combine(DataDirectoryPath, filename);

        /// <summary>
        /// Checks if a data chunk file exists with the specified filename
        /// </summary>
        private static bool DataFilenameExists(string filename)
            => File.Exists(DataChunkFilenameToPath(filename));

        public struct IndexEntry
        {

            /// <summary>
            /// Type of data for the entry
            /// </summary>
            public enum Type
            {
                /// <summary>The entry relates to a chunk which has its data stored in a file</summary>
                DataFile,
                /// <summary>The entry relates to a chunk whose values are all 0</summary>
                Never,
                /// <summary>The entry relates to a chunk whose values are all 1</summary>
                Immediate
            }

            public uint level;
            public uint indexReal;
            public uint indexImag;
            public Type type;
            public string filename;

            public IndexEntry(uint level, uint indexReal, uint indexImag, Type type)
            {
                this.level = level;
                this.indexReal = indexReal;
                this.indexImag = indexImag;
                this.type = type;
                filename = string.Empty;
            }

            public IndexEntry(uint level, uint indexReal, uint indexImag, string filename) : this(level, indexReal, indexImag, Type.DataFile)
            {
                this.filename = filename;
            }

            public static IndexEntry CreateIndexEntryForDataChunk(DataChunk chunk)
            {

                if (chunk.IsNeverChunk)
                    return new(chunk.level, chunk.indexReal, chunk.indexImag, Type.Never);
                else if (chunk.IsImmediateChunk)
                    return new(chunk.level, chunk.indexReal, chunk.indexImag, Type.Immediate);
                else
                {
                    string filename = GenerateDataChunkFilename(chunk);
                    return new(chunk.level, chunk.indexReal, chunk.indexImag, filename);
                }

            }

            /// <summary>
            /// Converts a basic (not stored in a file) index entry to a data chunk
            /// </summary>
            public DataChunk BasicToDataChunk()
            {

                if (type == Type.DataFile)
                    throw new NotSupportedException("Trying to convert data file index entry into a data chunk with incorrect method");

                DataChunk chunk;

                switch (type)
                {

                    case Type.Never:
                        chunk = DataChunk.CreateNeverChunk(level, indexReal, indexImag);
                        break;

                    case Type.Immediate:
                        chunk = DataChunk.CreateImmediateChunk(level, indexReal, indexImag);
                        break;

                    default:
                        throw new Exception("Unknown index entry type");

                }

                return chunk;

            }

        }

        private static bool CheckDataDirectoryExists()
            => Directory.Exists(DataDirectoryPath);

        private static bool CheckIndexFileExists()
            => File.Exists(IndexFilePath);

        /// <summary>
        /// Creates the data directory and index file if they don't already exist
        /// </summary>
        private static void SetUpDataDirectoryIfNeeded()
        {

            if (!CheckDataDirectoryExists())
                Directory.CreateDirectory(DataDirectoryPath);

            if (!CheckIndexFileExists())
                File.Create(IndexFilePath);

        }

        public DataStorageManager()
        {

            if (singletonExists)
                throw new Exception("Creating data storage manager instance when one already exists");

            singletonExists = true;

            indexFileLock = new();

            dataFilesBeingAccessed = new();
            jobsToProcess = new();

            SetUpDataDirectoryIfNeeded();

        }

        #region Job Processing

        public abstract class Job { }

        public class GetCompletedLevelsChunksJob : Job
        {

            public uint[] levels;
            public Action<IndexEntry[]> onComplete;

            public GetCompletedLevelsChunksJob(uint[] levels, Action<IndexEntry[]> onComplete)
            {
                this.levels = levels;
                this.onComplete = onComplete;
            }

        }

        public class LoadEntriesJob : Job
        {

            public QueryChunk[] queries;
            public Action<IndexEntry?[]> onComplete;

            public LoadEntriesJob(QueryChunk[] queries, Action<IndexEntry?[]> onComplete)
            {
                this.queries = queries;
                this.onComplete = onComplete;
            }

        }

        public class SaveChunkJob : Job
        {

            public DataChunk chunk;
            public Action onComplete;

            public SaveChunkJob(DataChunk chunk, Action onComplete)
            {
                this.chunk = chunk;
                this.onComplete = onComplete;
            }

        }

        public class LoadIndexEntryDataFileChunkJob : Job
        {

            public IndexEntry entry;
            public DataChunk emptyChunk;
            public Action<bool> onComplete;

            public LoadIndexEntryDataFileChunkJob(IndexEntry entry, DataChunk emptyChunk, Action<bool> onComplete)
            {
                this.entry = entry;
                this.emptyChunk = emptyChunk;
                this.onComplete = onComplete;
            }

        }

        /// <summary>
        /// Starts processing its jobs so that, whenever jobs arrive, it can be processing one
        /// </summary>
        public void StartProcessingJobsSync()
        {

            while (true)
            {

                // Wait until job available
                while (jobsToProcess.IsEmpty)
                    Thread.Sleep(10);

                // Get a job

                Job job;

                if (!jobsToProcess.TryTake(out Job? nJob))
                    continue;

                if (nJob == null)
                    continue;
                else
                    job = nJob;

                //Process the job

                if (job is GetCompletedLevelsChunksJob getCompletedJob)
                {

                    // Trying to find which chunks have already been completed for a specified set of levels

                    IndexEntry[] founds = GetIndexEntriesEnumerator()
                        .Where(entry => getCompletedJob.levels.Contains(entry.level))
                        .ToArray();

                    getCompletedJob.onComplete?.Invoke(founds);

                }
                else if (job is SaveChunkJob saveJob)
                {

                    // Trying to save a chunk

                    SaveDataChunk(saveJob.chunk);

                    saveJob.onComplete?.Invoke();

                }
                else if (job is LoadEntriesJob loadJob)
                {

                    // Trying to load index entries of chunks

                    IndexEntry?[] entries = TryLoadEntries(loadJob.queries);

                    loadJob.onComplete?.Invoke(entries);

                }
                else if (job is LoadIndexEntryDataFileChunkJob loadChunkJob)
                {

                    // Loading a chunk from a chunk data file

                    bool success = TryReadDataFile(loadChunkJob.entry.filename, ref loadChunkJob.emptyChunk);

                    loadChunkJob.onComplete?.Invoke(success);

                }
                else
                    throw new Exception("Unhandled job type");

            }

        }

        public void AddJob(Job job)
            => jobsToProcess.Add(job);

        #endregion

        /// <summary>
        /// Tries to open the index file for reading. If it fails because another process is using the file, keeps trying.
        /// </summary>
        private static FileStream TryUntilOpenFileRead(string path)
        {

            while (true) {
                try
                {
                    // Try returned the opened file
                    return File.OpenRead(path);
                }
                catch (IOException)
                {
                    // Keep trying after waiting briefly
                    Thread.Sleep(10);
                    continue;
                }
                catch
                {
                    // Throw any other exceptions
                    throw;
                }
            }

        }

        /// <summary>
        /// Tries to open the index file for writing. If it fails because another process is using the file, keeps trying.
        /// </summary>
        private static FileStream TryUntilOpenFileWrite(string path,
            bool appendMode = false)
        {

            FileMode fileMode = appendMode ? FileMode.Append : FileMode.OpenOrCreate;

            while (true)
            {
                try
                {
                    // Try returned the opened file
                    return File.Open(path, fileMode, FileAccess.Write);
                }
                catch (IOException)
                {
                    // Keep trying after waiting briefly
                    Thread.Sleep(10);
                    continue;
                }
                catch
                {
                    // Throw any other exceptions
                    throw;
                }
            }

        }

        #region Reading Data

        /// <summary>
        /// Try read a data chunk's data from a file
        /// </summary>
        /// <param name="path">The file path</param>
        /// <param name="chunk">The chunk that has been read from the file</param>
        /// <returns>If a chunk could be successfully read from the file</returns>
        private bool TryReadDataFile(string filename,
            ref DataChunk chunk)
        {

            // Wait until file available
            while (dataFilesBeingAccessed.Contains(filename))
                Thread.Sleep(10);

            dataFilesBeingAccessed.Add(filename);

            byte[] chunkData = new byte[DataChunk.dataChunkSize];

            try
            {
                using FileStream file = File.OpenRead(DataChunkFilenameToPath(filename));
                chunkData = DataChunk.DeserializeData(file);
            }
            catch (ArgumentException)
            {
                return false;
            }

            chunk.SetData(chunkData);

            dataFilesBeingAccessed.Remove(filename);

            return true;

        }

        /// <summary>
        /// Reads an entry from the index stream and outputs its data
        /// </summary>
        private static IndexEntry ReadIndexEntry(Stream stream)
        {

            uint level, indexReal, indexImag;
            IndexEntry.Type type;
            string filename;

            byte[] buffer = new byte[4];

            stream.Read(buffer, 0, 4);
            level = BitConverter.ToUInt32(buffer, 0);

            stream.Read(buffer, 0, 4);
            indexReal = BitConverter.ToUInt32(buffer, 0);

            stream.Read(buffer, 0, 4);
            indexImag = BitConverter.ToUInt32(buffer, 0);

            stream.Read(buffer, 0, 4);
            type = (IndexEntry.Type)BitConverter.ToInt32(buffer, 0);

            if (type == IndexEntry.Type.DataFile)
            {

                stream.Read(buffer, 0, 4);
                int filenameLength = BitConverter.ToInt32(buffer, 0);

                buffer = new byte[filenameLength];
                stream.Read(buffer, 0, filenameLength);

                filename = Encoding.ASCII.GetString(buffer, 0, filenameLength);

                return new(level, indexReal, indexImag, filename);

            }
            else
                return new(level, indexReal, indexImag, type);

        }

        public struct QueryChunk
        {

            public uint level;
            public uint indexReal;
            public uint indexImag;

            public QueryChunk(uint level, uint indexReal, uint indexImag)
            {
                this.level = level;
                this.indexReal = indexReal;
                this.indexImag = indexImag;
            }

            public bool MatchesIndexEntry(IndexEntry entry)
                => entry.level == level
                && entry.indexReal == indexReal
                && entry.indexImag == indexImag;

        }

        private IndexEntry?[] TryLoadEntries(QueryChunk[] queries)
        {

            // Initialise output chunks array

            IndexEntry?[] entries = new IndexEntry?[queries.Length];

            for (int i = 0; i < entries.Length; i++)
                entries[i] = null;

            // Look through index

            foreach (IndexEntry entry in GetIndexEntriesEnumerator())
            {
                
                for (int qIndex = 0; qIndex < queries.Length; qIndex++)
                {

                    QueryChunk query = queries[qIndex];

                    if (query.MatchesIndexEntry(entry))
                    {

                        entries[qIndex] = entry;

                        if (entries.All(c => c != null))
                            return entries; // If all found, return without needing to look at other index entries

                    }

                }

            }

            return entries;

        }

        private IEnumerable<IndexEntry> GetIndexEntriesEnumerator()
        {

            lock (indexFileLock)
            {

                using FileStream file = TryUntilOpenFileRead(IndexFilePath);

                while (file.Position < file.Length)
                {

                    IndexEntry entry;

                    try
                    {
                        entry = ReadIndexEntry(file);
                    }
                    catch (ArgumentException)
                    {
                        throw new Exception("Corrupted index file");
                    }

                    yield return entry;

                }

            }

        }

        #endregion

        #region Writing Data

        /// <summary>
        /// Writes a data chunk to a specified file (existing or to-be-created)
        /// </summary>
        /// <param name="path">The file path to write the chunk to</param>
        /// <param name="data">The data chunk whose data should be written to the file</param>
        private void WriteDataToFile(string filename,
            DataChunk chunk)
        {

            // Wait until file available
            while (dataFilesBeingAccessed.Contains(filename))
                Thread.Sleep(10);

            dataFilesBeingAccessed.Add(filename);

            using (FileStream file = TryUntilOpenFileWrite(DataChunkFilenameToPath(filename)))
            {
                chunk.Serialize(file);
            }

            dataFilesBeingAccessed.Remove(filename);

        }

        /// <summary>
        /// Writes an index entry for a chunk with a specified filename to the index filestream
        /// </summary>
        /// <param name="stream">The index file's filestream</param>
        /// <param name="chunk">The chunk the entry refers to</param>
        /// <param name="filename">The filename the chunk is being saved with</param>
        private static void WriteIndexEntry(Stream stream,
            IndexEntry entry)
        {

            byte[] buffer;

            buffer = BitConverter.GetBytes(entry.level);
            stream.Write(buffer, 0, 4);

            buffer = BitConverter.GetBytes(entry.indexReal);
            stream.Write(buffer, 0, 4);

            buffer = BitConverter.GetBytes(entry.indexImag);
            stream.Write(buffer, 0, 4);

            buffer = BitConverter.GetBytes((int)entry.type);
            stream.Write(buffer, 0, 4);

            if (entry.type == IndexEntry.Type.DataFile)
            {

                buffer = BitConverter.GetBytes(entry.filename.Length);
                stream.Write(buffer, 0, 4);

                buffer = Encoding.ASCII.GetBytes(entry.filename);
                stream.Write(buffer, 0, entry.filename.Length);

            }

        }

        /// <summary>
        /// Generates a filename for a data chunk until a new, unique filename is found
        /// </summary>
        private static string GenerateDataChunkFilename(DataChunk chunk)
        {

            string baseFilename = chunk.level.ToString() + ';' + chunk.indexReal.ToString() + ';' + chunk.indexImag.ToString();

            if (!DataFilenameExists(baseFilename))
                return baseFilename;

            int indexSuffix;
            for (indexSuffix = 0; DataFilenameExists(baseFilename + indexSuffix.ToString()); indexSuffix++) { }

            return baseFilename + indexSuffix.ToString();

        }

        /// <summary>
        /// Saves a data chunk and adds it to the index
        /// </summary>
        public void SaveDataChunk(DataChunk chunk)
        {

            IndexEntry newEntry = IndexEntry.CreateIndexEntryForDataChunk(chunk);

            lock (indexFileLock)
            {

                using FileStream file = TryUntilOpenFileWrite(IndexFilePath, appendMode: true);

                WriteIndexEntry(file, newEntry);

                if (newEntry.type == IndexEntry.Type.DataFile)
                    WriteDataToFile(newEntry.filename, chunk);

            }

        }

        #endregion

    }
}
