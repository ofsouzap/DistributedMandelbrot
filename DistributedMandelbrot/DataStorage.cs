using System;
using System.IO;
using System.Text;

namespace DistributedMandelbrot
{
    public static class DataStorage
    {

        // Index file is contiguous list of entries
        // Entry format:
        //     (level[uint32])(indexReal[uint32])(indexImag[uint32])(entryType[uint8])(filenameLength[int32])(filename[char[filenameLength]])
        // N.B. level, indexReal and indexImag are unsigned but filenameLength is signed!

        public const string dataDirectory = "Data/";

        public const string indexFilename = "_index.dat";

        public static string IndexFilePath => Path.Combine(dataDirectory, indexFilename);

        private static readonly object indexFileLock = new();

        private static string DataChunkFilenameToPath(string filename)
            => Path.Combine(dataDirectory, filename);

        /// <summary>
        /// Checks if a data chunk file exists with the specified filename
        /// </summary>
        private static bool DataFilenameExists(string filename)
            => File.Exists(DataChunkFilenameToPath(filename));

        #region Reading and Writing Data Chunks

        /// <summary>
        /// Try read a data chunk's data from a file
        /// </summary>
        /// <param name="path">The file path</param>
        /// <param name="chunk">The chunk that has been read from the file</param>
        /// <returns>If a chunk could be successfully read from the file</returns>
        private static bool TryReadDataFile(string filename,
            ref DataChunk chunk)
        {

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

            return true;

        }

        /// <summary>
        /// Writes a data chunk to a specified file (existing or to-be-created)
        /// </summary>
        /// <param name="path">The file path to write the chunk to</param>
        /// <param name="data">The data chunk whose data should be written to the file</param>
        private static void WriteDataToFile(string filename,
            DataChunk chunk)
        {

            using FileStream file = File.OpenWrite(DataChunkFilenameToPath(filename));
            chunk.Serialize(file);

        }

        #endregion

        public struct IndexEntry
        {

            /// <summary>
            /// Type of data for the entry
            /// </summary>
            public enum Type
            {
                /// <summary>The entry relates to a chunk which has an array of its data stored in a file</summary>
                Regular,
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

            public IndexEntry(uint level, uint indexReal, uint indexImag, string filename) : this(level, indexReal, indexImag, Type.Regular)
            {
                this.filename = filename;
            }

            public static IndexEntry CreateIndexEntryForDataChunk(DataChunk chunk)
            {

                if (chunk.IsNeverChunk)
                    return new IndexEntry(chunk.level, chunk.indexReal, chunk.indexImag, Type.Never);
                else if (chunk.IsImmediateChunk)
                    return new IndexEntry(chunk.level, chunk.indexReal, chunk.indexImag, Type.Immediate);
                else
                {
                    string filename = GenerateDataChunkFilename(chunk);
                    return new IndexEntry(chunk.level, chunk.indexReal, chunk.indexImag, filename);
                }

            }

        }

        private static bool CheckDataDirectoryExists()
            => Directory.Exists(dataDirectory);

        private static bool CheckIndexFileExists()
            => File.Exists(IndexFilePath);

        /// <summary>
        /// Creates the data directory and index file if they don't already exist
        /// </summary>
        private static void SetUpDataDirectoryIfNeeded()
        {

            if (!CheckDataDirectoryExists())
                Directory.CreateDirectory(dataDirectory);

            if (!CheckIndexFileExists())
                File.Create(IndexFilePath);

        }

        #region Reading Index File

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

            if (type == IndexEntry.Type.Regular)
            {

                stream.Read(buffer, 0, 4);
                int filenameLength = BitConverter.ToInt32(buffer, 0);

                buffer = new byte[filenameLength];
                stream.Read(buffer, 0, filenameLength);

                filename = Encoding.ASCII.GetString(buffer, 0, filenameLength);

                return new IndexEntry(level, indexReal, indexImag, filename);

            }
            else
                return new IndexEntry(level, indexReal, indexImag, type);

        }

        /// <summary>
        /// Tries to find the filename for a specified data chunk
        /// </summary>
        /// <param name="qLevel">The level of data chunk being looked for</param>
        /// <param name="qIndexReal">The real index of data chunk being looked for</param>
        /// <param name="qIndexImag">The imaginary index of data chunk being looked for</param>
        /// <param name="filename">The filename of the found data chunk file</param>
        /// <returns>Whether a data chunk file was found for the specified chunk</returns>
        public static bool TryFindChunkFilename(uint qLevel,
            uint qIndexReal,
            uint qIndexImag,
            out string filename)
        {

            foreach (IndexEntry entry in GetIndexEntriesEnumerator())
            {
                if (entry.level == qLevel
                    && entry.indexReal == qIndexReal
                    && entry.indexImag == qIndexImag)
                {
                    filename = entry.filename;
                    return true;
                }
            }

            filename = string.Empty;
            return false;

        }

        public static IEnumerable<IndexEntry> GetIndexEntriesEnumerator()
        {

            lock (indexFileLock)
            {

                SetUpDataDirectoryIfNeeded();

                using FileStream file = File.OpenRead(IndexFilePath);

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

        #region Writing Index File

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

            if (entry.type == IndexEntry.Type.Regular)
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
        
        public static void SaveDataChunk(DataChunk chunk)
        {

            IndexEntry newEntry = IndexEntry.CreateIndexEntryForDataChunk(chunk);

            lock (indexFileLock)
            {

                using FileStream file = File.Open(IndexFilePath, FileMode.Append, FileAccess.Write);

                WriteIndexEntry(file, newEntry);

                if (newEntry.type == IndexEntry.Type.Regular)
                    WriteDataToFile(newEntry.filename, chunk);

            }

        }

        #endregion

    }
}
