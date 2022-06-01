using System;
using System.Numerics;
using System.IO;

namespace DistributedMandelbrot
{
    public partial class DataChunk
    {

        // All images are square from (minAxis)+(minAxis)i to (maxAxis)+(maxAxis)i
        // Images are made of multiple chunks
        // Each data element in a chunk is a byte

        public const double minAxis = -2;
        public const double maxAxis = 2;
        
        /// <summary>
        /// The range of each data chunk on each axis
        /// </summary>
        public const int dataChunkRange = 4096;

        public const uint maximumRecursionDepth = 65535;

        /// <summary>
        /// The number of elements in a data chunk
        /// </summary>
        public const int dataChunkSize = dataChunkRange * dataChunkRange;

        /// <summary>
        /// Get the range (on the real and imaginary axes) of a chunk at the specified level
        /// </summary>
        public static double GetLevelChunkRange(uint level)
            => (maxAxis - minAxis) / level;

        /// <summary>
        /// The "level" of the chunk. A data chunk for level n is for the image with dimensions dataChunkSize*n x dataChunkSize*n
        /// </summary>
        public readonly uint level;

        /// <summary>
        /// The range of the chunk on the real and imaginary axes
        /// </summary>
        public double ChunkRange
            => GetLevelChunkRange(level);

        /// <summary>
        /// The index of the chunk along the real axis
        /// </summary>
        public readonly uint indexReal;

        /// <summary>
        /// The index of the chunk along the imaginary axis
        /// </summary>
        public readonly uint indexImag;

        /// <summary>
        /// The real part of the imaginary number represented at the start of the chunk
        /// </summary>
        public double StartValueReal
            => minAxis + (ChunkRange * indexReal);

        /// <summary>
        /// The imaginary part of the imaginary number represented at the start of the chunk
        /// </summary>
        public double StartValueImag
            => minAxis + (ChunkRange * indexImag);

        /// <summary>
        /// The imaginary number represented at the start of the data chunk
        /// </summary>
        public Complex StartValue
            => new(StartValueReal, StartValueImag);

        /// <summary>
        /// The data stored by the data chunk. Only valid if data is loaded is true
        /// </summary>
        private byte[] data;

        /// <summary>
        /// If all the values for the chunk are 0
        /// </summary>
        public bool IsNeverChunk => data.All(x => x == 0);

        /// <summary>
        /// If all the values for the chunk are 1
        /// </summary>
        public bool IsImmediateChunk => data.All(x => x == 1);

        #region Constructors

        /// <summary>
        /// Create an empty data chunk
        /// </summary>
        public DataChunk(uint level, uint indexReal, uint indexImag)
        {

            #region Checks

            if (level <= 0)
                throw new ArgumentException("Level must be positive");

            if (indexReal >= level)
                throw new ArgumentException("Real index must be lesser than level");

            if (indexImag >= level)
                throw new ArgumentException("Imag index must be lesser than level");

            #endregion

            this.level = level;
            this.indexReal = indexReal;
            this.indexImag = indexImag;

            data = Array.Empty<byte>();

        }

        /// <summary>
        /// Create a data chunk with its data
        /// </summary>
        public DataChunk(uint level, uint indexReal, uint indexImag, byte[] data) : this(level, indexReal, indexImag)
        {
            this.data = data;
        }

        public static DataChunk CreateIdenticalChunk(uint level, uint indexReal, uint indexImag, byte value)
        {

            byte[] data = new byte[dataChunkSize];

            for (int i = 0; i < dataChunkSize; i++)
                data[i] = value;

            return new(level, indexReal, indexImag, data);

        }

        public static DataChunk CreateNeverChunk(uint level, uint indexReal, uint indexImag)
            => CreateIdenticalChunk(level, indexReal, indexImag, 0);

        public static DataChunk CreateImmediateChunk(uint level, uint indexReal, uint indexImag)
            => CreateIdenticalChunk(level, indexReal, indexImag, 1);

        #endregion

        public void SetData(byte[] data)
        {

            if (data.Length != dataChunkSize)
                throw new ArgumentException("Data provided is of incorrect length");

            if (this.data.Length == dataChunkSize)
                throw new Exception("Setting data when chunk's data already set");

            this.data = new byte[dataChunkSize];

            Array.Copy(data, this.data, dataChunkSize);

        }

        #region Serialization

        private static readonly Serializer[] serializers = new Serializer[]
        {
            new RawSerializer(),
            new RLESerializer()
        };

        /// <summary>
        /// Serialize the data chunk to a stream
        /// </summary>
        /// <param name="stream">The stream to serialize to</param>
        public void Serialize(Stream stream)
        {

            if (data == null)
                throw new Exception("Trying to serialize data chunk when data is null");

            // Find serializer that will minimise output length

            long minLength = long.MaxValue;
            Serializer? minSerializer = null;

            foreach (Serializer serializer in serializers)
            {

                SizeCountStream sizeStream = new();

                serializer.Serialize(sizeStream, data);

                if (sizeStream.GetCurrentSize() < minLength)
                {
                    minLength = sizeStream.GetCurrentSize();
                    minSerializer = serializer;
                }

            }

            if (minSerializer == null)
                throw new Exception("No serializer selected to be used");

            // Serialize data

            minSerializer.Serialize(stream, data);

        }

        /// <summary>
        /// Try to deserialize a data chunk from a stream
        /// </summary>
        /// <param name="stream">The stream to deserailize from</param>
        /// <param name="chunk">The chunk that is deserialized if successful</param>
        /// <returns>Whether the deserialization was successful</returns>
        public static byte[] DeserializeData(Stream stream)
        {

            byte[] buffer = new byte[1];
            stream.Read(buffer, 0, 1);

            byte code = buffer[0];
            Serializer? chunkSerializer = null;

            foreach (Serializer serializer in serializers)
                if (serializer.GetCode() == code)
                {
                    chunkSerializer = serializer;
                    break;
                }

            if (chunkSerializer == null)
                throw new Exception("No serializer found for chunk file");

            return chunkSerializer.DeserializeData(stream);

        }

        #endregion

    }
}
