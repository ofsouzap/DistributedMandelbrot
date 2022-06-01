using System.IO;

namespace DistributedMandelbrot
{
    public partial class DataChunk
    {

        private abstract class Serializer
        {

            public abstract byte GetCode();

            public void Serialize(Stream stream, byte[] data)
            {
                SerializeCode(stream);
                SerializeData(stream, data);
            }

            private void SerializeCode(Stream stream)
            {
                stream.Write(new byte[1] { GetCode() }, 0, 1);
            }

            protected abstract void SerializeData(Stream stream, byte[] data);
            public abstract byte[] DeserializeData(Stream stream);

        }

        private class RawSerializer : Serializer
        {

            public override byte GetCode() => 0x00;

            protected override void SerializeData(Stream stream, byte[] data)
            {
                stream.Write(data, 0, data.Length);
            }

            public override byte[] DeserializeData(Stream stream)
            {

                byte[] data = new byte[dataChunkSize];
                stream.Read(data, 0, data.Length);

                return data;

            }

        }

        private class RLESerializer : Serializer
        {

            public override byte GetCode() => 0x01;

            protected override void SerializeData(Stream stream, byte[] data)
            {

                stream.Write(new byte[1] { GetCode() }, 0, 1);

                uint runLength = 0; // If 0, run hasn't been started yet
                byte runValue = 0;

                foreach (byte value in data)
                {

                    if (runLength == 0)
                    {
                        runLength = 1;
                        runValue = value;
                    }
                    else if (value == runValue)
                        runLength++;
                    else
                    {

                        // Write run

                        if (runLength == 0)
                            throw new Exception("Trying to write run of length 0");

                        stream.Write(BitConverter.GetBytes(runLength), 0, 4);
                        stream.Write(new byte[1] { runValue }, 0, 1);

                        // Reset run to current value

                        runLength = 1;
                        runValue = value;

                    }

                }

                // Write final run (if exists)

                if (runLength == 0)
                    throw new WaitHandleCannotBeOpenedException("No end run to write");

                stream.Write(BitConverter.GetBytes(runLength), 0, 4);
                stream.Write(new byte[1] { runValue }, 0, 1);

            }

            public override byte[] DeserializeData(Stream stream)
            {

                byte[] data = new byte[dataChunkSize];

                int dataIndex = 0;

                while (dataIndex < dataChunkSize)
                {

                    uint runLength;
                    byte runValue;

                    // Read run data

                    byte[] buffer = new byte[4];
                    stream.Read(buffer, 0, 4);
                    runLength = BitConverter.ToUInt32(buffer, 0);

                    buffer = new byte[1];
                    stream.Read(buffer, 0, 1);
                    runValue = buffer[0];

                    // Check run length

                    if (dataIndex + runLength >= dataChunkSize)
                        throw new Exception("Data exceeds chunk expected length");

                    if (runLength == 0)
                        throw new Exception("Encountered run of length 0");

                    // Add run data

                    for (uint i = 0; i < runLength; i++)
                        data[dataIndex++] = runValue;

                }

                return data;

            }

        }

    }
}
