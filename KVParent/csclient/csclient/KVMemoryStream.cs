using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace kvstore
{
    class KVMemoryStream:MemoryStream
    {
        private byte[] writeBuffer = new byte[8];
        private byte[] readBuffer = new byte[8];
        public KVMemoryStream(int p):base(p)
        {
        }

        public void WriteInt(int v)
        {
            WriteByte((byte)((v >> 24) & 0xFF));
            WriteByte((byte)((v >> 16) & 0xFF));
            WriteByte((byte)((v >> 8) & 0xFF));
            WriteByte((byte)((v >> 0) & 0xFF));
        }

        public void WriteDouble(double v)
        {
            byte[] longBuf = BitConverter.GetBytes(v);
            long l = BitConverter.ToInt64(longBuf, 0);
            WriteLong(l);
        }

        public void WriteLong(long v)
        {
            writeBuffer[0] = (byte)(v >> 56);
            writeBuffer[1] = (byte)(v >> 48);
            writeBuffer[2] = (byte)(v >> 40);
            writeBuffer[3] = (byte)(v >> 32);
            writeBuffer[4] = (byte)(v >> 24);
            writeBuffer[5] = (byte)(v >> 16);
            writeBuffer[6] = (byte)(v >> 8);
            writeBuffer[7] = (byte)(v >> 0);
            Write(writeBuffer, 0, 8);
        }

        public void WriteShort(short v)
        {
            WriteByte((byte)((v >> 8) & 0xFF));
            WriteByte((byte)((v >> 0) & 0xFF));
        }

        public void WriteString(String value)
        {
            byte[] buf = Encoding.UTF8.GetBytes(value);
            WriteInt(buf.Length);
            Write(buf, 0, buf.Length);
        }

        public void WriteBytes(byte[] bytes)
        {
            int length = bytes != null ? bytes.Length : 0;
            WriteInt(length);
            Write(bytes, 0, length);
        }

        public int ReadInt()
        {

            int ch1 = ReadByte();
            int ch2 = ReadByte();
            int ch3 = ReadByte();
            int ch4 = ReadByte();
            if ((ch1 | ch2 | ch3 | ch4) < 0)
                throw new EndOfStreamException();
            return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
        }

        public double ReadDouble()
        {
            Read(readBuffer,0,8);
            Array.Reverse(readBuffer);
            return BitConverter.ToDouble(readBuffer, 0);
        }

        public short ReadShort()
        {
            int ch1 = ReadByte();
            int ch2 = ReadByte();
            if ((ch1 | ch2) < 0)
                throw new EndOfStreamException();
            return (short)((ch1 << 8) + (ch2 << 0));
        }

        public long ReadLong()
        {
            Read(readBuffer, 0, 8);
            return (((long)readBuffer[0] << 56) +
               ((long)(readBuffer[1] & 255) << 48) +
               ((long)(readBuffer[2] & 255) << 40) +
               ((long)(readBuffer[3] & 255) << 32) +
               ((long)(readBuffer[4] & 255) << 24) +
               ((readBuffer[5] & 255) << 16) +
               ((readBuffer[6] & 255) << 8) +
               ((readBuffer[7] & 255) << 0));
        }

        public String ReadString()
        {
            int length = ReadInt();
            byte[] buffer = new byte[length];
            Read(buffer, 0, length);
            return Encoding.UTF8.GetString(buffer);
        }

        public byte[] ReadBytes()
        {
            int length = ReadInt();
            byte[] bytes = null;
            if (length > 0)
            {
                bytes = new byte[length];
                Read(bytes, 0, length);
            }
            return bytes;
        }
    }
}
