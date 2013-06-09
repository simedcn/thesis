using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Net;
using System.Net.Sockets;

namespace kvstore
{
    class SocketUtil
    {
        private static byte[] readBuffer = new byte[1024];

        private static void EndWrite(KVMemoryStream stream)
        {
            long pos = stream.Position;
            stream.Position = 0;
            int len = (int)pos - 4;
            stream.WriteInt(len);
        }
        internal static void WriteUpdateRegionTableRequest(Socket socket)
        {
            using (KVMemoryStream stream = new KVMemoryStream(32))
            {
                stream.Position = 4;
                stream.WriteInt(KVProtocolType.Region_Table_Req);
                EndWrite(stream);
                socket.Send(stream.ToArray());
            }
        }

        internal static int ReadRegionTableResponse(Socket socket,out RegionTable table)
        {
            using (KVMemoryStream stream = ReadResponse(socket))
            {
                int len = stream.ReadInt();
                int type = stream.ReadInt();
                int retCode = stream.ReadInt();
                if (retCode == ProtocolCode.Success)
                {
                    table = DecoderUtil.decodeRegionTable(stream); 
                }
                else
                {
                    table = null;
                }
                return retCode;
            }
        }

        
        internal static void WriteDeleteRequest(Socket socket,byte[] key)
        {
            using (KVMemoryStream stream = new KVMemoryStream(64))
            {
                stream.Position = 4;
                stream.WriteInt(KVProtocolType.Delete_Req);
                stream.WriteByte(1);
                stream.WriteBytes(key);
                EndWrite(stream);
                socket.Send(stream.ToArray());
            }
        }

        internal static int ReadDeleteResponse(Socket socket)
        {
            using (KVMemoryStream stream = ReadResponse(socket))
            {
                int len = stream.ReadInt();
                int type = stream.ReadInt();
                int retCode = stream.ReadInt();
                return retCode;
            }
        }



        internal static void WriteGetRequest(Socket socket, byte[] key)
        {
            using (KVMemoryStream stream = new KVMemoryStream(64))
            {
                stream.Position = 4;
                stream.WriteInt(KVProtocolType.Get_Req);
                stream.WriteByte(1);
                stream.WriteBytes(key);
                EndWrite(stream);
                socket.Send(stream.ToArray());
            }
        }

        internal static int ReadGetResponse(Socket socket, out byte[] value)
        {
            using (KVMemoryStream stream = ReadResponse(socket))
            {
                int len = stream.ReadInt();
                int type = stream.ReadInt();
                int retCode = stream.ReadInt();
                int retry = stream.ReadByte();
                int ttl = stream.ReadInt();
                byte[] key = stream.ReadBytes();
                value = stream.ReadBytes();
                return retCode;
            }
        }


        internal static void WriteSetRequest(Socket socket, byte[] key, byte[] value)
        {
            using (KVMemoryStream stream = new KVMemoryStream(64))
            {
                stream.Position = 4;
                stream.WriteInt(KVProtocolType.Set_Req);
                stream.WriteByte(1);
                stream.WriteInt(0);
                stream.WriteBytes(key);
                stream.WriteBytes(value);
                EndWrite(stream);
                socket.Send(stream.ToArray());
            }
        }

        internal static int ReadSetResponse(Socket socket)
        {
            using (KVMemoryStream stream = ReadResponse(socket))
            {
                int len = stream.ReadInt();
                int type = stream.ReadInt();
                int retCode = stream.ReadInt();
                return retCode;
            }
        }


        internal static void WriteIncrRequest(Socket socket, byte[] key, int incremental, int initValue)
        {
            using (KVMemoryStream stream = new KVMemoryStream(64))
            {
                stream.Position = 4;
                stream.WriteInt(KVProtocolType.Incr_Req);
                stream.WriteByte(1);
                stream.WriteInt(0);
                stream.WriteBytes(key);
                stream.WriteInt(incremental);
                stream.WriteInt(initValue);
                EndWrite(stream);
                socket.Send(stream.ToArray());
            }
        }

        internal static int ReadIncrResponse(Socket socket, out int value)
        {
            using (KVMemoryStream stream = ReadResponse(socket))
            {
                int len = stream.ReadInt();
                int type = stream.ReadInt();
                int retCode = stream.ReadInt();
                int retry = stream.ReadByte();
                int ttl = stream.ReadInt();
                byte[] key = stream.ReadBytes();
                int incremental = stream.ReadInt();
                value = stream.ReadInt();
                return retCode;
            }
        }

        internal static void WriteStatRequest(Socket socket)
        {
            using (KVMemoryStream stream = new KVMemoryStream(64))
            {
                stream.Position = 4;
                stream.WriteInt(KVProtocolType.Stat_Req);
                EndWrite(stream);
                socket.Send(stream.ToArray());
            }
        }

        internal static int ReadStatResponse(Socket socket, out DataServerStruct[] dataServers)
        {
            using (KVMemoryStream stream = ReadResponse(socket))
            {
                int len = stream.ReadInt();
                int type = stream.ReadInt();
                int retCode = stream.ReadInt();
                dataServers = null;
                int size = stream.ReadInt();
                if (size > 0)
                {
                    dataServers = new DataServerStruct[size];
                    for (int i = 0; i < size; i++)
                    {
                        dataServers[i] = DecoderUtil.decodeDataServer(stream);
                    }
                }
                return retCode;
            }
        }

        private static KVMemoryStream ReadResponse(Socket socket)
        {
            KVMemoryStream stream = new KVMemoryStream(64);
            int readLength = 0;
            int messageLength = 0;
            while ((readLength = socket.Receive(readBuffer)) > 0)
            {
                stream.Write(readBuffer, 0, readLength);
                if (messageLength == 0 || readLength >= 4)
                {
                    //calculate message length
                    long oldPos = stream.Position;
                    stream.Position = 0;
                    messageLength = stream.ReadInt();
                    stream.Position = oldPos;
                }
                if (messageLength <= readLength - 4)
                {
                    stream.Position = 0;
                    return stream;
                }
            }
            throw new InvalidDataException();
        }
    }
}
