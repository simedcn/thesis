using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kvstore
{
    class DecoderUtil
    {

        public static RegionTable decodeRegionTable(KVMemoryStream stream)
        {
            RegionTable table =new RegionTable();
            int size = stream.ReadInt();
            for (int i = 0; i < size; i++)
            {
                Region region = decodeRegion(stream);
                Address addr = decodeAddress(stream);
                table.addRegion(region, addr);
            }
            return table;
        }

        public static Address decodeAddress(KVMemoryStream stream)
        {
            String ip = stream.ReadString();
            int port = stream.ReadInt();
            return new Address(ip, port);
        }

        public static Region decodeRegion(KVMemoryStream stream)
        {
            int regionId = stream.ReadInt();
            byte[] start = stream.ReadBytes();
            byte[] end = stream.ReadBytes();
            RegionStat stat = decodeRegionStat(stream);
            return new Region(regionId, start, end, stat);
        }

        public static RegionStat decodeRegionStat(KVMemoryStream stream)
        {
            RegionStat stat = new RegionStat();
            stat.keyNum = stream.ReadInt();
            stat.size = stream.ReadLong();
            stat.readCount = stream.ReadLong();
            stat.writeCount = stream.ReadLong();
            return stat;
        }

        internal static DataServerStruct decodeDataServer(KVMemoryStream stream)
        {
            Address addr = decodeAddress(stream);
            int weight = stream.ReadInt();
            SystemInfo info = decodeSystemInfo(stream);
            int size = stream.ReadInt();
            DataServerStruct dataServer = new DataServerStruct(addr, weight, info);
            for (int i = 0; i < size; i++)
            {
                Region region = decodeRegion(stream);
                dataServer.addRegion(region);
            }
            return dataServer;
        }

        private static SystemInfo decodeSystemInfo(KVMemoryStream stream)
        {
            SystemInfo info = new SystemInfo();
            info.memoryFree = stream.ReadLong();
            info.memoryTotal = stream.ReadLong();
            info.cpuUsage = stream.ReadDouble();
            return info;
        }
    }
}
