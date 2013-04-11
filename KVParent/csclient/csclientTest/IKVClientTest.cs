using kvstore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;

namespace csclientTest
{
    [TestClass()]
    public class IKVClientTest
    {
        [TestMethod()]
        public void TestOperation()
        {
            ClientOption option = new ClientOption(2000);
            option.AddMasterAddr(new Address("127.0.0.1", 20000));
            IKVClient client = new KVClient(option);

            try
            {
                for (byte i = 0; i < 100; i++)
                {
                    client.Set(new byte[] { i }, new byte[] { i });
                }
                for (byte i = 0; i < 100; i += 2)
                {
                    client.Delete(new byte[] { i });
                }
                for (byte i = 0; i < 100; i++)
                {
                    byte[] value = client.Get(new byte[] { i });
                    if (i % 2 != 0)
                    {
                        CollectionAssert.AreEqual(value, new byte[] { i });
                    }
                    else
                    {
                        Assert.IsNull(value);
                    }
                }

                client.Incr(new byte[] { 100 }, 0, 10);
                Assert.AreEqual(client.GetCounter(new byte[] { 100 }), 10);

                DataServerStruct[] dataServers = client.Stat();
                formatStat(dataServers);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }

        }

        private void formatStat(DataServerStruct[] dataServers)
        {
            if (dataServers == null)
            {
                Console.WriteLine("No data server found");
                return;
            }
            foreach (DataServerStruct server in dataServers)
            {
                Console.WriteLine("Data Server: " + server.Addr);
                indent(1);
                Console.WriteLine("Info:");
                indent(2);
                Console.WriteLine("Memory Free:" + server.Info.memoryFree / (1024*1024)
                        + "MB");
                indent(2);
                Console.WriteLine("Memory Total:" + server.Info.memoryTotal / (1024 * 1024)
                        + "MB");
                indent(2);
                Console.WriteLine("Cpu Usage:" + server.Info.cpuUsage * 100 + "%");
                ICollection<Region> regions = server.Regions;
                indent(1);
                Console.WriteLine("Regions:");
                foreach (Region region in regions)
                {
                    indent(2);
                    Console.Write("Region:");
                    Console.Write(region.RegionId);
                    indent(1);
                    Console.Write(region.Start);
                    Console.Write('-');
                    Console.Write(region.End);
                    Console.WriteLine();
                    indent(3);
                    Console.WriteLine("Read Count:" + region.Stat.readCount);
                    indent(3);
                    Console.WriteLine("Write Count:" + region.Stat.writeCount);
                    indent(3);
                    Console.WriteLine("Entry Num:" + region.Stat.keyNum);
                    indent(3);
                    Console.WriteLine("Size:" + formatRegionSize(region.Stat.size));
                }
            }
        }

        private void indent(int count)
        {
            for (int i = 0; i < count; i++)
            {
                Console.Write('\t');
            }
        }

        private String formatRegionSize(long size)
        {
            if (size > 1024 * 1024)
            {
                return (double)size / (1024 * 1024) + "MB";
            }
            else if (size > 1024)
            {
                return (double)size / 1024  + "B";
            }
            else
            {
                return size + "B";
            }
        }
    }
}
