using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kvstore
{
    class Program
    {
        static void Main(string[] args)
        {
            ClientOption option = new ClientOption(2000);
            option.AddMasterAddr(new Address("127.0.0.1",20000));
            KVClient client = new KVClient(option);
            client.UpdateRegionTable();
        }
    }
}
