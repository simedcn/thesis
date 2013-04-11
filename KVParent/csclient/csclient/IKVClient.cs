using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kvstore
{
    interface IKVClient
    {
        void Delete(byte[] key);

        byte[] Get(byte[] key);

        void Set(byte[] key, byte[] value);

        ClientOption GetClientOption();

        int GetCounter(byte[] key);

        int Incr(byte[] key, int incremental, int initValue);

        DataServerStruct[] Stat();

        void Close();

        void UpdateRegionTable();

        
    }
}
