using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kvstore
{
    interface IKeyComparable
    {
         int CompareTo(byte[] key);
    }
}
