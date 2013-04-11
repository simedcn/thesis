using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kvstore
{
    class KVException : Exception
    {

        public KVException(String msg) : base(msg) { }
    }
}
