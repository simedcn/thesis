using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kvstore
{
    class ClientOption
    {
        public int ConnectTimeout
        {
            get
            {
                return connectTimeout;
            }
            set
            {
                connectTimeout = value;
            }
        }

        public ICollection<Address> MasterAddrs
        {
            get
            {
                return masterAddrs;
            }
        }

        private int connectTimeout = 2000;// ms
        private ISet<Address> masterAddrs;

        public ClientOption(int connectTimeout)
        {
            this.connectTimeout = connectTimeout;
            this.masterAddrs = new HashSet<Address>();
        }

        public void AddMasterAddr(Address addr)
        {
            masterAddrs.Add(addr);
        }
    }
}
