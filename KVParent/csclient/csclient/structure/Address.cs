using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kvstore
{
    class Address
    {

        public Address(String ip, int port)
        {
            this.ip = ip;
            this.port = port;
        }

        public String Ip
        {
            get
            {
                return ip;
            }
        }
        public int Port
        {
            get
            {
                return port;
            }
        }

        private String ip;
        private int port;

    }
}
