using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kvstore
{
    class DataServerStruct
    {
        public Address Addr
        {
            get
            {
                return addr;
            }
        }
        public int Weight
        {
            get
            {
                return weight;
            }
        }
        public SystemInfo Info
        {
            get
            {
                return info;
            }
        }

        public ICollection<Region> Regions
        {
            get
            {
                return regions;
            }
        }

        public DataServerStruct(Address addr, int weight, SystemInfo info)
        {
            this.addr = addr;
            this.weight = weight;
            this.info = info;
            this.regions = new HashSet<Region>();
        }

        public void addRegion(Region region)
        {
            this.regions.Add(region);
        }

        private Address addr;

        private int weight;

        private SystemInfo info;

        private ISet<Region> regions;
    }
}
