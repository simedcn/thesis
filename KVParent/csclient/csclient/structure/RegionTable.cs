using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;

namespace kvstore
{
    class RegionTable
    {
        private IDictionary<Region,Address> map;
        private List<Region> regions;
        private Boolean sorted = false;

        public RegionTable()
        {
            map = new Dictionary<Region, Address>();
            regions = new List<Region>();
        }

        public void addRegion(Region region, Address addr)
        {
            map.Add(region, addr);
            regions.Add(region);
            sorted = false;
        }

        public Address getKeyAddr(byte[] key)
        {
            Region region = getKeyRegion(key);
            if (region == null)
            {
                return null;
            }
            else
            {
                Address addr = null;
                map.TryGetValue(region,out addr);
                return addr;
            }
        }


        public Region getKeyRegion(byte[] key)
        {
            if (!sorted)
            {
                regions.Sort();
                sorted = true;
            }
            return KeyValueUtil.Search(regions, key);
        }
    }
}
