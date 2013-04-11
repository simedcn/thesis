using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kvstore
{
    class Region: IComparable<Region>,IKeyComparable
    {
        public int RegionId
        {
            get
            {
                return regionId;
            }
        }
        public byte[] Start
        {
            get
            {
                return start;
            }
        }
        public byte[] End
        {
            get
            {
                return end;
            }
        }

        public RegionStat Stat
        {
            get
            {
                return stat;
            }
        }

        private int regionId;
        private byte[] start;
        private byte[] end;
        private RegionStat stat;
        public Region(int regionId, byte[] start, byte[] end, RegionStat stat)
        {
            this.regionId = regionId;
            this.start = start;
            this.end = end;
            this.stat = stat;
        }

        public int CompareTo(Region region)
        {
            if (this == region)
            {
                return 0;
            }
            int e1 = KeyValueUtil.Compare(start, region.End);
            int e2 = KeyValueUtil.Compare(end,region.Start);
            if (e1 > 0)
            {
                return 1;
            }
            else if (e2 < 0)
            {
                return -1;
            }
            else
            {
                return 0;
            }
        }

        public int CompareTo(byte[] key)
        {
            int e1 = KeyValueUtil.Compare(key, start);
            int e2 = KeyValueUtil.Compare(key, end);
            if (e1 >= 0 && e2 <= 0)
            {
                return 0;
            }
            else if (e1 < 0)
            {
                return 1;
            }
            else
            {
                return -1;
            }
        }
    }
}
