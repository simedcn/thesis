using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kvstore
{
    class KeyValueUtil
    {
        const long LONG_MASK = 0xffffffffL;

        public static int Compare(byte[] key1, byte[] key2)
        {
            if (key1 == null || key2 == null)
            {
                // null key is the max
                if (key1 != null)
                {
                    return -1;
                }
                else if (key2 != null)
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }
            int len1 = key1.Length;
            int len2 = key2.Length;
            if (len1 < len2)
                return -1;
            if (len1 > len2)
                return 1;
            for (int i = 0; i < len1; i++)
            {
                byte a = key1[i];
                byte b = key2[i];
                if (a != b)
                    return ((a & LONG_MASK) < (b & LONG_MASK)) ? -1 : 1;
            }
            return 0;
        }


        public static T Search<T>(List<T> list, byte[] key)
            where T : IKeyComparable
        {
            int low = 0;
            int high = list.Count - 1;
            while (low <= high)
            {
                int mid = (low + high) >> 1;
                T midVal = list[mid];
                int cmp = midVal.CompareTo(key);
                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                else
                    return list[mid]; // key found
            }
            return default(T); // key not found
        }

        internal static int bytesToInt(byte[] value)
        {
            int targets = (value[0] & 0xff) | ((value[1] << 8) & 0xff00) | ((value[2] << 24) >> 8)
                    | (value[3] << 24);
            return targets;
        }
    }
}
