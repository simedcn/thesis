package com.ebay.kvstore.util;

import java.util.List;

import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;

public class RegionUtil {


	public static Region mergeRegion(Region region1, Region region2, int id) {
		byte[] start, end;
		if (region1.compareTo(region2) < 0) {
			start = region1.getStart();
			end = region2.getEnd();
		} else {
			start = region2.getStart();
			end = region1.getEnd();
		}
		RegionStat stat1 = region1.getStat();
		RegionStat stat2 = region2.getStat();
		RegionStat stat = stat1.clone();
		stat.keyNum += stat2.keyNum;
		stat.readCount += stat2.readCount;
		stat.writeCount += stat2.writeCount;
		stat.size += stat2.size;
		Region region = new Region(id, start, end);
		region.setStat(stat);
		return region;
	}

	public static <T extends IKeyComparable> T search(List<T> list, byte[] key) {
		int low = 0;
		int high = list.size() - 1;
		while (low <= high) {
			int mid = (low + high) >>> 1;
			T midVal = list.get(mid);
			int cmp = midVal.compareTo(key);
			if (cmp < 0)
				low = mid + 1;
			else if (cmp > 0)
				high = mid - 1;
			else
				return list.get(mid); // key found
		}
		return null; // key not found
	}
}
