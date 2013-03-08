package com.ebay.chluo.kvstore;

import java.util.List;

import com.ebay.chluo.kvstore.structure.Region;

public class RegionUtil {

	public static Region searchRegion(List<Region> regions, byte[] key) {
		int low = 0;
		int high = regions.size() - 1;
		while (low <= high) {
			int mid = (low + high) >>> 1;
			Region midVal = regions.get(mid);
			int cmp = midVal.compareTo(key);
			if (cmp < 0)
				low = mid + 1;
			else if (cmp > 0)
				high = mid - 1;
			else
				return regions.get(mid); // key found
		}
		return null; // key not found
	}

}
