package com.ebay.chluo.kvstore;

import java.util.List;

public class RegionUtil {

	public static <T extends KeyComparable> T search(List<T> list, byte[] key) {
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
