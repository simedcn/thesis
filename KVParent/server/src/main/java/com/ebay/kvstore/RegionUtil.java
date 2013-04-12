package com.ebay.kvstore;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.logger.DataFileLoggerIterator;
import com.ebay.kvstore.server.data.logger.IMutation;
import com.ebay.kvstore.structure.Region;
import com.ebay.kvstore.structure.RegionStat;
import com.ebay.kvstore.structure.Value;

public class RegionUtil {
	private static Logger logger = LoggerFactory.getLogger(RegionUtil.class);

	public static void loadLogger(String file, KeyValueCache buffer) {
		try {
			DataFileLoggerIterator it = new DataFileLoggerIterator(file);
			while (it.hasNext()) {
				IMutation mutation = it.next();
				switch (mutation.getType()) {
				case IMutation.Set:
					buffer.set(mutation.getKey(), mutation.getValue());
					break;
				case IMutation.Delete:
					buffer.set(mutation.getKey(), new Value(null, true));
					break;
				}
			}
		} catch (IOException e) {
			logger.error("Error occured when loading the log file:" + file, e);
		}
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
		RegionStat stat = (RegionStat) stat1.clone();
		stat.keyNum += stat2.keyNum;
		stat.readCount += stat2.readCount;
		stat.writeCount += stat2.writeCount;
		stat.size += stat2.size;
		Region region = new Region(id, start, end);
		region.setStat(stat);
		return region;
	}
}
