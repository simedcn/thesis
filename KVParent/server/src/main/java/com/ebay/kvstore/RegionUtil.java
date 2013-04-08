package com.ebay.kvstore;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.logger.DataFileLoggerIterator;
import com.ebay.kvstore.server.data.logger.IMutation;
import com.ebay.kvstore.structure.Value;

public class RegionUtil {
	static Logger logger = LoggerFactory.getLogger(RegionUtil.class);

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
		} catch (EOFException e) {
		} catch (IOException e) {
			logger.error("Error occured when loading the log file:" + file, e);
		}
	}

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
