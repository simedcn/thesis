package com.ebay.kvstore;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.server.data.cache.KeyValueCache;
import com.ebay.kvstore.server.data.logger.FileLoggerInputIterator;
import com.ebay.kvstore.server.data.logger.IMutation;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;
import com.ebay.kvstore.structure.Value;

public class RegionUtil {
	private static Logger logger = LoggerFactory.getLogger(RegionUtil.class);

	private static Pattern regionPattern = Pattern.compile("^[0-9]+\\-[0-9]+\\.data$");
	private static Pattern logPattern = Pattern.compile("^[0-9]+\\-[0-9]+\\.log$");

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

	public static boolean isValidRegionFile(String name) {
		return regionPattern.matcher(name).matches();
	}

	public static boolean isValidLogFile(String name) {
		return logPattern.matcher(name).matches();
	}

	public static long getRegionFileTimestamp(String name) {
		Path path = new Path(name);
		String filename = path.getName();
		int slash = filename.indexOf('-');
		int end = slash + 1;
		while (filename.charAt(end) != '.') {
			end++;
		}
		return Long.valueOf(filename.substring(slash + 1, end));
	}

	public static String[] getRegionLogFiles(String dir) throws IOException {
		return scanDirFiles(dir, new Predictor() {
			@Override
			public boolean predict(String filename) {
				return isValidLogFile(filename);
			}
		});
	}

	private static String[] scanDirFiles(String dir, Predictor p) throws IOException {
		FileSystem fs = DFSManager.getDFS();
		FileStatus[] fileStatusArray = fs.listStatus(new Path(dir));
		List<String> list = new ArrayList<>();
		for (FileStatus status : fileStatusArray) {
			String filename = status.getPath().getName();
			if (p.predict(filename)) {
				list.add(filename);
			}
		}
		Collections.sort(list);
		String[] result = new String[list.size()];
		return list.toArray(result);
	}

	public static String[] getRegionFiles(String dir) throws IOException {
		return scanDirFiles(dir, new Predictor() {
			@Override
			public boolean predict(String filename) {
				return isValidRegionFile(filename);
			}
		});
	}

	private static interface Predictor {
		public boolean predict(String filename);
	}

	public static void loadLogger(String file, KeyValueCache buffer) {
		try {
			FileLoggerInputIterator it = new FileLoggerInputIterator(file);
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

	public static long getFileSize(String file) {
		try {
			FileStatus status = DFSManager.getDFS().getFileStatus(new Path(file));
			if (status == null) {
				return 0;
			} else {
				return status.getLen();
			}
		} catch (IOException e) {
			logger.error("Fail to get FileStatus for " + file, e);
			return 0;
		}

	}
}
