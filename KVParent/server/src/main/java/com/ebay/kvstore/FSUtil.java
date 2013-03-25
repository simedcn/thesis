package com.ebay.kvstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ebay.kvstore.server.data.storage.fs.DFSManager;

public class FSUtil {
	private static Pattern regionFilePattern = Pattern.compile("^[0-9]+\\-[0-9]+\\.data$");

	private static Pattern regionLogPattern = Pattern.compile("^[0-9]+\\-[0-9]+\\.log$");

	private static Pattern checkPointPattern = Pattern.compile("^[0-9]*\\.ckp$");
	private static Pattern masterLogPattern = Pattern.compile("^[0-9]*\\.log$");

	public static long getFileSize(String file) {
		try {
			FileStatus status = DFSManager.getDFS().getFileStatus(new Path(file));
			if (status == null) {
				return 0;
			} else {
				return status.getLen();
			}
		} catch (IOException e) {
			RegionUtil.logger.error("Fail to get FileStatus for " + file, e);
			return 0;
		}

	}

	public static String[] getMasterCheckpointFiles(String dir) throws IOException {
		return scanDirFiles(dir, new Predictor() {
			@Override
			public boolean predict(String filename) {
				return isValidCheckpointFile(filename);
			}
		});
	}

	public static long getMasterFileTimestamp(String name) {
		Path path = new Path(name);
		String filename = path.getName();
		int index = filename.indexOf('.');
		return Long.valueOf(filename.substring(0, index));
	}

	public static String[] getMasterLogFiles(String dir) throws IOException {
		return scanDirFiles(dir, new Predictor() {
			@Override
			public boolean predict(String filename) {
				return isValidMasterLogFile(filename);
			}
		});
	}

	public static String[] getRegionFiles(String dir) throws IOException {
		return scanDirFiles(dir, new Predictor() {
			@Override
			public boolean predict(String filename) {
				return FSUtil.isValidRegionFile(filename);
			}
		});
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
				return isValidRegionLogFile(filename);
			}
		});
	}

	public static boolean isValidCheckpointFile(String name) {
		return checkPointPattern.matcher(name).matches();
	}

	public static boolean isValidMasterLogFile(String name) {
		return masterLogPattern.matcher(name).matches();
	}

	public static boolean isValidRegionFile(String name) {
		return regionFilePattern.matcher(name).matches();
	}

	public static boolean isValidRegionLogFile(String name) {
		return regionLogPattern.matcher(name).matches();
	}

	private static String[] scanDirFiles(String dir, Predictor p) throws IOException {
		FileSystem fs = DFSManager.getDFS();
		FileStatus[] fileStatusArray = fs.listStatus(new Path(dir));
		List<String> list = new ArrayList<>();
		if (fileStatusArray != null) {
			for (FileStatus status : fileStatusArray) {
				String filename = status.getPath().getName();
				if (p.predict(filename)) {
					list.add(filename);
				}
			}
		}
		Collections.sort(list);
		String[] result = new String[list.size()];
		return list.toArray(result);
	}

	private static interface Predictor {
		public boolean predict(String filename);
	}
}
