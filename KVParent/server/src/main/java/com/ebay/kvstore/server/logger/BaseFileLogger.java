package com.ebay.kvstore.server.logger;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.server.util.DFSManager;

public abstract class BaseFileLogger implements ILogger {

	private static Logger logger = LoggerFactory.getLogger(BaseFileLogger.class);

	protected String file;

	protected ILoggerOutputStream out;

	protected FileSystem fs = DFSManager.getDFS();

	public BaseFileLogger(String file, boolean append) throws IOException {
		this.file = file;
		if (append) {
			this.out = new FileLoggerOutputStream(fs.append(new Path(file)));
		} else {
			this.out = new FileLoggerOutputStream(fs.create(new Path(file), true));
		}
	}

	@Override
	public synchronized void close() {
		try {
			out.close();
		} catch (IOException e) {
			logger.error("Error occured when closing log file:" + file, e);
		}
	}

	@Override
	public String getFile() {
		return file;
	}

	@Override
	public synchronized void renameTo(String newLog) throws IOException {
		this.close();
		fs.delete(new Path(newLog), false);
		if (!fs.rename(new Path(file), new Path(newLog))) {
			throw new IOException("Fail to rename " + file + " to " + newLog);
		}
		out = new FileLoggerOutputStream(fs.append(new Path(newLog)));
		this.file = newLog;
	}

	// TODO As there are some bugs in HDFS, this is a temporary solution.
	@Override
	public void write(ILogEntry entry) {
		try {
			entry.writeToExternal(out);
		} catch (IOException e) {
			logger.error("Error occured when logging operation:" + entry, e);
		}
	}
}
