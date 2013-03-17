package com.ebay.kvstore.server.data.logger;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.server.data.storage.fs.DFSManager;

/**
 * Used for log mutation operations in case of redo them to restore data.
 * 
 * @author luochen
 * 
 */
public class FileRedoLogger implements IRedoLogger {

	private static Logger logger = LoggerFactory.getLogger(FileRedoLogger.class);

	protected String file;

	protected ILoggerOutputStream out;

	protected StringBuilder sb = new StringBuilder();

	protected FileSystem fs = DFSManager.getDFS();

	public static FileRedoLogger forCreate(String file) throws IOException {
		return new FileRedoLogger(file, false);
	}

	public static FileRedoLogger forAppend(String file) throws IOException {
		return new FileRedoLogger(file, true);
	}

	@Override
	public synchronized void write(IMutation mutation) {
		try {
			mutation.writeToExternal(out);
		} catch (IOException e) {
			logger.error("Error occured when logging mutation:" + mutation, e);
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
	public synchronized void flush() throws IOException {
		out.flush();
	}

	private FileRedoLogger(String file, boolean append) throws IOException {
		this.file = file;
		if (append) {
			// this.out = new FileLoggerOutputStream(fs.append(new Path(file)));
			Path path = new Path(file);
			Path tmpPath = new Path(path.getParent(), String.valueOf(System.currentTimeMillis()));
			fs.rename(path, tmpPath);
			this.out = new FileLoggerOutputStream(fs.create(path));
			append(tmpPath.toString());
		} else {
			this.out = new FileLoggerOutputStream(fs.create(new Path(file), true));
		}
	}

	@Override
	public String getFile() {
		return file;
	}

	// TODO As there are some bugs in HDFS, this is a temporary solution.
	@Override
	public synchronized void renameTo(String newLog) throws IOException {
		this.close();
		out = new FileLoggerOutputStream(fs.create(new Path(newLog), true));
		append(file);
		this.file = newLog;
	}

	@Override
	public synchronized void append(String file) throws IOException {
		FileLoggerInputIterator it = new FileLoggerInputIterator(file);
		IMutation mutation = null;
		while (it.hasNext()) {
			mutation = it.next();
			mutation.writeToExternal(out);
		}
	}
}
