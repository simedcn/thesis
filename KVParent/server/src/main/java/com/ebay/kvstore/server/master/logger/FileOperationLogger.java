package com.ebay.kvstore.server.master.logger;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.logger.FileLoggerOutputStream;
import com.ebay.kvstore.logger.ILoggerOutputStream;
import com.ebay.kvstore.server.data.logger.FileDataLogger;
import com.ebay.kvstore.server.data.storage.fs.DFSManager;

public class FileOperationLogger implements IOperationLogger {
	private static Logger logger = LoggerFactory.getLogger(FileDataLogger.class);

	public static FileOperationLogger forCreate(String file) throws IOException {
		return new FileOperationLogger(file);
	}

	protected String file;

	protected ILoggerOutputStream out;

	protected StringBuilder sb = new StringBuilder();

	protected FileSystem fs = DFSManager.getDFS();

	public FileOperationLogger(String file) throws IOException {
		this.file = file;
		this.out = new FileLoggerOutputStream(fs.create(new Path(file), true));
	}

	public synchronized void append(String file) throws IOException {
		FileOperationLoggerIterator it = new FileOperationLoggerIterator(file);
		IOperation operation = null;
		while (it.hasNext()) {
			operation = it.next();
			operation.writeToExternal(out);
		}
	}

	@Override
	public void close() {
		try {
			out.close();
		} catch (IOException e) {
			logger.error("Error occured when cloing FileOperationLogger", e);
		}
	}

	@Override
	public void flush() throws IOException {
		out.flush();
	}

	@Override
	public String getFile() {
		return file;
	}

	@Override
	public void renameTo(String newLog) throws IOException {
		this.close();
		out = new FileLoggerOutputStream(fs.create(new Path(newLog), true));
		append(file);
		this.file = newLog;
	}

	// TODO As there are some bugs in HDFS, this is a temporary solution.
	@Override
	public void write(IOperation operation) {
		try {
			operation.writeToExternal(out);
		} catch (IOException e) {
			logger.error("Error occured when logging operation:" + operation, e);
		}
		;
	}
}
