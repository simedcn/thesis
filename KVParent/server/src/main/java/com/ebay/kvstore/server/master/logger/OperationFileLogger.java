package com.ebay.kvstore.server.master.logger;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.logger.BaseFileLogger;
import com.ebay.kvstore.logger.FileLoggerOutputStream;
import com.ebay.kvstore.server.data.logger.DataFileLogger;

public class OperationFileLogger extends BaseFileLogger implements IOperationLogger {
	private static Logger logger = LoggerFactory.getLogger(DataFileLogger.class);

	public static OperationFileLogger forCreate(String file) throws IOException {
		return new OperationFileLogger(file,false);
	}

	public static OperationFileLogger forAppend(String file) throws IOException {
		return new OperationFileLogger(file,true);
	}

	private OperationFileLogger(String file, boolean append) throws IOException {
		super(file, append);
	}

	public synchronized void append(String file) throws IOException {
		OperationFileLoggerIterator it = new OperationFileLoggerIterator(file);
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
		out.close();
		fs.delete(new Path(newLog), false);
		if (!fs.rename(new Path(file), new Path(newLog))) {
			throw new IOException("Fail to rename log file from " + file + " to " + newLog);
		}
		out = new FileLoggerOutputStream(fs.append(new Path(newLog)));
		// append(file);
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
