package com.ebay.kvstore.server.master.logger;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.server.data.logger.DataFileLogger;
import com.ebay.kvstore.server.logger.BaseFileLogger;

public class OperationFileLogger extends BaseFileLogger {
	private static Logger logger = LoggerFactory.getLogger(DataFileLogger.class);

	public static OperationFileLogger forAppend(String file) throws IOException {
		return new OperationFileLogger(file, true);
	}

	public static OperationFileLogger forCreate(String file) throws IOException {
		return new OperationFileLogger(file, false);
	}

	private OperationFileLogger(String file, boolean append) throws IOException {
		super(file, append);
	}

	@Override
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

}
