package com.ebay.kvstore.server.data.logger;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.kvstore.logger.BaseFileLogger;

/**
 * Used for log mutation operations in case of redo them to restore data.
 * 
 * @author luochen
 * 
 */
public class DataFileLogger extends BaseFileLogger implements IDataLogger {

	private static Logger logger = LoggerFactory.getLogger(DataFileLogger.class);

	public static DataFileLogger forAppend(String file) throws IOException {
		return new DataFileLogger(file, true);
	}

	public static DataFileLogger forCreate(String file) throws IOException {
		return new DataFileLogger(file, false);
	}

	private DataFileLogger(String file, boolean append) throws IOException {
		super(file, append);
	}

	@Override
	public synchronized void append(String file) throws IOException {
		DataFileLoggerIterator it = new DataFileLoggerIterator(file);
		IMutation mutation = null;
		while (it.hasNext()) {
			mutation = it.next();
			mutation.writeToExternal(out);
		}
	}

	@Override
	public synchronized void write(IMutation mutation) {
		try {
			mutation.writeToExternal(out);
		} catch (IOException e) {
			logger.error("Error occured when logging mutation:" + mutation, e);
		}
	}
}
