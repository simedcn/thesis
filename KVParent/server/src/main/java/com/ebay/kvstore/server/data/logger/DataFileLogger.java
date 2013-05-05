package com.ebay.kvstore.server.data.logger;

import java.io.IOException;

import com.ebay.kvstore.server.logger.BaseFileLogger;

/**
 * Used for log mutation operations in case of redo them to restore data.
 * 
 * @author luochen
 * 
 */
public class DataFileLogger extends BaseFileLogger {


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

}
