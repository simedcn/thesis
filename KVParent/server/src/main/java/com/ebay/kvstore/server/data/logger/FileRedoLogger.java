package com.ebay.kvstore.server.data.logger;

import java.io.IOException;

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

	@Override
	public void write(IMutation mutation) {
		try {
			mutation.writeToExternal(out);
		} catch (IOException e) {
			logger.error("Error occured when logging mutation:" + mutation, e);
		}
	}

	public void close() {
		try {
			out.close();
		} catch (IOException e) {
			logger.error("Error occured when closing log file:" + file, e);
		}
	}

	@Override
	public void flush() throws IOException {
		out.flush();
	}

	public FileRedoLogger(String file) throws IOException {
		this.file = file;
		this.out = new FileLoggerOutputStream(DFSManager.getDFS().create(new Path(file), true));
	}

	@Override
	public String getFile() {
		return file;
	}

}
