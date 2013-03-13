package com.ebay.chluo.kvstore.data.storage.logger;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.chluo.kvstore.data.storage.fs.DFSClientManager;

/**
 * Used for log mutation operations in case of redo them to restore data.
 * 
 * @author luochen
 * 
 */
public class FSRedoLogger implements IRedoLogger {

	private static Logger logger = LoggerFactory.getLogger(FSRedoLogger.class);

	protected String file;

	protected LoggerOutputStream out;

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

	public FSRedoLogger(String file) throws IOException {
		this.file = file;
		this.out = new LoggerFSOutputStream(DFSClientManager.getClient().create(file, true));
	}

}
