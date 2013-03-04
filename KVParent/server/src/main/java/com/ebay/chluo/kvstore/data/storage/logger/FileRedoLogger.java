package com.ebay.chluo.kvstore.data.storage.logger;

import java.util.List;

/**
 * Used for log mutation operations in case of redo them to restore data.
 * 
 * @author luochen
 * 
 */
public class FileRedoLogger implements IRedoLogger {

	protected String path;

	@Override
	public void write(IMutation mutation) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<IMutation> read() {
		// TODO Auto-generated method stub
		return null;
	}

}
