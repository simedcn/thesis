package com.ebay.chluo.kvstore.data.storage.logger;

import java.util.List;

public interface IRedoLogger {
	public void write(IMutation mutation);

	public List<IMutation> read();
}
