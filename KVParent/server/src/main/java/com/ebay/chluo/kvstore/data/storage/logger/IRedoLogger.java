package com.ebay.chluo.kvstore.data.storage.logger;


public interface IRedoLogger {
	public void write(IMutation mutation) ;

	public void close() ;
}
