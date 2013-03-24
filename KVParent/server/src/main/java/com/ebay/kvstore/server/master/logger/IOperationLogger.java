package com.ebay.kvstore.server.master.logger;

import com.ebay.kvstore.logger.ILogger;

public interface IOperationLogger extends ILogger {

	public void write(IOperation operation);

}