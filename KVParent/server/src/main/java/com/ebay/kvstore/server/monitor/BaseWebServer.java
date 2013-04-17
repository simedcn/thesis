package com.ebay.kvstore.server.monitor;

import com.ebay.kvstore.conf.IConfiguration;
import com.ebay.kvstore.structure.Address;

public abstract class BaseWebServer implements IWebServer {

	protected IConfiguration conf;
	protected Address addr;
	protected String webappDirLocation = "src/main/webapp/monitor";
	protected String appBase;

	protected static final String shutdown = "SHUTDOWN";

	public BaseWebServer(Address addr) {
		this.addr = addr;
	}
}
