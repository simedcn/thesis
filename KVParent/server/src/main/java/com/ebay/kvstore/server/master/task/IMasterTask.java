package com.ebay.kvstore.server.master.task;

public interface IMasterTask {

	public String getName();

	public void start();

	public void stop();

}
