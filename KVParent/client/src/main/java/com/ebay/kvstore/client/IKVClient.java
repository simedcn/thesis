package com.ebay.kvstore.client;

public interface IKVClient {

	public boolean delete(byte[] key);

	public byte[] get(byte[] key);

	public IClientHandler getClientHandler();

	public ClientOption getClientOption();

	public int getCounter(byte[] key);

	public void incr(byte[] key, int intial, int incremental);

	public void setHandler(IClientHandler handler);

	public void stat();
}
