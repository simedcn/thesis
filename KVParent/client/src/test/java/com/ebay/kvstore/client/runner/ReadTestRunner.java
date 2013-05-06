package com.ebay.kvstore.client.runner;

import java.util.concurrent.CountDownLatch;

import org.javasimon.SimonManager;

import com.ebay.kvstore.client.IKVClient;
import com.ebay.kvstore.exception.KVException;

public class ReadTestRunner extends BaseTestRunner {

	public ReadTestRunner(IKVClient client, int interval, int count, CountDownLatch latch) {
		super(client, interval, count, latch);
		this.watch = SimonManager.getStopwatch("test.read");
	}

	@Override
	protected void process() throws KVException {
		byte[] key = getRandBytes();
		client.get(key);
	}

}
