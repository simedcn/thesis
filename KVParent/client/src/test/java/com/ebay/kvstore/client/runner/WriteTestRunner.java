package com.ebay.kvstore.client.runner;

import java.util.concurrent.CountDownLatch;

import org.javasimon.SimonManager;

import com.ebay.kvstore.client.IKVClient;
import com.ebay.kvstore.exception.KVException;

public class WriteTestRunner extends BaseTestRunner {

	public WriteTestRunner(IKVClient client, int interval, int count, CountDownLatch latch) {
		super(client, interval, count,latch);
		this.watch = SimonManager.getStopwatch("test.write");
	}

	@Override
	protected void process() throws KVException {
		byte[] key = getRandBytes();
		if (random.nextBoolean()) {
			client.set(key, key);
		} else {
			client.delete(key);
		}
	}

}
